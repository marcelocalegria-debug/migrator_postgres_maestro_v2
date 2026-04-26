import os
import sys
import asyncio
import shutil
import psycopg2
from pathlib import Path
from typing import List, Optional
from datetime import datetime
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.styles import Style
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .project import MigrationProject
from .db import MigrationDB
from .config import MigrationConfig
from .steps.base import StepRunner
from .steps.s00_precheck import PrecheckStep
from .steps.s01_create_database import CreateDatabaseStep
from .steps.s02_import_schema import ImportSchemaStep
from .steps.s03_compare_pre import ComparePreStep
from .steps.s05_disable_constraints import DisableConstraintsStep
from .steps.s06_migrate_big import MigrateBigStep
from .steps.s07_migrate_small import MigrateSmallStep
from .steps.s08_enable_constraints import EnableConstraintsStep
from .steps.s09_sequences import SequencesStep
from .steps.s10_compare_post import ComparePostStep
from .steps.s11_validate import ValidateStep
from .steps.s12_analyze import AnalyzeStep
from .steps.s13_report import ReportStep

class MaestroCLI:
    """CLI interativa para o Maestro V2 baseada em prompt_toolkit e rich."""

    def __init__(self):
        self.console = Console()
        self.project = MigrationProject()
        self.current_seq = None
        self.db = None
        self.config = None
        self.runner = None
        
        self.commands = [
            "/init", "/resume", "/load", "/status", "/check", "/compare",
            "/monitor", "/run", "/rerun", "/rerun-only", "/reset-table", "/ignore", "/agent", "/help", "/quit"
        ]
        self.completer = WordCompleter(self.commands)
        self.session = PromptSession(completer=self.completer)
        
        self.style = Style.from_dict({
            'prompt': '#ansigreen bold',
            'seq': '#ansiyellow bold',
        })

        # [NOVO] Tenta carregar a última migração automaticamente
        self._auto_resume()

    def _auto_resume(self):
        """Localiza e carrega a migração mais recente disponível."""
        migrations = self.project.list_migrations()
        if migrations:
            last_seq = migrations[-1]
            try:
                mig_dir = self.project.get_migration_dir(last_seq)
                if (mig_dir / "migration.db").exists() and (mig_dir / "config.yaml").exists():
                    self.current_seq = last_seq
                    self.db = MigrationDB(mig_dir / "migration.db")
                    self.config = MigrationConfig(mig_dir / "config.yaml")
                    # Mensagem silenciosa ou discreta será exibida no display_welcome ou run
            except Exception:
                pass

    def display_warning_banner(self):
        warning_text = (
            "[bold red]**** ATENÇÃO!! ****[/bold red]\n\n"
            "[bold yellow]Processos em Background:[/bold yellow] Ao interromper o Maestro com Ctrl+C ou /quit, "
            "os subprocessos (Popen) podem continuar rodando como 'órfãos' no Windows.\n"
            "[bold cyan]Recomendação:[/bold cyan] Antes de executar um novo /run, verifique no Gerenciador de Tarefas "
            "se ainda existem processos [bold]python.exe[/bold] ativos consumindo CPU.\n\n"
            "[bold red]Risco de Duplicação:[/bold red]\n"
            " • O sistema é reiniciável através dos checkpoints (last_pk_value/last_db_key) salvos no SQLite.\n"
            " • [bold underline]PERIGO:[/bold underline] Se você executar /run enquanto os processos anteriores ainda estiverem rodando, "
            "você terá duas instâncias tentando migrar as mesmas tabelas. Isso causará erros de conexão, violação de PK no PostgreSQL ou, "
            "pior, [bold]duplicação de dados[/bold] caso a tabela não tenha PK definida.\n\n"
            "[bold green]=> Ação Correta:[/bold green] Se interromper o Maestro, [bold]encerre manualmente os processos de migração antigos[/bold] "
            "antes de entrar novamente e usar /run ou /load. Quando você voltar, o migrador saberá exatamente de onde continuar."
        )
        self.console.print(Panel(warning_text, border_style="red", title="[blink red]ALERTA DE SEGURANÇA[/blink red]"))

    def display_welcome(self):
        self.console.print(Panel.fit(
            "[bold blue]MAESTRO V2[/bold blue] - Orquestrador de Migração Firebird 3 -> PostgreSQL 18+",
            subtitle="Baseado no Plano de Implementação"
        ))
        if self.current_seq:
            self.console.print(f"[dim]Auto-load: Migração [bold cyan]{self.current_seq}[/bold cyan] carregada automaticamente.[/dim]\n")
        
        self.display_warning_banner()

    def run(self):
        self.display_welcome()
        while True:
            try:
                prompt_text = [
                    ('class:prompt', 'maestro '),
                ]
                if self.current_seq:
                    prompt_text.append(('class:seq', f'[{self.current_seq}] '))
                prompt_text.append(('class:prompt', '>> '))

                text = self.session.prompt(prompt_text, style=self.style).strip()
                if not text:
                    continue
                
                if text.startswith("/"):
                    cmd_parts = text.split()
                    cmd = cmd_parts[0]
                    args = cmd_parts[1:]
                    
                    if cmd == "/quit":
                        self.display_warning_banner()
                        break
                    elif cmd == "/help":
                        self.show_help()
                    elif cmd == "/init":
                        self.do_init()
                    elif cmd == "/resume" or cmd == "/load":
                        self.do_resume(args)
                    elif cmd == "/status":
                        self.do_status(args)
                    elif cmd == "/check":
                        self.do_check()
                    elif cmd == "/compare":
                        self.do_compare()
                    elif cmd == "/monitor":
                        self.do_monitor()
                    elif cmd == "/run":
                        self.do_run(args)
                    elif cmd == "/rerun":
                        self.do_rerun(args)
                    elif cmd == "/rerun-only":
                        self.do_rerun_only(args)
                    elif cmd == "/reset-table":
                        self.do_reset_table(args)
                    elif cmd == "/ignore":
                        self.do_ignore(args)
                    elif cmd == "/agent":
                        self.do_agent()
                    else:
                        self.console.print(f"[red]Comando desconhecido: {cmd}[/red]")
                else:
                    self.console.print("[yellow]Use comandos iniciados com / (ex: /help)[/yellow]")
            
            except KeyboardInterrupt:
                continue
            except EOFError:
                self.display_warning_banner()
                break
        
        self.console.print("[blue]Encerrando Maestro. Até logo![/blue]")

    def show_help(self):
        table = Table(title="Comandos Disponíveis")
        table.add_column("Comando", style="cyan")
        table.add_column("Descrição")
        table.add_row("/init", "Inicia uma nova migração (MIGRACAO_XXXX)")
        table.add_row("/resume [seq]", "Lista ou carrega uma migração existente")
        table.add_row("/load [seq]", "Alias para /resume (carrega migração)")
        table.add_row("/status [step]", "Mostra progresso global ou detalhamento de tabelas (5 e 6).")
        table.add_row("/check", "Verifica conectividade, disco e script de ajuste FB")
        table.add_row("/compare", "Compara estrutura entre bancos origem e destino")
        table.add_row("/run [step]", "Execução INCREMENTAL: Pula passos já concluídos (OK).")
        table.add_row("/rerun <step>", "Execução FORÇADA: Ignora status OK e reinicia a pipeline deste ponto.")
        table.add_row("/rerun-only <step>", "Execução ISOLADA: Reseta e executa SOMENTE o passo especificado.")
        table.add_row("/reset-table <nome>", "Reseta status de uma tabela específica para re-migração.")
        table.add_row("/ignore <nome>", "Marca uma tabela como OK manualmente (ignora erros).")
        table.add_row("/monitor", "Abre o dashboard interativo de monitoramento")
        table.add_row("/agent", "Inicia chat com Agente IA para análise de dados")
        table.add_row("/help", "Mostra esta ajuda")
        table.add_row("/quit", "Encerra o Maestro com segurança")
        self.console.print(table)

    def do_check(self):
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa. Use /init ou /resume primeiro.[/yellow]")
            return

        mig_info = self.db.get_migration_by_seq(self.current_seq)
        mig_dir = self.project.get_migration_dir(self.current_seq)

        # Se ajusta_base_firebird.sql não existe, oferece copiar um .sql da raiz antes do precheck
        adjust_path = mig_dir / "ajusta_base_firebird.sql"
        if not adjust_path.exists():
            sql_files = sorted(Path(".").glob("*.sql"))
            if sql_files:
                self.console.print(f"\n[yellow]ajusta_base_firebird.sql não encontrado em {mig_dir.name}.[/yellow]")
                self.console.print("[bold cyan]Arquivos .sql disponíveis na raiz:[/bold cyan]")
                for i, f in enumerate(sql_files, 1):
                    size_kb = f.stat().st_size // 1024
                    self.console.print(f"  [[bold]{i}[/bold]] {f.name}  ({size_kb} KB)")
                self.console.print("  [[bold]0[/bold]] Não copiar nenhum (continuar sem o arquivo)")

                choice = self.session.prompt("\nSelecione o arquivo para copiar como ajusta_base_firebird.sql [0]: ").strip()
                if choice and choice != '0':
                    try:
                        idx = int(choice) - 1
                        if 0 <= idx < len(sql_files):
                            shutil.copy2(sql_files[idx], adjust_path)
                            self.console.print(f"[green]Copiado: {sql_files[idx].name} -> {adjust_path.name}[/green]")
                        else:
                            self.console.print("[yellow]Seleção fora do intervalo. Continuando sem o arquivo.[/yellow]")
                    except ValueError:
                        self.console.print("[yellow]Entrada inválida. Continuando sem o arquivo.[/yellow]")
            else:
                self.console.print(f"[yellow]Nenhum .sql na raiz disponível para copiar como ajusta_base_firebird.sql.[/yellow]")

        self.console.print(f"[bold cyan]--- Executando Verificações (Migration {self.current_seq}) ---[/bold cyan]")

        # PRECHECK é o step 0
        precheck = PrecheckStep(mig_info['id'], self.db, self.config, 0)
        if precheck.run():
            self.console.print("[bold green][OK] Conectividade e pré-requisitos validados.[/bold green]")

            # Execução do ajusta_base_firebird.sql (Human-in-the-loop)
            
            if adjust_path.exists():
                self.console.print(f"\n[yellow]Atenção: Script de correção '{adjust_path.name}' detectado.[/yellow]")
                
                try:
                    script_content = adjust_path.read_text(encoding='utf-8')
                    self.console.print("\n[bold cyan]--- Conteúdo do Script ---[/bold cyan]")
                    self.console.print(script_content)
                    self.console.print("[bold cyan]--------------------------[/bold cyan]\n")
                except Exception as e:
                    self.console.print(f"[red]Não foi possível ler o script: {e}[/red]")

                confirm = self.session.prompt("Deseja executar as correções no banco Firebird agora? (s/N): ").lower()
                if confirm == 's':
                    if self._run_firebird_script(adjust_path):
                        self.console.print("[bold green][OK] Correções aplicadas no Firebird com sucesso.[/bold green]")
                    else:
                        self.console.print("[bold red][FAILED] Falha ao aplicar correções no Firebird. Corrija o script e tente novamente.[/bold red]")
                        return
                else:
                    self.console.print("[yellow]Execução do script de correção cancelada pelo usuário.[/yellow]")
            
            # COMPARE_PRE é o step 3
            confirm_comp = self.session.prompt("\nDeseja executar a comparação de estrutura entre os bancos agora? (s/N): ").lower()
            if confirm_comp == 's':
                compare = ComparePreStep(mig_info['id'], self.db, self.config, 3)
                compare.run()
            else:
                self.console.print("[yellow]Comparação de estrutura ignorada pelo usuário.[/yellow]")
        else:
            self.console.print("[bold red][FAILED] Falha nos pré-requisitos básicos.[/bold red]")
            return

    def _run_firebird_script(self, script_path: Path) -> bool:
        """Executa um script SQL no Firebird usando o driver interno fdb."""
        fb = self.config.firebird
        
        try:
            import fdb
            import re
            
            # Garante que a DLL local seja carregada no Windows
            if os.name == 'nt':
                fb_dll = os.path.abspath("fbclient.dll")
                if os.path.exists(fb_dll):
                    try:
                        fdb.load_api(fb_dll)
                    except: pass

            self.console.print(f"Conectando ao Firebird via driver interno (fdb)...")
            
            # Usa o charset configurado ou fallback para UTF8
            charset = fb.get('charset', 'UTF8').upper()
            # Normaliza nomes ISO que o Firebird não reconhece
            _charset_map = {'ISO-8859-1': 'ISO8859_1', 'ISO8859-1': 'ISO8859_1',
                            'LATIN-1': 'LATIN1', 'ISO-8859-15': 'ISO8859_15'}
            charset = _charset_map.get(charset, charset)
            
            conn = fdb.connect(
                host=fb['host'], 
                port=fb.get('port', 3050),
                database=fb['database'],
                user=fb['user'], 
                password=fb['password'],
                charset=charset
            )
            
            cur = conn.cursor()
            script_content = script_path.read_text(encoding='utf-8')
            
            # Limpeza básica de comentários e separação por ponto e vírgula
            # Remove comentários de linha (--)
            lines = [re.sub(r'--.*$', '', line) for line in script_content.splitlines()]
            content_clean = "\n".join(lines)
            
            # Divide por ; para execução individual
            statements = [s.strip() for s in content_clean.split(';') if s.strip()]
            
            self.console.print(f"Executando [bold]{len(statements)}[/bold] comandos SQL...")
            
            for i, sql in enumerate(statements, 1):
                try:
                    # Remove o comando 'commit' se ele vier como statement individual (o fdb faz commit no fim)
                    if sql.lower() == 'commit':
                        continue
                        
                    cur.execute(sql)
                    
                    # Mostra progresso simplificado
                    display_sql = (sql[:97] + "...") if len(sql) > 100 else sql
                    self.console.print(f"  ({i}/{len(statements)}) [dim]{display_sql}[/dim]")
                except Exception as e_sql:
                    self.console.print(f"\n[bold red]Erro no comando {i}:[/bold red]\n{sql}")
                    self.console.print(f"[red]Motivo:[/red] {e_sql}")
                    conn.rollback()
                    cur.close()
                    conn.close()
                    return False
            
            conn.commit()
            cur.close()
            conn.close()
            return True
            
        except Exception as e:
            self.console.print(f"[bold red]Falha na conexão ou execução via fdb:[/bold red] {e}")
            return False

    def do_compare(self):
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa. Use /init ou /resume primeiro.[/yellow]")
            return
        
        mig_info = self.db.get_migration_by_seq(self.current_seq)
        # COMPARE_PRE é o step 3
        compare = ComparePreStep(mig_info['id'], self.db, self.config, 3)
        compare.run()

    def do_agent(self):
        if not self.current_seq:
             self.console.print("[yellow]Ative uma migração com /init ou /resume para usar o agente.[/yellow]")
             return
        
        mig_dir = self.project.get_migration_dir(self.current_seq)
        
        self.console.print("[bold blue]Iniciando Sessão com Agente ADK...[/bold blue]")
        self.console.print("(Digite 'sair' ou 'voltar' para retornar ao Maestro)")
        
        import asyncio
        import threading
        from .ai.agent import MigrationAIAgent
        
        agent = MigrationAIAgent(project_path=str(mig_dir.absolute()))
        session_id = f"cli_{self.current_seq}"

        def run_async_task(coro):
            """Helper para rodar coroutine em qualquer ambiente (mesmo com loop ativo)."""
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Se o loop já está rodando, precisamos de uma thread para rodar o asyncio.run
                    result_container = {"data": None, "error": None}
                    def _thread_target():
                        try:
                            # Cria um novo loop na thread
                            result_container["data"] = asyncio.run(coro)
                        except Exception as e:
                            result_container["error"] = e
                    
                    t = threading.Thread(target=_thread_target)
                    t.start()
                    t.join()
                    if result_container["error"]: raise result_container["error"]
                    return result_container["data"]
                else:
                    return loop.run_until_complete(coro)
            except RuntimeError:
                # Nenhum loop criado ainda
                return asyncio.run(coro)

        while True:
            try:
                user_msg = self.session.prompt("Agente >> ").strip()
                if not user_msg: continue
                if user_msg.lower() in ('sair', 'voltar', 'exit', 'quit'):
                    break
                
                with self.console.status("[bold green]IA pensando..."):
                    # Passar o objeto de coroutine diretamente (não chamar await aqui)
                    response = run_async_task(agent.execute_task(session_id, user_msg))
                
                self.console.print(f"\n[bold blue]AGENTE:[/bold blue]\n{response}\n")
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.console.print(f"[red]Erro no agente: {e}[/red]")
                break

    def _check_db_exists(self, config_path: Path) -> Optional[str]:
        """Verifica se o banco de destino já existe no PostgreSQL."""
        try:
            cfg = MigrationConfig(config_path)
            pg = cfg.postgres
            conn = psycopg2.connect(
                host=pg['host'], 
                port=pg.get('port', 5432),
                database='postgres',
                user=pg['user'], 
                password=pg['password']
            )
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (pg['database'],))
            exists = cur.fetchone()
            cur.close()
            conn.close()
            return pg['database'] if exists else None
        except:
            return None

    def do_init(self):
        config_file = Path("config.yaml")
        if not config_file.exists():
            self.console.print("[red]Arquivo config.yaml não encontrado na raiz.[/red]")
            return

        # O banco DEVE ser criado pelo DBA de antemão. Se já existe: OK. Se não: avisa.
        db_name = self._check_db_exists(config_file)
        if db_name:
            self.console.print(f"[bold green][OK][/bold green] Banco de destino '[bold cyan]{db_name}[/bold cyan]' já existe no PostgreSQL e será utilizado.")
        else:
            self.console.print("[bold yellow][AVISO][/bold yellow] Banco de destino não encontrado no PostgreSQL.")
            self.console.print("[yellow]O banco deve ser criado pelo DBA antes de prosseguir com a migração.[/yellow]")
            confirm = self.session.prompt("Deseja prosseguir mesmo assim? (s/N): ").strip().lower()
            if confirm != 's':
                return

        seq = self.project.get_next_seq()

        # Lista arquivos .sql na raiz para seleção do schema.sql
        sql_files = sorted(Path(".").glob("*.sql"))
        schema_file = None
        if sql_files:
            self.console.print("\n[bold cyan]Arquivos .sql disponíveis na raiz:[/bold cyan]")
            for i, f in enumerate(sql_files, 1):
                size_kb = f.stat().st_size // 1024
                self.console.print(f"  [[bold]{i}[/bold]] {f.name}  ({size_kb} KB)")
            self.console.print("  [[bold]0[/bold]] Não copiar nenhum (schema.sql deverá ser colocado manualmente)")

            choice = self.session.prompt("\nSelecione o arquivo para usar como schema.sql [0]: ").strip()
            if choice and choice != '0':
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(sql_files):
                        schema_file = sql_files[idx]
                        self.console.print(f"[green]Selecionado: {schema_file.name}[/green]")
                    else:
                        self.console.print("[yellow]Seleção fora do intervalo. schema.sql não será copiado.[/yellow]")
                except ValueError:
                    self.console.print("[yellow]Entrada inválida. schema.sql não será copiado.[/yellow]")
        else:
            self.console.print(f"[yellow]Nenhum .sql encontrado na raiz. Coloque o schema.sql manualmente em MIGRACAO_{seq}/.[/yellow]")

        # 1. Cria diretórios
        mig_dir = self.project.init_migration(seq, config_file, schema_file)
        
        # 2. Inicializa Banco Maestro dentro da pasta da migração
        self.db = MigrationDB(mig_dir / "migration.db")
        
        # 3. Registra a migração no banco (Maestro)
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config_content = f.read()
            self.db.create_migration(seq, config_yaml=config_content)
        except Exception as e:
            self.console.print(f"[red]Erro ao criar registro de migração: {e}[/red]")
            return

        # 4. Carrega Configuração (do arquivo copiado para a pasta da migração)
        try:
            self.config = MigrationConfig(mig_dir / "config.yaml")
        except Exception as e:
            self.console.print(f"[red]Erro ao carregar configuração: {e}[/red]")
            return

        # 5. Registra os steps iniciais (S04 REMOVIDO)
        step_names = [
            'PRECHECK',          # 0
            'CREATE_DATABASE',   # 1
            'IMPORT_SCHEMA',     # 2
            'COMPARE_PRE',       # 3
            'DISABLE_CONSTRAINTS', # 4
            'MIGRATE_BIG',       # 5
            'MIGRATE_SMALL',     # 6
            'ENABLE_CONSTRAINTS',# 7
            'SEQUENCES',         # 8
            'COMPARE_POST',      # 9
            'VALIDATE',          # 10
            'ANALYZE',           # 11
            'REPORT'             # 12
        ]
        mig_info = self.db.get_migration_by_seq(seq)
        self.db.create_steps(mig_info['id'], step_names)
        
        self.current_seq = seq
        self.console.print(f"[green]Migração {seq} inicializada em {mig_dir}[/green]")

    def do_resume(self, args):
        migrations = self.project.list_migrations()
        if not migrations:
            self.console.print("[yellow]Nenhuma migração encontrada.[/yellow]")
            return

        if not args:
            table = Table(title="Migrações Disponíveis")
            table.add_column("Seq", style="cyan")
            table.add_column("Diretório")
            table.add_column("Última Alteração")
            
            from datetime import datetime
            for seq in migrations:
                m_dir = self.project.get_migration_dir(seq)
                mtime = datetime.fromtimestamp(m_dir.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                table.add_row(seq, f"MIGRACAO_{seq}", mtime)
            
            self.console.print(table)
            seq = self.session.prompt("Digite o número da sequência (Ex: 0001) ou 'voltar': ").strip()
            if not seq or seq.lower() == 'voltar':
                return
        else:
            seq = args[0]
        
        if seq not in migrations:
            self.console.print(f"[red]Migração {seq} não encontrada.[/red]")
            return
        
        mig_dir = self.project.get_migration_dir(seq)
        self.current_seq = seq
        self.db = MigrationDB(mig_dir / "migration.db")
        self.config = MigrationConfig(mig_dir / "config.yaml")
        self.console.print(f"[green]Retomando migração {seq}[/green]")

    def do_status(self, args):
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa. Use /init ou /resume.[/yellow]")
            return
        
        mig_info = self.db.get_migration_by_seq(self.current_seq)

        if args:
            try:
                step_num = int(args[0])
                if step_num not in (5, 6):
                    self.console.print(f"[yellow]O detalhamento de status via /status <step> só está disponível para os passos 5 (Big) e 6 (Small).[/yellow]")
                    return
                
                # Obtém lista de exclusão do config.yaml (as 10 big tables)
                # Tenta na raiz ou dentro de 'migration' (conforme seu config.yaml)
                migration_cfg = self.config.data.get('migration', {})
                exclude_list = migration_cfg.get('exclude_tables', [])
                if not exclude_list:
                    exclude_list = self.config.data.get('exclude_tables', [])
                
                exclude_list = [t.upper() for t in exclude_list]
                
                all_tables = self.db.list_tables(mig_info['id'])
                
                if step_num == 5:
                    # Passo 5: Apenas as tabelas na lista de exclusão
                    target_tables = [t for t in all_tables if t['source_table'].upper() in exclude_list]
                else:
                    # Passo 6: Tudo o que NÃO está na lista de exclusão
                    target_tables = [t for t in all_tables if t['source_table'].upper() not in exclude_list]
                
                if not target_tables:
                    self.console.print(f"[yellow]Nenhuma tabela encontrada para o passo {step_num}.[/yellow]")
                    return

                table = Table(title=f"Detalhes das Tabelas - Passo {step_num}")
                table.add_column("Tabela Origem", style="cyan")
                table.add_column("Status")
                table.add_column("Linhas Migradas", justify="right")
                table.add_column("Erro")
                
                completed_count = 0
                shown_count = 0
                
                for t in target_tables:
                    if t['status'] in ('completed', 'loaded'):
                        completed_count += 1
                        continue
                    
                    shown_count += 1
                    status_style = "yellow" if t['status'] == 'running' else "red" if t['status'] == 'failed' else "white"
                    table.add_row(
                        t['source_table'],
                        f"[{status_style}]{t['status']}[/{status_style}]",
                        f"{t['rows_migrated']:,}",
                        t['error_message'] or "-"
                    )
                
                if shown_count > 0:
                    self.console.print(table)
                    self.console.print(f"[dim]Resumo: {completed_count} tabelas concluídas (OK), {shown_count} pendentes/falhas.[/dim]")
                else:
                    self.console.print(f"[bold green][OK] Todas as {len(target_tables)} tabelas do passo {step_num} foram concluídas com sucesso.[/bold green]")
                return

            except ValueError:
                self.console.print("[red]O argumento de /status deve ser o número do step.[/red]")
                return

        # Comportamento padrão (Geral)
        steps = self.db.list_steps(mig_info['id'])
        
        table = Table(title=f"Status da Migração {self.current_seq}")
        table.add_column("Step", style="cyan")
        table.add_column("Nome")
        table.add_column("Status")
        table.add_column("Início")
        table.add_column("Fim")
        
        for s in steps:
            status_style = "green" if s['status'] == 'completed' else "yellow" if s['status'] == 'running' else "red" if s['status'] == 'failed' else "white"
            table.add_row(
                str(s['step_number']),
                s['step_name'],
                f"[{status_style}]{s['status']}[/{status_style}]",
                s['started_at'] or "-",
                s['completed_at'] or "-"
            )
        
        self.console.print(table)
        self.console.print("[dim]Dica: Use '/status 5' ou '/status 6' para ver detalhes das tabelas.[/dim]")

    def do_run(self, args):
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa.[/yellow]")
            return
        
        mig_info = self.db.get_migration_by_seq(self.current_seq)
        self.runner = StepRunner(mig_info['id'], self.db, self.config)
        
        # Registra os steps na pipeline (S04 REMOVIDO, reindexando os demais)
        self.runner.add_step(PrecheckStep, 0)
        self.runner.add_step(CreateDatabaseStep, 1)
        self.runner.add_step(ImportSchemaStep, 2)
        self.runner.add_step(ComparePreStep, 3)
        self.runner.add_step(DisableConstraintsStep, 4) # Antes era 5
        self.runner.add_step(MigrateBigStep, 5)          # Antes era 6
        self.runner.add_step(MigrateSmallStep, 6)
        self.runner.add_step(EnableConstraintsStep, 7)
        self.runner.add_step(SequencesStep, 8)
        self.runner.add_step(ComparePostStep, 9)
        self.runner.add_step(ValidateStep, 10)
        self.runner.add_step(AnalyzeStep, 11)
        self.runner.add_step(ReportStep, 12)
        
        start_at = 0
        if args:
            try:
                start_at = int(args[0])
            except ValueError:
                self.console.print("[red]O argumento de /run deve ser o número do step.[/red]")
                return

        # Verificar se constraints estão desabilitadas de execução anterior
        steps = self.db.list_steps(mig_info['id'])
        disable_done = any(s['step_name'] == 'DISABLE_CONSTRAINTS' and s['status'] == 'completed' for s in steps)
        enable_done = any(s['step_name'] == 'ENABLE_CONSTRAINTS' and s['status'] == 'completed' for s in steps)
        if disable_done and not enable_done:
            self.console.print(
                "[bold red]AVISO:[/bold red] Constraints estão desabilitadas de execução anterior!\n"
                "Se continuar sem re-habilitar, dados podem ser inseridos sem validação FK/PK.\n"
                "Para corrigir: [bold]python enable_constraints.py[/bold] ou [bold]/run 7[/bold]"
            )
            confirm = self.session.prompt("Continuar mesmo assim? (s/N): ").strip().lower()
            if confirm != 's':
                return

        self.runner.run_all(start_at=start_at)

    def do_rerun(self, args):
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa.[/yellow]")
            return
        
        if not args:
            self.console.print("[red]Uso: /rerun <step_number>[/red]")
            return
            
        try:
            step_num = int(args[0])
        except ValueError:
            self.console.print("[red]O argumento de /rerun deve ser o número do step.[/red]")
            return

        mig_info = self.db.get_migration_by_seq(self.current_seq)
        step = self.db.get_step(mig_info['id'], step_num)
        
        if not step:
            self.console.print(f"[red]Passo {step_num} não encontrado.[/red]")
            return
            
        # Confirmação antes de destruir estado
        confirm = self.session.prompt(
            f"Resetar step {step_num} ({step['step_name']}) e re-executar? "
            f"Arquivos de estado serão apagados. (s/N): "
        ).strip().lower()
        if confirm != 's':
            self.console.print("[yellow]Operação cancelada.[/yellow]")
            return

        # [MELHORIA] Identifica tabelas para limpar progresso físico e no banco
        self.console.print(f"[yellow]Resetando status do passo {step_num} ({step['step_name']}) para 'pending'...[/yellow]")
        self.db.update_step(mig_info['id'], step_num, 'pending')
        
        # Reset de tabelas no banco mestre (SQLite)
        if step_num == 5: # MigrateBigStep
            self.db.reset_tables(mig_info['id'], category='big')
            self.db.reset_tables(mig_info['id'], category='parallel_dbkey')
            self.console.print("[green]Status das Big Tables resetado no banco mestre.[/green]")
        elif step_num == 6: # MigrateSmallStep
            self.db.reset_tables(mig_info['id'], category='small')
            self.console.print("[green]Status das Small Tables resetado no banco mestre.[/green]")

        # Limpeza de arquivos de estado (.db) para forçar o TRUNCATE nos scripts paralelos
        mig_dir = self.project.get_migration_dir(self.current_seq)
        work_dir = mig_dir
        
        patterns = []
        if step_num == 5: # MigrateBigStep
            patterns = ["migration_state_*.db"] # Limpa todos os estados (grandes e pequenas)
        elif step_num == 6: # MigrateSmallStep
            patterns = ["migration_state_*.db"] # Limpa todos os estados de small tables
            
        cleaned_count = 0
        for pattern in patterns:
            for f in work_dir.glob(pattern):
                try:
                    # Não apaga o migration.db principal!
                    if f.name == "migration.db": continue
                    f.unlink()
                    cleaned_count += 1
                except Exception as e:
                    self.console.print(f"[dim][WARN] Não foi possível remover {f.name}: {e}[/dim]")
        
        if cleaned_count > 0:
            self.console.print(f"[green]Sucesso: {cleaned_count} arquivo(s) de estado limpos. A próxima execução fará um fresh start.[/green]")

        # Chama o do_run a partir deste passo
        self.do_run([str(step_num)])

    def do_rerun_only(self, args):
        """Reseta e executa SOMENTE o passo especificado, sem pipeline."""
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa.[/yellow]")
            return
        
        if not args:
            self.console.print("[red]Uso: /rerun-only <step_number>[/red]")
            return
            
        try:
            step_num = int(args[0])
        except ValueError:
            self.console.print("[red]O argumento de /rerun-only deve ser o número do step.[/red]")
            return

        mig_info = self.db.get_migration_by_seq(self.current_seq)
        step = self.db.get_step(mig_info['id'], step_num)
        
        if not step:
            self.console.print(f"[red]Passo {step_num} não encontrado.[/red]")
            return
            
        # Confirmação
        confirm = self.session.prompt(
            f"Resetar e executar SOMENTE o passo {step_num} ({step['step_name']})? (s/N): "
        ).strip().lower()
        if confirm != 's':
            self.console.print("[yellow]Operação cancelada.[/yellow]")
            return

        # Reset de status do step
        self.db.update_step(mig_info['id'], step_num, 'pending')
        
        # Se for passo de dados, reset de tabelas e arquivos
        if step_num in (5, 6):
            cat = 'big' if step_num == 5 else 'small'
            self.db.reset_tables(mig_info['id'], category=cat)
            if step_num == 5:
                self.db.reset_tables(mig_info['id'], category='parallel_dbkey')
            
            mig_dir = self.project.get_migration_dir(self.current_seq)
            for f in mig_dir.glob("migration_state_*.db"):
                try: f.unlink()
                except: pass
            self.console.print(f"[green]Estado das tabelas de {cat} limpo.[/green]")

        # Executa isoladamente
        self.runner = StepRunner(mig_info['id'], self.db, self.config)
        # Registra os steps na pipeline para que o runner saiba qual classe instanciar
        self.runner.add_step(PrecheckStep, 0)
        self.runner.add_step(CreateDatabaseStep, 1)
        self.runner.add_step(ImportSchemaStep, 2)
        self.runner.add_step(ComparePreStep, 3)
        self.runner.add_step(DisableConstraintsStep, 4)
        self.runner.add_step(MigrateBigStep, 5)
        self.runner.add_step(MigrateSmallStep, 6)
        self.runner.add_step(EnableConstraintsStep, 7)
        self.runner.add_step(SequencesStep, 8)
        self.runner.add_step(ComparePostStep, 9)
        self.runner.add_step(ValidateStep, 10)
        self.runner.add_step(AnalyzeStep, 11)
        self.runner.add_step(ReportStep, 12)

        self.runner.run_one(step_num)

    def do_reset_table(self, args):
        """Reseta o status de uma única tabela para forçar re-migração."""
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa.[/yellow]")
            return
        
        if not args:
            self.console.print("[red]Uso: /reset-table <nome_tabela>[/red]")
            return
            
        table_name = args[0]
        mig_info = self.db.get_migration_by_seq(self.current_seq)
        
        # 1. Reseta no banco Maestro e descobre a categoria
        category = self.db.reset_table_status(mig_info['id'], table_name)
        
        if not category:
            self.console.print(f"[red]Tabela '{table_name}' não encontrada nesta migração.[/red]")
            return

        # 2. Apaga arquivo físico de checkpoint
        mig_dir = self.project.get_migration_dir(self.current_seq)
        # Tenta vários padrões de nome comuns
        patterns = [
            f"migration_state_{table_name.lower()}.db",
            f"migration_state_{table_name.upper()}.db",
            f"migration_state_{table_name}.db"
        ]
        
        deleted_file = False
        for p in patterns:
            f = mig_dir / p
            if f.exists():
                try:
                    f.unlink()
                    deleted_file = True
                except: pass
        
        # 3. Sincroniza o status do Step pai para 'pending'
        # Assim o /run não pulará o passo.
        parent_step = 5 if category in ('big', 'parallel_dbkey', 'parallel_pk') else 6
        self.db.update_step(mig_info['id'], parent_step, 'pending')
        
        msg = f"[green]Tabela {table_name} resetada com sucesso![/green]\n"
        msg += f"[dim]- Categoria: {category}[/dim]\n"
        msg += f"[dim]- Passo {parent_step} marcado como pendente para permitir re-migração.[/dim]"
        if deleted_file:
            msg += "\n[dim]- Arquivo de checkpoint físico removido.[/dim]"
            
        self.console.print(Panel(msg, title="Reset Cirúrgico"))

    def do_ignore(self, args):
        """Marca uma tabela como concluída manualmente para destravar a pipeline."""
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa.[/yellow]")
            return
        
        if not args:
            self.console.print("[red]Uso: /ignore <nome_tabela>[/red]")
            return
            
        table_name = args[0]
        mig_info = self.db.get_migration_by_seq(self.current_seq)
        
        # 1. Marca como ignorada no banco
        category = self.db.ignore_table(mig_info['id'], table_name)
        
        if not category:
            self.console.print(f"[red]Tabela '{table_name}' não encontrada.[/red]")
            return

        # 2. Sincroniza o Step pai para 'pending' para re-avaliação
        parent_step = 5 if category in ('big', 'parallel_dbkey', 'parallel_pk') else 6
        self.db.update_step(mig_info['id'], parent_step, 'pending')
        
        self.console.print(f"[green][OK] Tabela {table_name} marcada como concluída manualmente.[/green]")
        self.console.print(f"[dim]O passo {parent_step} foi marcado como pendente para que o próximo /run avance na pipeline.[/dim]")

    def do_monitor(self):
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa.[/yellow]")
            return
        
        mig_dir = self.project.get_migration_dir(self.current_seq)

        import subprocess
        try:
            subprocess.run([sys.executable, 'monitor.py', str(mig_dir.absolute())])
        except KeyboardInterrupt:
            pass
