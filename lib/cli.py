import os
from pathlib import Path
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
from .steps.s04_fix_blobs import FixBlobsStep
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
            "/init", "/resume", "/status", "/check", "/compare",
            "/monitor", "/run", "/help", "/quit"
        ]
        self.completer = WordCompleter(self.commands)
        self.session = PromptSession(completer=self.completer)
        
        self.style = Style.from_dict({
            'prompt': '#ansigreen bold',
            'seq': '#ansiyellow bold',
        })

    def display_welcome(self):
        self.console.print(Panel.fit(
            "[bold blue]MAESTRO V2[/bold blue] - Orquestrador de Migração Firebird 3 → PostgreSQL 18+",
            subtitle="Baseado no Plano de Implementação"
        ))

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
                        break
                    elif cmd == "/help":
                        self.show_help()
                    elif cmd == "/init":
                        self.do_init()
                    elif cmd == "/resume":
                        self.do_resume(args)
                    elif cmd == "/status":
                        self.do_status()
                    elif cmd == "/check":
                        self.do_check()
                    elif cmd == "/compare":
                        self.do_compare()
                    elif cmd == "/monitor":
                        self.do_monitor()
                    elif cmd == "/run":
                        self.do_run(args)
                    else:
                        self.console.print(f"[red]Comando desconhecido: {cmd}[/red]")
                else:
                    self.console.print("[yellow]Use comandos iniciados com / (ex: /help)[/yellow]")
            
            except KeyboardInterrupt:
                continue
            except EOFError:
                break
        
        self.console.print("[blue]Encerrando Maestro. Até logo![/blue]")

    def show_help(self):
        table = Table(title="Comandos Disponíveis")
        table.add_column("Comando", style="cyan")
        table.add_column("Descrição")
        table.add_row("/init", "Inicia uma nova migração (MIGRACAO_XXXX)")
        table.add_row("/resume <seq>", "Retoma uma migração existente")
        table.add_row("/status", "Mostra o progresso atual")
        table.add_row("/check", "Verifica conectividade e estrutura (FB vs PG)")
        table.add_row("/compare", "Compara estrutura entre bancos")
        table.add_row("/run [step]", "Executa um passo específico ou todos em sequência")
        table.add_row("/monitor", "Abre o dashboard de monitoramento")
        table.add_row("/help", "Mostra esta ajuda")
        table.add_row("/quit", "Sai do Maestro")
        self.console.print(table)

    def do_check(self):
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa. Use /init ou /resume primeiro.[/yellow]")
            return
        
        mig_info = self.db.get_migration_by_seq(self.current_seq)
        self.console.print(f"[bold cyan]--- Executando Verificações (Migration {self.current_seq}) ---[/bold cyan]")
        
        # PRECHECK é o step 0
        precheck = PrecheckStep(mig_info['id'], self.db, self.config, 0)
        if precheck.run():
            self.console.print("[bold green][OK] Conectividade e pré-requisitos validados.[/bold green]")
        else:
            self.console.print("[bold red][FAILED] Falha nos pré-requisitos básicos.[/bold red]")
            return

        # COMPARE_PRE é o step 3
        compare = ComparePreStep(mig_info['id'], self.db, self.config, 3)
        compare.run()

    def do_compare(self):
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa. Use /init ou /resume primeiro.[/yellow]")
            return
        
        mig_info = self.db.get_migration_by_seq(self.current_seq)
        # COMPARE_PRE é o step 3
        compare = ComparePreStep(mig_info['id'], self.db, self.config, 3)
        compare.run()

    def do_init(self):
        seq = self.project.get_next_seq()
        config_file = Path("config.yaml")
        if not config_file.exists():
            self.console.print("[red]Arquivo config.yaml não encontrado na raiz.[/red]")
            return

        schema_file = Path("schema.sql")
        
        # 1. Cria diretórios
        mig_dir = self.project.init_migration(seq, config_file, schema_file if schema_file.exists() else None)
        
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

        # 5. Registra os steps iniciais
        step_names = [
            'PRECHECK', 'CREATE_DATABASE', 'IMPORT_SCHEMA', 'COMPARE_PRE',
            'FIX_BLOBS', 'DISABLE_CONSTRAINTS', 'MIGRATE_BIG', 'MIGRATE_SMALL',
            'ENABLE_CONSTRAINTS', 'SEQUENCES', 'COMPARE_POST', 'VALIDATE',
            'ANALYZE', 'REPORT'
        ]
        mig_info = self.db.get_migration_by_seq(seq)
        self.db.create_steps(mig_info['id'], step_names)
        
        self.current_seq = seq
        self.console.print(f"[green]Migração {seq} inicializada em {mig_dir}[/green]")

    def do_resume(self, args):
        if not args:
            migrations = self.project.list_migrations()
            if not migrations:
                self.console.print("[yellow]Nenhuma migração encontrada.[/yellow]")
                return
            self.console.print(f"Migrações disponíveis: {', '.join(migrations)}")
            return
        
        seq = args[0]
        if not self.project.exists(seq):
            self.console.print(f"[red]Migração {seq} não encontrada.[/red]")
            return
        
        mig_dir = self.project.get_migration_dir(seq)
        self.current_seq = seq
        self.db = MigrationDB(mig_dir / "migration.db")
        self.config = MigrationConfig(mig_dir / "config.yaml")
        self.console.print(f"[green]Retomando migração {seq}[/green]")

    def do_status(self):
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa. Use /init ou /resume.[/yellow]")
            return
        
        mig_info = self.db.get_migration_by_seq(self.current_seq)
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

    def do_run(self, args):
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa.[/yellow]")
            return
        
        mig_info = self.db.get_migration_by_seq(self.current_seq)
        self.runner = StepRunner(mig_info['id'], self.db, self.config)
        
        # Registra todos os steps na pipeline
        self.runner.add_step(PrecheckStep, 0)
        self.runner.add_step(CreateDatabaseStep, 1)
        self.runner.add_step(ImportSchemaStep, 2)
        self.runner.add_step(ComparePreStep, 3)
        self.runner.add_step(FixBlobsStep, 4)
        self.runner.add_step(DisableConstraintsStep, 5)
        self.runner.add_step(MigrateBigStep, 6)
        self.runner.add_step(MigrateSmallStep, 7)
        self.runner.add_step(EnableConstraintsStep, 8)
        self.runner.add_step(SequencesStep, 9)
        self.runner.add_step(ComparePostStep, 10)
        self.runner.add_step(ValidateStep, 11)
        self.runner.add_step(AnalyzeStep, 12)
        self.runner.add_step(ReportStep, 13)
        
        start_at = 0
        if args:
            try:
                start_at = int(args[0])
            except ValueError:
                self.console.print("[red]O argumento de /run deve ser o número do step.[/red]")
                return
        
        self.runner.run_all(start_at=start_at)

    def do_monitor(self):
        if not self.current_seq:
            self.console.print("[yellow]Nenhuma migração ativa.[/yellow]")
            return
        
        # O monitor.py original pode ser usado ou adaptado.
        # Por simplicidade, vamos chamar o monitor.py passando o master-db.
        mig_dir = self.project.get_migration_dir(self.current_seq)
        master_db = mig_dir / "migration.db"
        
        import subprocess
        try:
            # Abre o monitor em um novo processo que assume o terminal
            subprocess.run(['python', 'monitor.py', '--db', str(master_db.absolute())])
        except KeyboardInterrupt:
            pass
