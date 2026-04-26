import sys
import re
import subprocess
import asyncio
import psycopg2
from pathlib import Path
from .base import StepBase
from ..ai.agent import MigrationAIAgent

_ANSI_RE = re.compile(r'\x1b\[[0-9;]*[mGKHFABCDJsu]|\x1b[)(][AB]|\r')

def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub('', text)

class ComparePreStep(StepBase):
    """Compara a estrutura FB vs PG antes da migração."""

    def _apply_sql_fixes(self, sql_path: Path):
        """Executa o script SQL de correção no PostgreSQL."""
        try:
            print(f"Executando script de correção: {sql_path.name}...")
            pg = self.config.postgres
            conn = psycopg2.connect(
                host=pg['host'], 
                port=pg.get('port', 5432),
                database=pg['database'],
                user=pg['user'], 
                password=pg['password']
            )
            conn.autocommit = True
            cur = conn.cursor()
            
            with open(sql_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Executa os comandos
            for statement in sql_content.split(';'):
                stmt = statement.strip()
                if not stmt or stmt.startswith('--'):
                    continue
                try:
                    cur.execute(stmt)
                    print(f"  [OK] Executado: {stmt[:60]}...")
                except Exception as e:
                    print(f"  [WARNING] Falha ao executar: {str(e)}")

            cur.close()
            conn.close()
            print("[OK] Correções aplicadas com sucesso.")
        except Exception as e:
            print(f"[ERROR] Falha ao aplicar correções: {e}")

    def _gerar_ddl_correcao(self, mig_dir: Path):
        """
        Executa gera_ddl_correcao_schema.py, exibe o DDL gerado e aguarda ação do DBA.

        Retorna:
            'continue' — prosseguir pipeline
            'exit'     — bloquear pipeline (usuário quer sair)
            None       — usuário pediu análise pela IA
        """
        MAX_TENTATIVAS = 3
        cmd_ddl = [
            sys.executable, 'gera_ddl_correcao_schema.py',
            '--work-dir', str(mig_dir.absolute()),
        ]
        cmd_recheck = [
            sys.executable, 'compara_estrutura_fb2pg.py',
            '--work-dir', str(mig_dir.absolute()),
            '--skip-count',
        ]

        for tentativa in range(1, MAX_TENTATIVAS + 1):
            print(f"\n[DDL] Gerando script de correção (tentativa {tentativa}/{MAX_TENTATIVAS})...")

            ddl_path = None
            try:
                proc = subprocess.Popen(
                    cmd_ddl, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1, encoding='utf-8', errors='replace'
                )
                if proc.stdout:
                    for line in proc.stdout:
                        print(line, end='', flush=True)
                        if line.startswith("DDL_PATH:"):
                            ddl_path = line.split("DDL_PATH:", 1)[1].strip()
                proc.wait()
            except Exception as e:
                print(f"[ERROR] Falha ao executar gera_ddl_correcao_schema.py: {e}")
                return None

            if proc.returncode == 1:
                print("\n[OK] Schemas idênticos — nenhuma diferença encontrada!")
                return 'continue'

            if proc.returncode == 2:
                print("\n[INFO] Apenas diferenças manuais (sem DDL automático gerado). Revise os comentários no arquivo DDL.")
                resp = input("Deseja analisar as diferenças restantes com o Agente ADK? (s/N): ").strip().lower()
                return None if resp == 's' else 'continue'

            # returncode == 0: DDL gerado com correções automáticas
            if ddl_path:
                print(f"\n{'=' * 62}")
                print(f"  DDL PRONTO: {ddl_path}")
                print(f"{'=' * 62}")
                pg = self.config.postgres
                print("\nExecute em um novo terminal:")
                print(f"  psql -h {pg.get('host')} -p {pg.get('port', 5432)} "
                      f"-U {pg.get('user')} -d {pg.get('database')} -f \"{ddl_path}\"")
                print("\nApós aplicar o DDL, volte aqui e:")

            resp = input("\n  ENTER = recheck  |  'pular' = continuar sem recheck  |  'sair' = bloquear pipeline\n>> ").strip().lower()

            if resp == 'pular':
                print("[INFO] Continuando sem recheck.")
                return 'continue'
            if resp == 'sair':
                print("[INFO] Use /rerun 3 após corrigir manualmente.")
                return 'exit'

            # Recheck
            print(f"\n[RECHECK {tentativa}/{MAX_TENTATIVAS}] Recomparando estrutura...")
            recheck_proc = subprocess.Popen(
                cmd_recheck, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, encoding='utf-8', errors='replace'
            )
            if recheck_proc.stdout:
                for line in recheck_proc.stdout:
                    print(line, end='', flush=True)
            recheck_proc.wait()

            if recheck_proc.returncode == 0:
                print("\n[OK] Schemas idênticos após correção!")
                return 'continue'

            if tentativa < MAX_TENTATIVAS:
                print(f"\n[INFO] Ainda há diferenças. Gerando novo DDL (tentativa {tentativa + 1})...")
            else:
                print(f"\n[WARNING] Ainda há diferenças após {MAX_TENTATIVAS} tentativas.")
                resp = input("Analisar com Agente ADK? (s/N): ").strip().lower()
                return None if resp == 's' else 'continue'

        return 'continue'

    def run(self) -> bool:
        print("--- Comparando Estrutura (Pré-Migração) ---")
        print("[INFO] Esta etapa pode demorar alguns minutos dependendo do número de tabelas e contagem de registros.")
        
        mig_info = self.db.get_migration(self.migration_id)
        mig_dir = Path(f"MIGRACAO_{mig_info['seq']}")
        config_path = mig_dir / "config.yaml"
        
        # Executa compara_estrutura_fb2pg.py com streaming de output
        cmd = [
            sys.executable, 'compara_estrutura_fb2pg.py',
            '--work-dir', str(mig_dir.absolute()),
            '--skip-count'
        ]
        
        print(f"[DEBUG] Executando comando: {' '.join(cmd)}")
        
        full_output = []
        try:
            # Usando Popen para ler o output linha a linha em tempo real
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                text=True, bufsize=1, encoding='utf-8', errors='replace'
            )
            
            if process.stdout:
                for line in process.stdout:
                    print(line, end='', flush=True)
                    full_output.append(line)
            
            process.wait()
            output = "".join(full_output)

            # Salva log sem códigos ANSI (compara_estrutura usa force_terminal=True no rich)
            log_path = mig_dir / "logs" / "compare_pre.log"
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(_strip_ansi(output))
            
            print(f"\n[OK] Comparação finalizada. Log completo em: [bold cyan]{log_path.absolute()}[/bold cyan]")

            if process.returncode != 0:
                # Se o banco não existe, não faz sentido chamar a IA para analisar diferenças
                if "nao existe no PostgreSQL" in output:
                    print("[INFO] Ignorando análise da IA pois o banco de destino ainda não foi criado.")
                    return True

                # Se o banco existe mas está vazio (zero tabelas em comum), também pula a análise de diferenças de esquema
                if "Total de tabelas       ║        0" in output or "Total de tabelas comparadas: 0" in output:
                    print("[INFO] O banco de destino está vazio. Pule este passo ou rode o Step 2 (IMPORT_SCHEMA) para criar as tabelas.")
                    return True

                print("[WARNING] Diferenças encontradas na estrutura.")

                print("\nO que deseja fazer?")
                print("  [1] Gerar DDL de correção automática (recomendado)")
                print("  [2] Analisar com Agente ADK (IA)")
                print("  [3] Continuar mesmo assim")
                print("  [4] Sair (corrigir manualmente e usar /rerun 3)")
                opcao = input("\nEscolha [1/2/3/4] (default=1): ").strip() or "1"

                if opcao == "1":
                    resultado = self._gerar_ddl_correcao(mig_dir)
                    if resultado == 'continue':
                        return True
                    if resultado == 'exit':
                        return False
                    # None = usuário pediu IA, cai no bloco abaixo
                elif opcao == "3":
                    print("[INFO] Continuando com possíveis discrepâncias.")
                    return True
                elif opcao == "4":
                    print("[INFO] Corrija manualmente e use /rerun 3 para re-executar este passo.")
                    return False
                elif opcao != "2":
                    print("[INFO] Opção inválida — continuando sem correção.")
                    return True

                # Integração com o Agente ADK
                try:
                    print("Solicitando análise do Agente ADK para as diferenças encontradas...")

                    full_log_content = log_path.read_text(encoding='utf-8')

                    diff_context = full_log_content
                    if "DETALHAMENTO DE DIFERENCAS" in full_log_content:
                        diff_context = full_log_content.split("DETALHAMENTO DE DIFERENCAS")[1]

                    # Filtra apenas linhas relevantes (diferenças reais) e trunca para evitar
                    # prompts gigantes que tornam o LLM extremamente lento
                    MAX_DIFF_CHARS = 8000
                    diff_lines = diff_context.strip().split('\n')
                    interesting = [l for l in diff_lines if any(
                        kw in l for kw in ['DIFERENTE', 'AUSENTE', 'FK-', 'ERRO', 'WARNING', 'FALTANDO']
                    )]
                    diff_context_filtrado = '\n'.join(interesting) if interesting else '\n'.join(diff_lines)
                    if len(diff_context_filtrado) > MAX_DIFF_CHARS:
                        diff_context_filtrado = (
                            diff_context_filtrado[:MAX_DIFF_CHARS]
                            + f"\n\n[... truncado em {MAX_DIFF_CHARS} chars — log completo em {log_path.name} ...]"
                        )

                    user_input = f"""Foram encontradas diferenças na estrutura entre Firebird e Postgres durante a fase PRÉ-MIGRAÇÃO.
Analise as diferenças abaixo e gere um script SQL corretivo (ALTER TABLE) para aplicar no Postgres.

REQUISITOS DO SCRIPT:
1. Gere o script passo a passo.
2. Para cada ajuste, inclua uma explicação detalhada em formato de comentário SQL (-- ou /* ... */).
3. O foco são apenas as diferenças em tabelas que já existem no destino.

DIFERENÇAS:
{diff_context_filtrado}
"""

                    LLM_TIMEOUT = 300  # timeout por chamada LLM (5 min)

                    async def chat_with_agent():
                        agent = MigrationAIAgent(project_path=str(mig_dir.absolute()))
                        session_id = f"migracao_{self.migration_id}_{self.step_number}"
                        loop = asyncio.get_event_loop()

                        # Primeira chamada LLM — timeout individual, não engloba input humano
                        try:
                            suggestion = await asyncio.wait_for(
                                agent.execute_task(session_id, user_input),
                                timeout=LLM_TIMEOUT
                            )
                        except asyncio.TimeoutError:
                            return "llm_timeout"

                        print("\n[AI AGENT SUGGESTION]")
                        print(suggestion)

                        sql_fix_path = mig_dir / "sql" / "ai_schema_fixes.sql"
                        with open(sql_fix_path, 'w', encoding='utf-8') as f:
                            f.write(suggestion)
                        print(f"Sugestão salva em {sql_fix_path}")

                        # Loop Interativo — input() via executor para não bloquear o event loop
                        while True:
                            print("\nOPÇÕES: [check] checar novamente | [pular] seguir sem checar | ou pergunte à IA.")
                            user_cmd = (await loop.run_in_executor(None, input, "(Agente ADK) >> ")).strip()

                            if not user_cmd or user_cmd.lower() == 'check':
                                print("[INFO] Reiniciando comparação de estrutura...")
                                return "check"

                            if user_cmd.lower() in ['pular verificacao', 'pular', 'continuar']:
                                print("[WARNING] Continuando com possíveis discrepâncias. Use por sua conta e risco.")
                                return "continue"

                            try:
                                response = await asyncio.wait_for(
                                    agent.execute_task(session_id, user_cmd),
                                    timeout=LLM_TIMEOUT
                                )
                            except asyncio.TimeoutError:
                                print(f"[ERROR] LLM não respondeu em {LLM_TIMEOUT}s. Tente novamente.")
                                continue

                            print(f"\n[AI AGENT]\n{response}")

                            if "```sql" in response:
                                sql_content = response.split("```sql")[1].split("```")[0].strip()
                                with open(sql_fix_path, 'w', encoding='utf-8') as f:
                                    f.write(sql_content)
                                print(f"Script SQL atualizado em: {sql_fix_path}")
                                print("[DICA] Aplique no pgAdmin/psql e depois digite 'check' aqui.")

                    try:
                        action = asyncio.run(chat_with_agent())
                    except Exception as ai_err:
                        print(f"[ERROR] Falha ao interagir com o Agente ADK: {ai_err}")
                        return False

                    if action == "llm_timeout":
                        print(f"[ERROR] LLM não respondeu em {LLM_TIMEOUT}s. Use /run 3 no Maestro para re-tentar este step.")
                        return False
                    if action == "check":
                        print("[INFO] Use /run 3 no Maestro para re-executar este step após aplicar as correções.")
                        return False

                except Exception as ai_err:
                    print(f"[ERROR] Falha ao interagir com o Agente ADK: {ai_err}")
                    return False

                return True
            
            print("[OK] Estruturas idênticas.")
            return True
            
        except Exception as e:
            print(f"[ERROR] Erro ao executar comparação: {str(e)}")
            return False
