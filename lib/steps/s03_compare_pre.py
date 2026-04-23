import sys
import subprocess
import asyncio
import psycopg2
from pathlib import Path
from .base import StepBase
from ..ai.agent import MigrationAIAgent

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

    def run(self) -> bool:
        print("--- Comparando Estrutura (Pré-Migração) ---")
        mig_info = self.db.get_migration(self.migration_id)
        mig_dir = Path(f"MIGRACAO_{mig_info['seq']}")
        config_path = mig_dir / "config.yaml"
        
        # Executa compara_estrutura_fb2pg.py
        cmd = [
            sys.executable, 'compara_estrutura_fb2pg.py',
            '--config', str(config_path.absolute())
        ]
        
        try:
            process = subprocess.run(
                cmd, capture_output=True, text=True, check=False
            )
            
            output = process.stdout
            print(output)
            
            # Salva log
            log_path = mig_dir / "logs" / "compare_pre.log"
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(output)
                if process.stderr:
                    f.write("\n--- STDERR ---\n")
                    f.write(process.stderr)

            if process.returncode != 0:
                print("[WARNING] Diferenças encontradas na estrutura.")
                
                # Integração com o Agente ADK
                try:
                    print("Solicitando análise do Agente ADK para as diferenças encontradas...")
                    
                    # Prepara o prompt para o agente
                    diff_context = output
                    if "DETALHAMENTO DE DIFERENCAS" in output:
                         diff_context = output.split("DETALHAMENTO DE DIFERENCAS")[1]
                    
                    user_input = f"""Foram encontradas diferenças na estrutura entre Firebird e Postgres durante a fase PRÉ-MIGRAÇÃO.
Analise as diferenças abaixo e gere um script SQL corretivo (ALTER TABLE) para aplicar no Postgres.
IMPORTANTE: Se as tabelas não existirem, ignore-as, pois o foco são apenas as diferenças em tabelas já existentes.

DIFERENÇAS:
{diff_context}
"""
                    
                    # Executa de forma assíncrona usando asyncio.run
                    async def chat_with_agent():
                        audit_db = f"sqlite+aiosqlite:///{mig_dir.absolute().as_posix()}/migration_audit.db"
                        agent = MigrationAIAgent(db_audit_path=audit_db)
                        session_id = f"migracao_{self.migration_id}_{self.step_number}"
                        
                        # Primeira interação
                        suggestion = await agent.execute_task(session_id, user_input)
                        
                        print("\n[AI AGENT SUGGESTION]")
                        print(suggestion)
                        
                        # Salva sugestão inicial
                        sql_fix_path = mig_dir / "sql" / "ai_schema_fixes.sql"
                        with open(sql_fix_path, 'w', encoding='utf-8') as f:
                            f.write(suggestion)
                        print(f"Sugestão salva em {sql_fix_path}")

                        # Loop Interativo (Human-in-the-Loop)
                        while True:
                            print("\nOPÇÕES: [aplicar] para executar o SQL, [continuar] para seguir a migração, ou digite sua dúvida para a IA.")
                            user_cmd = input("(Agente ADK) >> ").strip()
                            
                            if not user_cmd:
                                continue

                            if user_cmd.lower() in ['continuar', 'sair', 'pular']:
                                break
                            
                            if user_cmd.lower() == 'aplicar':
                                self._apply_sql_fixes(sql_fix_path)
                                break
                            
                            # Conversa com a IA
                            response = await agent.execute_task(session_id, user_cmd)
                            print(f"\n[AI AGENT]\n{response}")
                            
                            # Se a IA respondeu com um bloco SQL, atualiza o arquivo
                            if "```sql" in response:
                                sql_content = response.split("```sql")[1].split("```")[0].strip()
                                with open(sql_fix_path, 'w', encoding='utf-8') as f:
                                    f.write(sql_content)
                                print(f"Script SQL atualizado em: {sql_fix_path}")
                    
                    asyncio.run(chat_with_agent())
                    
                except Exception as ai_err:
                    print(f"[ERROR] Falha ao interagir com o Agente ADK: {ai_err}")
                
                return True
            
            print("[OK] Estruturas idênticas.")
            return True
            
        except Exception as e:
            print(f"[ERROR] Erro ao executar comparação: {str(e)}")
            return False
