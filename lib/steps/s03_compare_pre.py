import subprocess
import asyncio
from pathlib import Path
from .base import StepBase
from ..ai.agent import MigrationAIAgent

class ComparePreStep(StepBase):
    """Compara a estrutura FB vs PG antes da migração."""

    def run(self) -> bool:
        print("--- Comparando Estrutura (Pré-Migração) ---")
        mig_dir = Path(f"MIGRACAO_{self.db.get_migration(self.migration_id)['seq']}")
        config_path = mig_dir / "config.yaml"
        
        # Executa compara_estrutura_fb2pg.py
        cmd = [
            'python', 'compara_estrutura_fb2pg.py',
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
                
                # Integração com o novo Agente ADK
                try:
                    print("Solicitando análise do Agente ADK para as diferenças encontradas...")
                    
                    # Prepara o prompt para o agente
                    diff_context = output
                    if "DETALHAMENTO DE DIFERENCAS" in output:
                         diff_context = output.split("DETALHAMENTO DE DIFERENCAS")[1]
                    
                    user_input = f"""Foram encontradas diferenças na estrutura entre Firebird e Postgres durante a fase PRÉ-MIGRAÇÃO.
Analise as diferenças abaixo e gere um script SQL corretivo (ALTER TABLE) para aplicar no Postgres:

{diff_context}
"""
                    
                    # Executa de forma assíncrona usando asyncio.run
                    async def get_ai_fix():
                        audit_db = f"sqlite+aiosqlite:///{mig_dir.absolute().as_posix()}/migration_audit.db"
                        agent = MigrationAIAgent(db_audit_path=audit_db)
                        session_id = f"migracao_{self.migration_id}_{self.step_number}"
                        return await agent.execute_task(session_id, user_input)
                    
                    suggestion = asyncio.run(get_ai_fix())
                    
                    print("\n[AI AGENT SUGGESTION]")
                    print(suggestion)
                    
                    # Salva sugestão
                    sql_fix_path = mig_dir / "sql" / "ai_schema_fixes.sql"
                    with open(sql_fix_path, 'w', encoding='utf-8') as f:
                        f.write(suggestion)
                    print(f"Sugestão salva em {sql_fix_path}")
                    
                except Exception as ai_err:
                    print(f"[ERROR] Falha ao chamar o Agente ADK: {ai_err}")
                
                return True
            
            print("[OK] Estruturas idênticas.")
            return True
            
        except Exception as e:
            print(f"[ERROR] Erro ao executar comparação: {str(e)}")
            return False
