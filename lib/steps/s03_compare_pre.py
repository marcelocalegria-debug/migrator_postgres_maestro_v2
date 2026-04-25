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
            
            # Salva log
            log_path = mig_dir / "logs" / "compare_pre.log"
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(output)
            
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
                
                # [NOVO] Prompt de confirmação para análise da IA
                confirm_ai = input("\nDeseja que o Agente ADK analise as diferenças e gere um script SQL de correção? (s/N): ").lower()
                if confirm_ai != 's':
                    print("[INFO] Análise da IA cancelada pelo usuário. Prossiga com os ajustes manuais se necessário.")
                    return True

                # Integração com o Agente ADK
                try:
                    print("Solicitando análise do Agente ADK para as diferenças encontradas...")
                    
                    # [MELHORIA] Lê o arquivo de log COMPLETO para enviar para a IA
                    full_log_content = log_path.read_text(encoding='utf-8')
                    
                    diff_context = full_log_content
                    if "DETALHAMENTO DE DIFERENCAS" in full_log_content:
                         diff_context = full_log_content.split("DETALHAMENTO DE DIFERENCAS")[1]
                    
                    user_input = f"""Foram encontradas diferenças na estrutura entre Firebird e Postgres durante a fase PRÉ-MIGRAÇÃO.
Analise as diferenças abaixo e gere um script SQL corretivo (ALTER TABLE) para aplicar no Postgres.

REQUISITOS DO SCRIPT:
1. Gere o script passo a passo.
2. Para cada ajuste, inclua uma explicação detalhada em formato de comentário SQL (-- ou /* ... */).
3. O foco são apenas as diferenças em tabelas que já existem no destino.

DIFERENÇAS:
{diff_context}
"""
                    
                    # Executa de forma assíncrona usando asyncio.run
                    async def chat_with_agent():
                        agent = MigrationAIAgent(project_path=str(mig_dir.absolute()))
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
                            print("\nOPÇÕES: [check] para checar novamente (padrão), [pular verificacao] para seguir (não recomendado), ou digite sua dúvida para a IA.")
                            user_cmd = input("(Agente ADK) >> ").strip()
                            
                            if not user_cmd or user_cmd.lower() == 'check':
                                print("[INFO] Reiniciando comparação de estrutura...")
                                return "check"

                            if user_cmd.lower() in ['pular verificacao', 'pular', 'continuar']:
                                print("[WARNING] Continuando migração com possíveis discrepâncias de estrutura. Use por sua conta e risco.")
                                return "continue"
                            
                            # Conversa com a IA
                            response = await agent.execute_task(session_id, user_cmd)
                            print(f"\n[AI AGENT]\n{response}")
                            
                            # Se a IA respondeu com um bloco SQL, atualiza o arquivo
                            if "```sql" in response:
                                sql_content = response.split("```sql")[1].split("```")[0].strip()
                                with open(sql_fix_path, 'w', encoding='utf-8') as f:
                                    f.write(sql_content)
                                print(f"Script SQL atualizado em: {sql_fix_path}")
                                print(f"[DICA] Aplique o script acima manualmente no seu cliente SQL (pgAdmin, psql, etc) e depois digite 'check' aqui.")

                    action = asyncio.run(chat_with_agent())
                    if action == "check":
                        return self.run()
                    
                except Exception as ai_err:
                    print(f"[ERROR] Falha ao interagir com o Agente ADK: {ai_err}")
                
                return True
            
            print("[OK] Estruturas idênticas.")
            return True
            
        except Exception as e:
            print(f"[ERROR] Erro ao executar comparação: {str(e)}")
            return False
