import psycopg2
from .base import StepBase

class AnalyzeStep(StepBase):
    """Executa ANALYZE em todo o banco PostgreSQL para otimizar o planejador."""

    def run(self) -> bool:
        print("--- Executando ANALYZE (PostgreSQL) ---")
        try:
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
            
            print("Analyze em andamento (pode demorar em bancos grandes)...")
            cur.execute("ANALYZE VERBOSE")
            
            cur.close()
            conn.close()
            print("[OK] ANALYZE concluído.")
            return True
            
        except Exception as e:
            print(f"[ERROR] Falha ao executar ANALYZE: {str(e)}")
            return False
