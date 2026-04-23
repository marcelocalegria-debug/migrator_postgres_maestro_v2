import sys
import subprocess
import psycopg2
from pathlib import Path
from .base import StepBase

class FixBlobsStep(StepBase):
    """Executa o script fix_blob_text_columns e aplica as correções no PostgreSQL."""

    def run(self) -> bool:
        print("--- Corrigindo Colunas BLOB TEXT ---")
        mig_info = self.db.get_migration(self.migration_id)
        mig_dir = Path(f"MIGRACAO_{mig_info['seq']}")
        config_path = mig_dir / "config.yaml"
        output_sql = mig_dir / "sql" / "fix_blob_to_text.sql"
        
        # 1. Gera o script SQL
        cmd = [
            sys.executable, 'fix_blob_text_columns.py',
            '--config', str(config_path.absolute()),
            '--output', str(output_sql.absolute())
        ]
        
        try:
            print(f"Executando fix_blob_text_columns.py...")
            process = subprocess.run(
                cmd, capture_output=True, text=True, check=False
            )
            
            if process.returncode != 0:
                print(f"[ERROR] Falha ao gerar script de correção de BLOBs: {process.stderr}")
                return False
                
            if not output_sql.exists():
                print("[OK] Nenhuma coluna BLOB SUB_TYPE 0 precisando de correção encontrada.")
                return True

            # 2. Executa o SQL no PostgreSQL
            print(f"Executando script de correção: {output_sql.name}...")
            pg = self.config.postgres
            conn = psycopg2.connect(
                host=pg['host'], database=pg['database'],
                user=pg['user'], password=pg['password']
            )
            conn.autocommit = True
            cur = conn.cursor()
            
            with open(output_sql, 'r', encoding='utf-8') as f:
                sql_content = f.read()
                
            # O script fix_blob_to_text.sql contém múltiplos comandos ALTER TABLE
            # Vamos executá-los um a um para ter mais controle.
            for statement in sql_content.split(';'):
                stmt = statement.strip()
                if not stmt or stmt.startswith('--'):
                    continue
                try:
                    cur.execute(stmt)
                    # print(f"  [OK] Executado: {stmt[:60]}...")
                except Exception as e:
                    print(f"  [WARNING] Falha ao executar '{stmt[:60]}...': {str(e)}")

            cur.close()
            conn.close()
            print("[OK] Processo de correção de BLOBs concluído.")
            return True
            
        except Exception as e:
            print(f"[ERROR] Exceção em FixBlobsStep: {str(e)}")
            return False
