import os
import subprocess
from pathlib import Path
from .base import StepBase

class ImportSchemaStep(StepBase):
    """Importa o arquivo schema.sql no banco PostgreSQL."""

    def run(self) -> bool:
        pg = self.config.postgres
        target_db = pg['database']
        
        mig_info = self.db.get_migration(self.migration_id)
        mig_dir = Path(f"MIGRACAO_{mig_info['seq']}")
        schema_path = mig_dir / "schema.sql"
        print(f"--- Importando Schema: {schema_path} ---")

        if not schema_path.exists():
            print(f"[ERROR] schema.sql não encontrado.")
            return False

        # Prepara variáveis de ambiente para psql não pedir senha
        env = os.environ.copy()
        env['PGPASSWORD'] = pg['password']
        
        # Executa psql
        cmd = [
            'psql',
            '-h', pg['host'],
            '-p', str(pg.get('port', 5432)),
            '-U', pg['user'],
            '-d', target_db,
            '-f', str(schema_path.absolute())
        ]
        
        try:
            print(f"Executando: psql -h {pg['host']} -p {pg.get('port', 5432)} -U {pg['user']} -d {target_db} -f schema.sql")
            process = subprocess.run(
                cmd, env=env, capture_output=True, text=True, check=False
            )
            
            # Salva logs
            log_path = mig_dir / "logs" / "import_schema.log"
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write("--- STDOUT ---\n")
                f.write(process.stdout)
                f.write("\n--- STDERR ---\n")
                f.write(process.stderr)
            
            if process.returncode == 0:
                print(f"[OK] Schema importado com sucesso.")
                return True
            else:
                print(f"[ERROR] Falha ao importar schema (RC={process.returncode}).")
                print(f"Verifique o log em {log_path}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Exceção ao executar psql: {str(e)}")
            return False
