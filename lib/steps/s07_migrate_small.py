import subprocess
import sys
from pathlib import Path
from .base import StepBase

class MigrateSmallStep(StepBase):
    """Migra todas as tabelas pequenas em paralelo usando ProcessPoolExecutor."""

    def run(self) -> bool:
        print("--- Migrando Tabelas Pequenas ---")
        mig_info = self.db.get_migration(self.migration_id)
        mig_dir = Path(f"MIGRACAO_{mig_info['seq']}")
        config_path = mig_dir / "config.yaml"
        master_db = mig_dir / "migration.db"
        
        # Executa migrator_smalltables_v2.py
        cmd = [
            sys.executable, 'migrator_smalltables_v2.py',
            '--config', str(config_path.absolute()),
            '--small-tables',
            '--master-db', str(master_db.absolute()),
            '--migration-id', str(self.migration_id),
            '--work-dir', str(mig_dir.absolute()),
            '--workers', str(self.config.get('migration', {}).get('parallel_workers', 4))
        ]
        
        try:
            print(f"Iniciando carga paralela de tabelas pequenas...")
            log_path = mig_dir / "logs" / "migrate_small.stdout.log"
            with open(log_path, "w") as log_f:
                process = subprocess.run(
                    cmd, stdout=log_f, stderr=subprocess.STDOUT, text=True, check=False
                )
            
            if process.returncode == 0:
                print("[OK] Tabelas pequenas migradas.")
                return True
            else:
                print(f"[ERROR] Falha na migração das tabelas pequenas (RC={process.returncode}).")
                return False
                
        except Exception as e:
            print(f"[ERROR] Exceção em MigrateSmallStep: {str(e)}")
            return False
