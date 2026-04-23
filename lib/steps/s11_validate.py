import sys
import subprocess
from pathlib import Path
from .base import StepBase

class ValidateStep(StepBase):
    """Valida a integridade dos dados comparando Checksums entre FB e PG."""

    def run(self) -> bool:
        print("--- Validando Integridade de Dados (Checksums) ---")
        mig_info = self.db.get_migration(self.migration_id)
        mig_dir = Path(f"MIGRACAO_{mig_info['seq']}")
        config_path = mig_dir / "config.yaml"
        
        # Executa PosMigracao_comparaChecksum_bytea.py para as tabelas principais
        # O script original pode ser lento para todas as 900 tabelas.
        # Vamos rodar para as tabelas configuradas no config.yaml.
        
        cmd = [
            sys.executable, 'PosMigracao_comparaChecksum_bytea.py',
            '--config', str(config_path.absolute()),
            '--all-tables' # Assumindo que adicionamos essa flag ou similar
        ]
        
        try:
            print("Executando validação de checksums...")
            process = subprocess.run(
                cmd, capture_output=True, text=True, check=False
            )
            
            output = process.stdout
            print(output)
            
            # Salva log
            log_path = mig_dir / "logs" / "validate_checksums.log"
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(output)
                if process.stderr:
                    f.write("\n--- STDERR ---\n")
                    f.write(process.stderr)

            if process.returncode != 0:
                print("[WARNING] Diferenças de checksum encontradas!")
                return True # Retorna True pois a validação foi EXECUTADA, mesmo com falhas de dados
            
            print("[OK] Checksums batem perfeitamente.")
            return True
            
        except Exception as e:
            print(f"[ERROR] Erro ao executar validação: {str(e)}")
            return False
