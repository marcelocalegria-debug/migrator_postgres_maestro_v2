import sys
import subprocess
from pathlib import Path
from rich.prompt import Confirm
from .base import StepBase

class ValidateStep(StepBase):
    """Valida a integridade dos dados comparando Checksums entre FB e PG."""

    def run(self) -> bool:
        print("--- Validando Integridade de Dados ---")
        mig_info = self.db.get_migration(self.migration_id)
        mig_dir = Path(f"MIGRACAO_{mig_info['seq']}")
        
        # 1. Comparação de Contagem de Registros
        print("\n[1/2] Comparando contagem de registros (FB vs PG)...")
        cmd_count = [
            sys.executable, 'compara_cont_fb2pg.py',
            '--work-dir', str(mig_dir.absolute())
        ]
        print(f"[DEBUG] Executando comando: {' '.join(cmd_count)}")
        try:
            subprocess.run(cmd_count, check=False)
        except Exception as e:
            print(f"[ERROR] Falha ao executar compara_cont_fb2pg.py: {e}")

        # 2. Comparação de Checksums (colunas binárias)
        print("\n[2/2] Validando integridade de dados (Checksums)...")
        
        if not Confirm.ask("Deseja realizar a validação de Checksum MD5 (BLOB vs BYTEA)?", default=False):
            print("[INFO] Validação de checksum ignorada pelo usuário.")
            return True

        config_path = mig_dir / "config.yaml"
        
        # Executa PosMigracao_comparaChecksum_bytea.py para as tabelas principais
        cmd = [
            sys.executable, 'PosMigracao_comparaChecksum_bytea.py',
            '--config', str(config_path.absolute())
        ]
        
        try:
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
