import sys
import subprocess
from pathlib import Path
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

        # 2. Comparação de Checksums (colunas binárias) — automático
        print("\n[2/2] Validando integridade de dados (Checksums)...")

        config_path = mig_dir / "config.yaml"

        # Executa PosMigracao_comparaChecksum_bytea.py para as tabelas principais
        cmd = [
            sys.executable, 'PosMigracao_comparaChecksum_bytea.py',
            '--config', str(config_path.absolute()),
            '--sample', '100',
        ]
        
        log_path = mig_dir / "logs" / "validate_checksums.log"
        returncode = 0
        try:
            with open(log_path, 'w', encoding='utf-8') as log_f:
                with subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    bufsize=1,
                ) as proc:
                    for line in proc.stdout:
                        print(line, end='', flush=True)
                        log_f.write(line)
                    proc.wait()
                    returncode = proc.returncode
        except Exception as e:
            print(f"[ERROR] Erro ao executar validação: {str(e)}")
            return False

        if returncode != 0:
            print("[WARNING] Diferenças de checksum encontradas!")
        else:
            print("[OK] Checksums batem perfeitamente.")
        return True
