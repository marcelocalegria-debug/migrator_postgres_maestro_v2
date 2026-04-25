import subprocess
import sys
from pathlib import Path
from .base import StepBase

class ReportStep(StepBase):
    """Gera o relatório final da migração em HTML."""

    def run(self) -> bool:
        print("--- Gerando Relatório Final ---")
        mig_info = self.db.get_migration(self.migration_id)
        mig_dir = Path(f"MIGRACAO_{mig_info['seq']}")
        
        # O script agora aceita --work-dir e resolve os caminhos internos
        cmd = [
            sys.executable, 'gera_relatorio_compara_estrutura_fb2pg_html.py',
            '--work-dir', str(mig_dir.absolute())
        ]
        
        output_html = mig_dir / "reports" / "relatorio_estrutura.html"
        
        try:
            print("Executando gera_relatorio_compara_estrutura_fb2pg_html.py...")
            process = subprocess.run(
                cmd, capture_output=True, text=True, check=False
            )
            
            if process.returncode == 0:
                print(f"[OK] Relatório gerado em {output_html}")
                return True
            else:
                print(f"[ERROR] Falha ao gerar relatório HTML (RC={process.returncode}).")
                return False
                
        except Exception as e:
            print(f"[ERROR] Exceção ao gerar relatório: {str(e)}")
            return False
