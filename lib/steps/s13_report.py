import subprocess
from pathlib import Path
from .base import StepBase

class ReportStep(StepBase):
    """Gera o relatório final da migração em HTML."""

    def run(self) -> bool:
        print("--- Gerando Relatório Final ---")
        mig_info = self.db.get_migration(self.migration_id)
        mig_dir = Path(f"MIGRACAO_{mig_info['seq']}")
        config_path = mig_dir / "config.yaml"
        output_html = mig_dir / "reports" / "relatorio_migracao.html"
        
        # O script original gera um relatório de estrutura
        # Vamos adaptá-lo ou usá-lo como base
        cmd = [
            'python', 'gera_relatorio_compara_estrutura_fb2pg_html.py',
            '--config', str(config_path.absolute()),
            '--output', str(output_html.absolute())
        ]
        
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
