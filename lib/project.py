import os
import shutil
from pathlib import Path
from typing import List, Optional

class MigrationProject:
    """Gerencia o diretório e workspace da migração (MIGRACAO_<SEQ>)."""

    def __init__(self, base_dir: str | Path = "."):
        self.base_dir = Path(base_dir)

    def get_next_seq(self) -> str:
        """Detecta o próximo número de sequência disponível (ex: '0001')."""
        existing = self.list_migrations()
        if not existing:
            return "0001"
        
        last_seq = int(existing[-1])
        return f"{last_seq + 1:04d}"

    def list_migrations(self) -> List[str]:
        """Lista as sequências de migração existentes (ex: ['0001', '0002'])."""
        migrations = []
        if not self.base_dir.exists():
            return []
            
        for d in self.base_dir.iterdir():
            if d.is_dir() and d.name.startswith("MIGRACAO_"):
                try:
                    seq = d.name.split("_")[1]
                    if len(seq) == 4 and seq.isdigit():
                        migrations.append(seq)
                except (IndexError, ValueError):
                    continue
        
        return sorted(migrations)

    def init_migration(self, seq: str, config_path: Path, schema_path: Optional[Path] = None) -> Path:
        """Cria a estrutura de diretórios para uma nova migração."""
        mig_dir = self.base_dir / f"MIGRACAO_{seq}"
        mig_dir.mkdir(parents=True, exist_ok=True)
        
        # Subdiretórios
        (mig_dir / "logs").mkdir(exist_ok=True)
        (mig_dir / "sql").mkdir(exist_ok=True)
        (mig_dir / "json").mkdir(exist_ok=True)
        (mig_dir / "reports").mkdir(exist_ok=True)
        
        # Copia config
        shutil.copy2(config_path, mig_dir / "config.yaml")
        
        # Copia schema se fornecido
        if schema_path and schema_path.exists():
            shutil.copy2(schema_path, mig_dir / "schema.sql")
        
        return mig_dir

    def get_migration_dir(self, seq: str) -> Path:
        """Retorna o path do diretório de uma migração existente."""
        return self.base_dir / f"MIGRACAO_{seq}"

    def exists(self, seq: str) -> bool:
        """Verifica se uma migração existe."""
        return self.get_migration_dir(seq).exists()
