import yaml
import os
from pathlib import Path
from typing import Any, Dict

class MigrationConfig:
    """Carregador e validador de configuração para a migração."""

    def __init__(self, yaml_path: str | Path):
        self.yaml_path = Path(yaml_path)
        self.data: Dict[str, Any] = {}
        self.load()

    def load(self):
        if not self.yaml_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.yaml_path}")
        
        with open(self.yaml_path, 'r', encoding='utf-8') as f:
            self.data = yaml.safe_load(f)
        
        # print(f"DEBUG: Loaded keys: {list(self.data.keys())}")
        self._validate()

    def _validate(self):
        required_sections = ['firebird']
        if 'postgres' not in self.data and 'postgresql' not in self.data:
            raise ValueError("Missing required section in config: postgres or postgresql")
        
        # Normalizar para 'postgres' internamente se vier como 'postgresql'
        if 'postgresql' in self.data and 'postgres' not in self.data:
            self.data['postgres'] = self.data['postgresql']

        fb = self.data['firebird']
        pg = self.data['postgres']
        
        required_fb = ['host', 'database', 'user', 'password']
        for field in required_fb:
            if field not in fb:
                raise ValueError(f"Missing required Firebird field: {field}")
        
        required_pg = ['host', 'database', 'user', 'password']
        for field in required_pg:
            if field not in pg:
                raise ValueError(f"Missing required PostgreSQL field: {field}")

    @property
    def firebird(self) -> Dict[str, Any]:
        return self.data.get('firebird', {})

    @property
    def postgres(self) -> Dict[str, Any]:
        return self.data.get('postgres', {})

    @property
    def ai(self) -> Dict[str, Any]:
        return self.data.get('ai', {})

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)
