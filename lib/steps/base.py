import time
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Any, Dict
from ..db import MigrationDB
from ..config import MigrationConfig

class StepBase(ABC):
    """Classe base para todos os passos da pipeline de migração."""

    def __init__(self, migration_id: int, db: MigrationDB, config: MigrationConfig, step_number: int):
        self.migration_id = migration_id
        self.db = db
        self.config = config
        self.step_number = step_number
        self.name = self.__class__.__name__.replace("Step", "").lower()

    @abstractmethod
    def run(self) -> bool:
        """Executa o passo. Deve retornar True se sucesso, False se falha."""
        pass

    def skip(self):
        """Marca o passo como pulado."""
        self.db.update_step(self.migration_id, self.step_number, 'skipped')

    def log_error(self, message: str, context: Optional[Dict] = None):
        """Registra um erro no MigrationDB."""
        self.db.log_error(
            self.migration_id, self.step_number, 
            table_name=None, error_type='step_fail', 
            error_message=message, context=context
        )

class StepRunner:
    """Orquestrador que executa uma lista de passos em sequência."""

    def __init__(self, migration_id: int, db: MigrationDB, config: MigrationConfig):
        self.migration_id = migration_id
        self.db = db
        self.config = config
        self.steps: Dict[int, StepBase] = {}

    def add_step(self, step_class: type, step_number: int):
        step = step_class(self.migration_id, self.db, self.config, step_number)
        self.steps[step_number] = step

    def run_all(self, start_at: int = 0):
        sorted_steps = sorted(self.steps.keys())
        for num in sorted_steps:
            if num < start_at:
                continue
            
            step = self.steps[num]
            print(f"\n>>> Executando Passo {num}: {step.__class__.__name__}")
            
            # Verifica se o passo já foi concluído (resume)
            status = self.db.get_step(self.migration_id, num)
            if status and status['status'] == 'completed':
                print(f"Passo {num} já concluído. Pulando.")
                continue

            self.db.update_step(self.migration_id, num, 'running')
            
            try:
                success = step.run()
                if success:
                    self.db.update_step(self.migration_id, num, 'completed')
                    print(f"Passo {num} concluído com sucesso.")
                else:
                    self.db.update_step(self.migration_id, num, 'failed', error_message="Step returned False")
                    print(f"Passo {num} falhou.")
                    break
            except Exception as e:
                error_msg = f"Erro inesperado: {str(e)}"
                self.db.update_step(self.migration_id, num, 'failed', error_message=error_msg)
                step.log_error(error_msg)
                print(f"Passo {num} falhou com exceção: {error_msg}")
                import traceback
                traceback.print_exc()
                break
