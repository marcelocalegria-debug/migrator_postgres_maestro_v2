import json
import sqlite3
import os
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Any, Union
from .db import MigrationDB

@dataclass
class MigrationProgress:
    source_table: str = ""
    dest_table: str = ""
    total_rows: int = 0
    rows_migrated: int = 0
    rows_failed: int = 0
    current_batch: int = 0
    total_batches: int = 0
    batch_size: int = 5000
    last_pk_value: Any = None          # restart checkpoint (pode ser list ou string)
    pk_columns: List[str] = field(default_factory=list)
    use_db_key: bool = False           # fallback sem PK
    last_db_key: bytes = None          # checkpoint RDB$DB_KEY
    status: str = "pending"            # pending|running|completed|failed|paused
    started_at: Optional[str] = None
    updated_at: Optional[str] = None
    completed_at: Optional[str] = None
    speed_rows_per_sec: float = 0.0
    eta_seconds: Optional[float] = None
    error_message: Optional[str] = None
    worker_id: Optional[str] = None
    category: str = "small"            # big|small|parallel_pk|parallel_dbkey

    def to_dict(self):
        d = asdict(self)
        # bytes e memoryview não são JSON-serializáveis
        if isinstance(d.get('last_db_key'), (bytes, memoryview)):
            d['last_db_key'] = d['last_db_key'].hex() if d['last_db_key'] else None
        
        # Garante que last_pk_value seja serializável (ex: bytes de GUIDs)
        if isinstance(d.get('last_pk_value'), (bytes, memoryview)):
             d['last_pk_value'] = d['last_pk_value'].hex()
        elif isinstance(d.get('last_pk_value'), list):
             d['last_pk_value'] = [v.hex() if isinstance(v, (bytes, memoryview)) else v for v in d['last_pk_value']]
             
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'MigrationProgress':
        valid = {k: v for k, v in d.items() if k in cls.__dataclass_fields__}
        if isinstance(valid.get('last_db_key'), str):
            try:
                valid['last_db_key'] = bytes.fromhex(valid['last_db_key'])
            except ValueError:
                pass
        
        # Re-parse JSON fields if they came from the master DB
        if isinstance(valid.get('pk_columns'), str):
            valid['pk_columns'] = json.loads(valid['pk_columns'])
        if isinstance(valid.get('last_pk_value'), str) and (valid['last_pk_value'].startswith('[') or valid['last_pk_value'].startswith('{')):
            valid['last_pk_value'] = json.loads(valid['last_pk_value'])
            
        return cls(**valid)

class StateManager:
    """Gerencia o estado da migração, suportando SQLite local ou MigrationDB centralizado."""

    def __init__(self, db_path: str | Path, migration_id: Optional[int] = None, table_name: Optional[str] = None):
        self.db_path = str(db_path)
        self.migration_id = migration_id
        self.table_name = table_name
        self.is_master = False
        self.master_db: Optional[MigrationDB] = None
        self.table_id: Optional[int] = None

        # Tenta detectar se é o master DB (MigrationDB) ou o SQLite legado
        if self._is_migration_db():
            self.is_master = True
            self.master_db = MigrationDB(self.db_path)
            if self.migration_id and self.table_name:
                # Carrega ou cria a entrada da tabela no master DB
                table_info = self.master_db.get_table_by_name(self.migration_id, self.table_name)
                if table_info:
                    self.table_id = table_info['id']
        else:
            self._init_local_db()

    def _is_migration_db(self) -> bool:
        """Verifica se o DB no path é um MigrationDB (tem tabela migration_meta)."""
        if not os.path.exists(self.db_path):
            return False
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='migration_meta'")
            res = cur.fetchone()
            conn.close()
            return res is not None
        except Exception:
            return False

    def _init_local_db(self):
        """Inicializa o banco SQLite local legado."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS migration_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                progress_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS migration_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                batch_number INTEGER,
                rows_in_batch INTEGER,
                total_rows INTEGER,
                speed_rps REAL,
                eta_seconds REAL,
                message TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_log_ts ON migration_log(timestamp);
        """)
        conn.commit()
        conn.close()

    def save_progress(self, p: MigrationProgress):
        if self.is_master:
            if not self.table_id:
                # Upsert inicial se não tiver table_id
                self.table_id = self.master_db.upsert_table(
                    self.migration_id, p.source_table, p.dest_table, p.category
                )
            
            # Mapeia MigrationProgress para os campos da tabela 'tables' do MigrationDB
            data = p.to_dict()
            update_fields = {
                'total_rows': p.total_rows,
                'rows_migrated': p.rows_migrated,
                'rows_failed': p.rows_failed,
                'current_batch': p.current_batch,
                'total_batches': p.total_batches,
                'batch_size': p.batch_size,
                'last_pk_value': json.dumps(p.last_pk_value) if p.last_pk_value is not None else None,
                'pk_columns': json.dumps(p.pk_columns),
                'use_db_key': p.use_db_key,
                'status': p.status,
                'speed_rows_per_sec': p.speed_rows_per_sec,
                'eta_seconds': p.eta_seconds,
                'started_at': p.started_at,
                'completed_at': p.completed_at,
                'error_message': p.error_message,
                'worker_id': p.worker_id
            }
            self.master_db.update_table(self.table_id, **update_fields)
        else:
            # SQLite Local Legado
            conn = sqlite3.connect(self.db_path, timeout=10)
            data = json.dumps(p.to_dict(), default=str)
            now = datetime.now().isoformat()
            conn.execute("""
                INSERT INTO migration_state (id, progress_json, updated_at)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    progress_json = excluded.progress_json,
                    updated_at    = excluded.updated_at
            """, (data, now))
            conn.commit()
            conn.close()

    def load_progress(self) -> Optional[MigrationProgress]:
        if self.is_master:
            if not self.migration_id or not self.table_name:
                return None
            row = self.master_db.get_table_by_name(self.migration_id, self.table_name)
            if row:
                return MigrationProgress.from_dict(dict(row))
            return None
        else:
            # SQLite Local Legado
            conn = sqlite3.connect(self.db_path, timeout=10)
            row = conn.execute(
                "SELECT progress_json FROM migration_state WHERE id=1"
            ).fetchone()
            conn.close()
            if row:
                return MigrationProgress.from_dict(json.loads(row[0]))
            return None

    def log_batch(self, batch_num, rows, speed, eta, msg=""):
        if self.is_master:
            if self.table_id:
                self.master_db.log_batch(self.table_id, batch_num, rows, speed, eta)
        else:
            # SQLite Local Legado
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.execute("""
                INSERT INTO migration_log
                    (timestamp, batch_number, rows_in_batch, total_rows,
                     speed_rps, eta_seconds, message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (datetime.now().isoformat(), batch_num, rows, 0, speed, eta, msg))
            conn.commit()
            conn.close()

    def reset(self):
        if self.is_master:
            if self.table_id:
                self.master_db.update_table(self.table_id, 
                    rows_migrated=0, rows_failed=0, current_batch=0, 
                    last_pk_value=None, status='pending', started_at=None,
                    completed_at=None, error_message=None
                )
                # Opcionalmente deletar batches? O PRD não especifica, vamos manter por ora.
        else:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.executescript("""
                DELETE FROM migration_state;
                DELETE FROM migration_log;
            """)
            conn.commit()
            conn.close()
