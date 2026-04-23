import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional


class MigrationDB:
    """Banco SQLite centralizado para orquestração Maestro V2."""

    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _read_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS migration_meta (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seq TEXT NOT NULL,
                status TEXT DEFAULT 'created',
                config_yaml TEXT,
                schema_sql_path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                completed_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_id INTEGER REFERENCES migration_meta(id),
                step_number INTEGER NOT NULL,
                step_name TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                details_json TEXT
            );

            CREATE TABLE IF NOT EXISTS tables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_id INTEGER REFERENCES migration_meta(id),
                source_table TEXT NOT NULL,
                dest_table TEXT NOT NULL,
                category TEXT DEFAULT 'small',
                total_rows INTEGER DEFAULT 0,
                rows_migrated INTEGER DEFAULT 0,
                rows_failed INTEGER DEFAULT 0,
                current_batch INTEGER DEFAULT 0,
                total_batches INTEGER DEFAULT 0,
                batch_size INTEGER DEFAULT 5000,
                last_pk_value TEXT,
                pk_columns TEXT,
                use_db_key BOOLEAN DEFAULT 0,
                status TEXT DEFAULT 'pending',
                speed_rows_per_sec REAL DEFAULT 0,
                eta_seconds REAL,
                started_at TIMESTAMP,
                updated_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                worker_id TEXT
            );

            CREATE TABLE IF NOT EXISTS batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id INTEGER REFERENCES tables(id),
                batch_number INTEGER,
                rows_in_batch INTEGER,
                speed_rps REAL,
                eta_seconds REAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS constraints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_id INTEGER REFERENCES migration_meta(id),
                dest_table TEXT NOT NULL,
                constraint_type TEXT,
                constraint_name TEXT,
                sql_disable TEXT,
                sql_enable TEXT,
                status TEXT DEFAULT 'active',
                error_message TEXT
            );

            CREATE TABLE IF NOT EXISTS errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_id INTEGER REFERENCES migration_meta(id),
                step_number INTEGER,
                table_name TEXT,
                error_type TEXT,
                error_message TEXT,
                context_json TEXT,
                ai_suggestion TEXT,
                resolution TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_steps_migration ON steps(migration_id);
            CREATE INDEX IF NOT EXISTS idx_tables_migration ON tables(migration_id);
            CREATE INDEX IF NOT EXISTS idx_batches_table ON batches(table_id);
            CREATE INDEX IF NOT EXISTS idx_constraints_migration ON constraints(migration_id);
            CREATE INDEX IF NOT EXISTS idx_errors_migration ON errors(migration_id);
        """)
        conn.close()

    # ── migration_meta ────────────────────────────────────────────────────────

    def create_migration(self, seq: str, config_yaml: str = None,
                         schema_sql_path: str = None) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO migration_meta (seq, config_yaml, schema_sql_path) VALUES (?, ?, ?)",
                (seq, config_yaml, schema_sql_path)
            )
            return cur.lastrowid

    def get_migration(self, migration_id: int) -> Optional[dict]:
        conn = self._read_conn()
        row = conn.execute(
            "SELECT * FROM migration_meta WHERE id = ?", (migration_id,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_migration_by_seq(self, seq: str) -> Optional[dict]:
        conn = self._read_conn()
        row = conn.execute(
            "SELECT * FROM migration_meta WHERE seq = ? ORDER BY id DESC LIMIT 1", (seq,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def update_migration_status(self, migration_id: int, status: str):
        now = datetime.now().isoformat()
        completed_at = now if status in ('completed', 'failed') else None
        with self._conn() as conn:
            conn.execute(
                "UPDATE migration_meta SET status=?, updated_at=?, completed_at=? WHERE id=?",
                (status, now, completed_at, migration_id)
            )

    # ── steps ─────────────────────────────────────────────────────────────────

    def create_steps(self, migration_id: int, step_names: list):
        with self._conn() as conn:
            for i, name in enumerate(step_names):
                conn.execute(
                    "INSERT INTO steps (migration_id, step_number, step_name) VALUES (?, ?, ?)",
                    (migration_id, i, name)
                )

    def update_step(self, migration_id: int, step_number: int, status: str,
                    error_message: str = None, details: dict = None):
        now = datetime.now().isoformat()
        with self._conn() as conn:
            if status == 'running':
                conn.execute(
                    "UPDATE steps SET status=?, started_at=? WHERE migration_id=? AND step_number=?",
                    (status, now, migration_id, step_number)
                )
            else:
                conn.execute(
                    """UPDATE steps SET status=?, completed_at=?, error_message=?, details_json=?
                       WHERE migration_id=? AND step_number=?""",
                    (status, now, error_message,
                     json.dumps(details) if details else None,
                     migration_id, step_number)
                )

    def get_step(self, migration_id: int, step_number: int) -> Optional[dict]:
        conn = self._read_conn()
        row = conn.execute(
            "SELECT * FROM steps WHERE migration_id=? AND step_number=?",
            (migration_id, step_number)
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def list_steps(self, migration_id: int) -> list:
        conn = self._read_conn()
        rows = conn.execute(
            "SELECT * FROM steps WHERE migration_id=? ORDER BY step_number",
            (migration_id,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── tables ────────────────────────────────────────────────────────────────

    def upsert_table(self, migration_id: int, source_table: str, dest_table: str,
                     category: str = 'small', **kwargs) -> int:
        conn_r = self._read_conn()
        row = conn_r.execute(
            "SELECT id FROM tables WHERE migration_id=? AND source_table=?",
            (migration_id, source_table)
        ).fetchone()
        conn_r.close()

        if row:
            table_id = row['id']
            if kwargs:
                self.update_table(table_id, **kwargs)
            return table_id

        fields = ['migration_id', 'source_table', 'dest_table', 'category'] + list(kwargs.keys())
        placeholders = ', '.join('?' for _ in fields)
        vals = [migration_id, source_table, dest_table, category] + list(kwargs.values())
        with self._conn() as conn:
            cur = conn.execute(
                f"INSERT INTO tables ({', '.join(fields)}) VALUES ({placeholders})", vals
            )
            return cur.lastrowid

    def update_table(self, table_id: int, **kwargs):
        if not kwargs:
            return
        kwargs.setdefault('updated_at', datetime.now().isoformat())
        sets = ', '.join(f"{k}=?" for k in kwargs)
        vals = list(kwargs.values()) + [table_id]
        with self._conn() as conn:
            conn.execute(f"UPDATE tables SET {sets} WHERE id=?", vals)

    def get_table(self, table_id: int) -> Optional[dict]:
        conn = self._read_conn()
        row = conn.execute("SELECT * FROM tables WHERE id=?", (table_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_table_by_name(self, migration_id: int, source_table: str) -> Optional[dict]:
        conn = self._read_conn()
        row = conn.execute(
            "SELECT * FROM tables WHERE migration_id=? AND source_table=?",
            (migration_id, source_table)
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def list_tables(self, migration_id: int, status: str = None) -> list:
        conn = self._read_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM tables WHERE migration_id=? AND status=? ORDER BY id",
                (migration_id, status)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM tables WHERE migration_id=? ORDER BY id", (migration_id,)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── batches ───────────────────────────────────────────────────────────────

    def log_batch(self, table_id: int, batch_number: int, rows_in_batch: int,
                  speed_rps: float, eta_seconds: float):
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO batches (table_id, batch_number, rows_in_batch, speed_rps, eta_seconds)
                   VALUES (?, ?, ?, ?, ?)""",
                (table_id, batch_number, rows_in_batch, speed_rps, eta_seconds)
            )

    # ── constraints ───────────────────────────────────────────────────────────

    def add_constraint(self, migration_id: int, dest_table: str, constraint_type: str,
                       constraint_name: str, sql_disable: str, sql_enable: str) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                """INSERT INTO constraints
                   (migration_id, dest_table, constraint_type, constraint_name,
                    sql_disable, sql_enable)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (migration_id, dest_table, constraint_type, constraint_name,
                 sql_disable, sql_enable)
            )
            return cur.lastrowid

    def update_constraint_status(self, constraint_id: int, status: str,
                                  error_message: str = None):
        with self._conn() as conn:
            conn.execute(
                "UPDATE constraints SET status=?, error_message=? WHERE id=?",
                (status, error_message, constraint_id)
            )

    def list_constraints(self, migration_id: int, status: str = None) -> list:
        conn = self._read_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM constraints WHERE migration_id=? AND status=? ORDER BY id",
                (migration_id, status)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM constraints WHERE migration_id=? ORDER BY id",
                (migration_id,)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── errors ────────────────────────────────────────────────────────────────

    def log_error(self, migration_id: int, step_number: int, table_name: str,
                  error_type: str, error_message: str, context: dict = None) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                """INSERT INTO errors
                   (migration_id, step_number, table_name, error_type,
                    error_message, context_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (migration_id, step_number, table_name, error_type, error_message,
                 json.dumps(context) if context else None)
            )
            return cur.lastrowid

    def update_error_resolution(self, error_id: int, ai_suggestion: str = None,
                                 resolution: str = None):
        with self._conn() as conn:
            conn.execute(
                "UPDATE errors SET ai_suggestion=?, resolution=? WHERE id=?",
                (ai_suggestion, resolution, error_id)
            )

    def list_errors(self, migration_id: int, unresolved_only: bool = False) -> list:
        conn = self._read_conn()
        if unresolved_only:
            rows = conn.execute(
                "SELECT * FROM errors WHERE migration_id=? AND resolution IS NULL ORDER BY id",
                (migration_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM errors WHERE migration_id=? ORDER BY id", (migration_id,)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
