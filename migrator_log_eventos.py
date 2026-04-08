#!/usr/bin/env python3
"""
migrator_log_eventos.py
========================
Migra LOG_EVENTOS (Firebird 3 → PostgreSQL) em N threads paralelas.

LOG_EVENTOS não tem PK nem FK — o particionamento usa RDB$DB_KEY,
o ponteiro físico de linha do Firebird (8 bytes, único, ordenável).
Cada thread migra um slice WHERE RDB$DB_KEY >= low [AND RDB$DB_KEY < high].

Uso:
    source .venv/bin/activate
    export PYTHONIOENCODING=utf-8
    python migrator_log_eventos.py --threads 8
    python migrator_log_eventos.py --threads 8 --reset
    python migrator_log_eventos.py --threads 8 --dry-run
    python migrator_log_eventos.py --threads 8 --batch-size 5000
    python migrator_log_eventos.py --threads 4 --use-insert
    python migrator_log_eventos.py --generate-scripts-only

Arquivos gerados:
    migration_state_log_eventos.db          → monitor.py (progresso agregado)
    migration_state_log_eventos_tN.db       → checkpoint individual por thread
    migration_log_eventos_tN.log            → log individual por thread
    migration_log_eventos_parallel.log      → log do orquestrador
    disable_constraints_log_eventos.sql
    enable_constraints_log_eventos.sql
    constraint_state_log_eventos.json

Monitor:
    python monitor.py                 # mostra todas as threads
    python monitor.py --big-tables    # inclui log_eventos e threads _tN
"""

import sys
import os
import io
import gc
import json
import time
import signal
import sqlite3
import logging
import argparse
import threading
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Any, List, Optional, Tuple

import yaml
import psycopg2

# ── Firebird DLL discovery (Windows) ────────────────────────────────────────
if os.name == 'nt' and hasattr(os, 'add_dll_directory'):
    try:
        os.add_dll_directory(os.path.abspath(os.path.dirname(__file__) or '.'))
    except Exception:
        pass

import fdb

if os.name == 'nt':
    for _p in [
        os.path.abspath(os.path.join(os.path.dirname(__file__) or '.', 'fbclient.dll')),
        r'C:\Program Files\Firebird\Firebird_3_0\fbclient.dll',
        r'C:\Program Files\Firebird\Firebird_4_0\fbclient.dll',
        r'C:\Program Files\Firebird\Firebird_5_0\fbclient.dll',
        r'C:\Program Files\Firebird\Firebird_2_5\bin\fbclient.dll',
        r'C:\Program Files (x86)\Firebird\Firebird_3_0\fbclient.dll',
        r'C:\Program Files (x86)\Firebird\Firebird_2_5\bin\fbclient.dll',
    ]:
        if os.path.exists(_p):
            try:
                fdb.load_api(_p)
                break
            except Exception:
                pass

from pg_constraints import ConstraintManager

BASE_DIR     = Path(__file__).parent
SOURCE_TABLE = 'LOG_EVENTOS'
DEST_TABLE   = 'log_eventos'


# ═══════════════════════════════════════════════════════════════
#  ESTRUTURAS DE DADOS  (idênticas ao migrator_parallel_doc_oper)
# ═══════════════════════════════════════════════════════════════

@dataclass
class MigrationProgress:
    source_table: str = ''
    dest_table: str = ''
    total_rows: int = 0
    rows_migrated: int = 0
    rows_failed: int = 0
    current_batch: int = 0
    total_batches: int = 0
    batch_size: int = 5000
    last_pk_value: Any = None
    pk_columns: List[str] = field(default_factory=list)
    use_db_key: bool = True          # LOG_EVENTOS nunca tem PK
    last_db_key: bytes = None        # checkpoint RDB$DB_KEY
    status: str = 'idle'
    started_at: Optional[str] = None
    updated_at: Optional[str] = None
    completed_at: Optional[str] = None
    elapsed_seconds: float = 0.0
    speed_rows_per_sec: float = 0.0
    eta_seconds: Optional[float] = None
    error_message: Optional[str] = None
    constraints_disabled: bool = False
    phase: str = 'idle'

    def to_dict(self):
        d = asdict(self)
        if isinstance(d.get('last_db_key'), (bytes, memoryview)):
            d['last_db_key'] = d['last_db_key'].hex() if d['last_db_key'] else None
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'MigrationProgress':
        valid = {k: v for k, v in d.items() if k in cls.__dataclass_fields__}
        if isinstance(valid.get('last_db_key'), str):
            valid['last_db_key'] = bytes.fromhex(valid['last_db_key'])
        return cls(**valid)


@dataclass
class ColumnMeta:
    name: str
    fb_type_code: int
    pg_type: str
    is_blob: bool = False
    blob_subtype: int = 0
    fb_charset: str = 'NONE'
    nullable: bool = True
    position: int = 0


# ═══════════════════════════════════════════════════════════════
#  STATE MANAGER  (idêntico ao migrator_parallel_doc_oper)
# ═══════════════════════════════════════════════════════════════

class StateManager:
    def __init__(self, db_path):
        self.db_path = str(db_path)
        self._init_db()

    def _conn(self):
        return sqlite3.connect(self.db_path, timeout=10)

    def _init_db(self):
        conn = self._conn()
        conn.executescript("""
            PRAGMA journal_mode=WAL;
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
        conn = self._conn()
        data = json.dumps(p.to_dict(), default=str)
        now  = datetime.now().isoformat()
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
        conn = self._conn()
        row  = conn.execute(
            'SELECT progress_json FROM migration_state WHERE id=1').fetchone()
        conn.close()
        return MigrationProgress.from_dict(json.loads(row[0])) if row else None

    def log_batch(self, batch, rows, total, speed, eta, msg=''):
        conn = self._conn()
        conn.execute("""
            INSERT INTO migration_log
                (timestamp, batch_number, rows_in_batch, total_rows,
                 speed_rps, eta_seconds, message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now().isoformat(), batch, rows, total, speed, eta, msg))
        conn.commit()
        conn.close()

    def reset(self):
        conn = self._conn()
        conn.executescript('DELETE FROM migration_state; DELETE FROM migration_log;')
        conn.commit()
        conn.close()


# ═══════════════════════════════════════════════════════════════
#  MAPEAMENTO DE TIPOS  (idêntico)
# ═══════════════════════════════════════════════════════════════

_FB = {7:'SMALLINT', 8:'INTEGER', 10:'REAL', 12:'DATE', 13:'TIME',
       14:'CHAR', 16:'BIGINT', 27:'DOUBLE PRECISION', 35:'TIMESTAMP',
       37:'VARCHAR', 261:'BLOB'}

_FB_CHARSET_TO_PYTHON = {
    'WIN1252':'cp1252', 'WIN1250':'cp1250', 'WIN1251':'cp1251',
    'ISO8859_1':'iso-8859-1', 'ISO8859_2':'iso-8859-2',
    'UTF8':'utf-8', 'UNICODE_FSS':'utf-8', 'ASCII':'ascii', 'NONE':'latin-1',
}
_CONFIG_CHARSET_TO_FB = {
    'iso-8859-1':'ISO8859_1', 'iso8859-1':'ISO8859_1', 'latin1':'ISO8859_1',
    'latin-1':'ISO8859_1', 'win1252':'WIN1252', 'windows-1252':'WIN1252',
    'cp1252':'WIN1252', 'utf-8':'UTF8', 'utf8':'UTF8',
}


def _fb_charset_for_connect(raw: str) -> str:
    return _CONFIG_CHARSET_TO_FB.get(raw.lower(), raw.upper())

def _fb_charset_to_python(fb_charset: str) -> str:
    return _FB_CHARSET_TO_PYTHON.get(fb_charset.upper(), 'latin-1')

def map_fb_to_pg(type_code, subtype, length, precision, scale):
    if type_code == 261:
        return ('TEXT', True) if subtype == 1 else ('BYTEA', True)
    if type_code in (8, 16) and precision > 0:
        return f'NUMERIC({precision},{abs(scale)})', False
    base = _FB.get(type_code, 'TEXT')
    if type_code in (14, 37):
        return f'{base}({length})', False
    return base, False

def _fmt_dur(seconds: float) -> str:
    if seconds is None or seconds < 0:
        return 'N/A'
    seconds = int(seconds)
    h, r = divmod(seconds, 3600)
    m, s = divmod(r, 60)
    if h: return f'{h}h{m:02d}m{s:02d}s'
    if m: return f'{m}m{s:02d}s'
    return f'{s}s'


# ═══════════════════════════════════════════════════════════════
#  BLOB CONVERSION  (idêntico)
# ═══════════════════════════════════════════════════════════════

def _convert_blob(val, blob_subtype: int, charset: str):
    if val is None:
        return None
    if hasattr(val, 'read'):
        val = val.read()
    elif isinstance(val, memoryview):
        val = bytes(val)
    if blob_subtype == 1:
        enc = _fb_charset_to_python(charset)
        if isinstance(val, bytes):
            try:
                text = val.decode(enc, errors='replace')
            except Exception:
                text = val.decode('latin-1', errors='replace')
        elif isinstance(val, str):
            text = val
        else:
            text = str(val)
        text = text.replace('\x00', '')
        return text.encode('utf-8', errors='replace')
    if isinstance(val, str):
        return val.encode('latin-1', errors='replace')
    return val


def _copy_escape(val) -> str:
    if val is None:
        return r'\N'
    if isinstance(val, (bytes, memoryview, bytearray)):
        b = bytes(val) if not isinstance(val, bytes) else val
        return r'\x' + b.hex()
    if isinstance(val, bool):
        return 't' if val else 'f'
    if hasattr(val, 'isoformat'):
        return str(val.isoformat())
    s = str(val)
    s = s.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
    s = s.replace('\x00', '')
    try:
        return s.encode('latin-1', errors='replace').decode('latin-1')
    except Exception:
        return s


def _copy_row_str(row: tuple, ncols: int) -> str:
    return '\t'.join(_copy_escape(row[i]) if i < len(row) else r'\N'
                     for i in range(ncols)) + '\n'


# ═══════════════════════════════════════════════════════════════
#  FUNÇÕES DE CONEXÃO  (idêntico)
# ═══════════════════════════════════════════════════════════════

def _fb_conn(config: dict):
    c = config['firebird']
    return fdb.connect(
        host=c['host'], port=c.get('port', 3050),
        database=c['database'], user=c['user'], password=c['password'],
        charset=_fb_charset_for_connect(c.get('charset', 'WIN1252')))

def _pg_conn(config: dict):
    c = config['postgresql']
    conn = psycopg2.connect(
        host=c['host'], port=c.get('port', 5432),
        database=c['database'], user=c['user'], password=c['password'])
    conn.set_client_encoding('UTF8')
    conn.autocommit = False
    return conn


# ═══════════════════════════════════════════════════════════════
#  DESCOBERTA DE COLUNAS  (idêntico — sem PK)
# ═══════════════════════════════════════════════════════════════

def discover_columns(config: dict) -> List[ColumnMeta]:
    conn = _fb_conn(config)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT rf.RDB$FIELD_NAME,
                   f.RDB$FIELD_TYPE,
                   COALESCE(f.RDB$FIELD_SUB_TYPE, 0),
                   COALESCE(f.RDB$FIELD_LENGTH, 0),
                   COALESCE(f.RDB$FIELD_PRECISION, 0),
                   COALESCE(f.RDB$FIELD_SCALE, 0),
                   rf.RDB$NULL_FLAG,
                   rf.RDB$FIELD_POSITION,
                   COALESCE(cs.RDB$CHARACTER_SET_NAME, 'NONE')
            FROM RDB$RELATION_FIELDS rf
            JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = rf.RDB$FIELD_SOURCE
            LEFT JOIN RDB$CHARACTER_SETS cs
                ON cs.RDB$CHARACTER_SET_ID = f.RDB$CHARACTER_SET_ID
            WHERE rf.RDB$RELATION_NAME = ?
            ORDER BY rf.RDB$FIELD_POSITION
        """, (SOURCE_TABLE,))
        cols = []
        for row in cur:
            name = row[0].strip()
            pg_type, is_blob = map_fb_to_pg(row[1], row[2], row[3], row[4], row[5])
            cols.append(ColumnMeta(
                name=name, fb_type_code=row[1], pg_type=pg_type,
                is_blob=is_blob, blob_subtype=row[2],
                fb_charset=row[8].strip(), nullable=row[6] is None,
                position=row[7]))
    finally:
        conn.close()
    return cols


def count_rows(config: dict, log: logging.Logger) -> int:
    log.info(f'Contando linhas de {SOURCE_TABLE}...')
    conn = _fb_conn(config)
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{SOURCE_TABLE}"')
        total = cur.fetchone()[0]
    finally:
        conn.close()
    log.info(f'Total: {total:,} linhas')
    return total


# ═══════════════════════════════════════════════════════════════
#  PARTICIONAMENTO POR RDB$DB_KEY  (novo — sem PK)
# ═══════════════════════════════════════════════════════════════

def compute_dbkey_ranges(config: dict, n_threads: int, total_rows: int,
                         log: logging.Logger) -> List[dict]:
    """
    Calcula N ranges de RDB$DB_KEY via FIRST 1 SKIP K.

    RDB$DB_KEY é o ponteiro físico da linha (8 bytes, único, comparável).
    Usamos FIRST/SKIP para obter pontos de corte equidistantes por contagem
    de linhas, independente de gaps na sequência física.

    Cada range:
      {low: bytes, high: bytes|None, is_last: bool, rows: int}

    Worker query (fresh):
      WHERE RDB$DB_KEY >= low [AND RDB$DB_KEY < high]
      ORDER BY RDB$DB_KEY

    Worker query (resume):
      WHERE RDB$DB_KEY > last_ck [AND RDB$DB_KEY < high]
      ORDER BY RDB$DB_KEY
    """
    conn = _fb_conn(config)
    try:
        cur = conn.cursor()

        # Ponto de início: RDB$DB_KEY mínima
        cur.execute(
            f'SELECT FIRST 1 RDB$DB_KEY FROM "{SOURCE_TABLE}" ORDER BY RDB$DB_KEY')
        row = cur.fetchone()
        if not row:
            log.warning('Tabela vazia — retornando 1 range vazio.')
            return [{'low': None, 'high': None, 'is_last': True, 'rows': 0}]
        min_key = bytes(row[0]) if isinstance(row[0], memoryview) else row[0]

        if n_threads == 1:
            return [{'low': min_key, 'high': None, 'is_last': True, 'rows': total_rows}]

        step = total_rows // n_threads
        split_keys: List[bytes] = []
        seen: set = set()

        for i in range(1, n_threads):
            skip = step * i
            cur.execute(
                f'SELECT FIRST 1 SKIP ? RDB$DB_KEY '
                f'FROM "{SOURCE_TABLE}" ORDER BY RDB$DB_KEY',
                (skip,))
            row = cur.fetchone()
            if row and row[0] is not None:
                key = bytes(row[0]) if isinstance(row[0], memoryview) else row[0]
                if key not in seen:
                    seen.add(key)
                    split_keys.append(key)

    finally:
        conn.close()

    if len(split_keys) < n_threads - 1:
        actual = len(split_keys) + 1
        log.warning(
            f'Apenas {actual} ranges distintos disponíveis '
            f'(solicitado: {n_threads}). Ajustando para {actual} threads.')

    lows  = [min_key] + split_keys
    highs = split_keys + [None]
    rows_each = max(1, total_rows // len(lows))

    return [
        {'low': lo, 'high': hi, 'is_last': hi is None, 'rows': rows_each}
        for lo, hi in zip(lows, highs)
    ]


def truncate_table(config: dict, log: logging.Logger):
    schema = config['postgresql'].get('schema', 'public')
    conn   = _pg_conn(config)
    cur    = conn.cursor()
    cur.execute(f'TRUNCATE TABLE "{schema}"."{DEST_TABLE}" CASCADE')
    conn.commit()
    cur.close()
    conn.close()
    log.info(f'Tabela {DEST_TABLE} truncada.')


# ═══════════════════════════════════════════════════════════════
#  AGGREGATOR THREAD  (idêntico ao doc_oper)
# ═══════════════════════════════════════════════════════════════

class AggregatorThread(threading.Thread):
    """Agrega progresso de todos os workers a cada 2s no migration_state_log_eventos.db."""

    def __init__(self, worker_db_paths: List[Path], master_state: StateManager,
                 shutdown_event: threading.Event, total_rows: int):
        super().__init__(name='aggregator', daemon=True)
        self.worker_db_paths = worker_db_paths
        self.master          = master_state
        self.shutdown        = shutdown_event
        self.total_rows      = total_rows

    def _read_worker(self, db_path: Path) -> Optional[MigrationProgress]:
        if not db_path.exists():
            return None
        try:
            conn = sqlite3.connect(str(db_path), timeout=3)
            conn.execute('PRAGMA journal_mode=WAL')
            row = conn.execute(
                'SELECT progress_json FROM migration_state WHERE id=1').fetchone()
            conn.close()
            return MigrationProgress.from_dict(json.loads(row[0])) if row else None
        except Exception:
            return None

    def aggregate(self):
        total_migrated = total_failed = 0
        total_speed = max_eta = max_elapsed = 0.0
        statuses = []

        for db_path in self.worker_db_paths:
            p = self._read_worker(db_path)
            if not p:
                continue
            total_migrated += p.rows_migrated
            total_failed   += p.rows_failed
            total_speed    += (p.speed_rows_per_sec or 0)
            max_eta         = max(max_eta, p.eta_seconds or 0)
            max_elapsed     = max(max_elapsed, p.elapsed_seconds or 0)
            statuses.append(p.status)

        agg_status = ('completed' if statuses and all(s in ('loaded', 'completed')
                                                      for s in statuses) else
                      'error'     if 'error'  in statuses else
                      'paused'    if 'paused' in statuses else
                      'running')

        agg = MigrationProgress(
            source_table=SOURCE_TABLE, dest_table=DEST_TABLE,
            total_rows=self.total_rows, rows_migrated=total_migrated,
            rows_failed=total_failed, status=agg_status, phase='migrating',
            speed_rows_per_sec=total_speed, eta_seconds=max_eta,
            elapsed_seconds=max_elapsed, updated_at=datetime.now().isoformat(),
            use_db_key=True, constraints_disabled=True)
        self.master.save_progress(agg)

    def run(self):
        while not self.shutdown.is_set():
            try:
                self.aggregate()
            except Exception:
                pass
            time.sleep(2)
        # Agregação final
        try:
            self.aggregate()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
#  WORKER THREAD  (adaptado para RDB$DB_KEY)
# ═══════════════════════════════════════════════════════════════

class WorkerThread(threading.Thread):
    """
    Migra o slice WHERE RDB$DB_KEY >= low [AND RDB$DB_KEY < high].

    Checkpoint salva o último RDB$DB_KEY processado para retomada exata.
    Cada worker tem conexões FB/PG próprias, state DB e log individuais.
    """

    def __init__(self, thread_id: int, config: dict, columns: List[ColumnMeta],
                 range_low: bytes, range_high: Optional[bytes], is_last: bool,
                 total_rows_in_range: int, shutdown_event: threading.Event,
                 use_insert: bool = False):
        super().__init__(name=f'worker-{thread_id}', daemon=False)
        self.tid                  = thread_id
        self.config               = config
        self.columns              = columns
        self.range_low            = range_low
        self.range_high           = range_high
        self.is_last              = is_last
        self._total_rows_in_range = total_rows_in_range
        self.shutdown             = shutdown_event
        self.use_insert           = use_insert

        self.state_db_path = BASE_DIR / f'migration_state_{DEST_TABLE}_t{thread_id}.db'
        self.log_file      = BASE_DIR / f'migration_{DEST_TABLE}_t{thread_id}.log'
        self.state         = StateManager(self.state_db_path)
        self.progress      = MigrationProgress()
        self.exception: Optional[Exception] = None

        self._setup_logger()

    def _setup_logger(self):
        self.log = logging.getLogger(f'worker-{self.tid}')
        self.log.setLevel(logging.INFO)
        self.log.propagate = False
        if not self.log.handlers:
            fmt = logging.Formatter(
                '%(asctime)s [%(levelname)-7s] %(name)s: %(message)s',
                datefmt='%H:%M:%S')
            fh = logging.FileHandler(str(self.log_file), encoding='utf-8')
            fh.setFormatter(fmt)
            self.log.addHandler(fh)
            ch = logging.StreamHandler(sys.stdout)
            ch.setFormatter(fmt)
            self.log.addHandler(ch)

    def _fb_conn(self): return _fb_conn(self.config)
    def _pg_conn(self): return _pg_conn(self.config)

    def _optimize_pg(self, conn):
        cur    = conn.cursor()
        perf   = self.config.get('performance', {})
        schema = self.config['postgresql'].get('schema', 'public')
        cur.execute(f"SET work_mem = '{perf.get('work_mem', '256MB')}'")
        cur.execute(f"SET maintenance_work_mem = '{perf.get('maintenance_work_mem', '512MB')}'")
        cur.execute('SET synchronous_commit = off')
        cur.execute('SET jit = off')
        cur.execute('SET constraint_exclusion = on')
        try:
            cur.execute(
                f'ALTER TABLE "{schema}"."{DEST_TABLE}" SET (autovacuum_enabled = false)')
            conn.commit()
        except Exception:
            conn.rollback()
        conn.commit()

    def _restore_pg(self, conn):
        cur    = conn.cursor()
        schema = self.config['postgresql'].get('schema', 'public')
        cur.execute('SET synchronous_commit = on')
        cur.execute('SET jit = on')
        try:
            cur.execute(
                f'ALTER TABLE "{schema}"."{DEST_TABLE}" SET (autovacuum_enabled = true)')
        except Exception:
            pass
        conn.commit()

    def _convert_row(self, row: tuple) -> tuple:
        out = []
        for i, col in enumerate(self.columns):
            val = row[i] if i < len(row) else None
            if val is None:
                out.append(None)
            elif col.is_blob:
                out.append(_convert_blob(val, col.blob_subtype, col.fb_charset))
            else:
                out.append(val)
        return tuple(out)

    def _build_select(self, saved: Optional[MigrationProgress]) -> Tuple[str, tuple]:
        """
        SELECT com range de RDB$DB_KEY para este worker.

        Fresh start:
            WHERE RDB$DB_KEY >= low [AND RDB$DB_KEY < high]
            ORDER BY RDB$DB_KEY

        Resume (checkpoint):
            WHERE RDB$DB_KEY > last_ck [AND RDB$DB_KEY < high]
            ORDER BY RDB$DB_KEY
        """
        upper_cond   = '' if self.is_last else ' AND RDB$DB_KEY < ?'
        upper_params = [] if self.is_last else [self.range_high]

        # Checkpoint válido?
        has_ck = (saved is not None
                  and saved.last_db_key is not None
                  and saved.rows_migrated > 0)

        if has_ck:
            return (
                f'SELECT * FROM "{SOURCE_TABLE}" '
                f'WHERE RDB$DB_KEY > ?{upper_cond} '
                f'ORDER BY RDB$DB_KEY',
                tuple([saved.last_db_key] + upper_params),
            )
        else:
            return (
                f'SELECT * FROM "{SOURCE_TABLE}" '
                f'WHERE RDB$DB_KEY >= ?{upper_cond} '
                f'ORDER BY RDB$DB_KEY',
                tuple([self.range_low] + upper_params),
            )

    def _insert_copy(self, pg_conn, rows: list, batch_num: int,
                     max_retries: int = 3):
        schema   = self.config['postgresql'].get('schema', 'public')
        col_names = ', '.join(f'"{c.name.lower()}"' for c in self.columns)
        copy_sql  = f'COPY "{schema}"."{DEST_TABLE}" ({col_names}) FROM STDIN'
        ncols     = len(self.columns)

        for attempt in range(max_retries):
            try:
                buf = io.StringIO()
                for row in rows:
                    buf.write(_copy_row_str(self._convert_row(row), ncols))
                buf.seek(0)
                cur = pg_conn.cursor()
                cur.copy_expert(copy_sql, buf)
                pg_conn.commit()
                return
            except Exception as exc:
                pg_conn.rollback()
                if attempt < max_retries - 1:
                    mid = len(rows) // 2
                    if mid > 0:
                        self._insert_copy(pg_conn, rows[:mid], batch_num)
                        self._insert_copy(pg_conn, rows[mid:], batch_num)
                        return
                    time.sleep(0.5)
                else:
                    raise

    def _insert_values(self, pg_conn, rows: list, batch_num: int,
                       max_retries: int = 3):
        from psycopg2.extras import execute_values
        schema    = self.config['postgresql'].get('schema', 'public')
        col_names = ', '.join(f'"{c.name.lower()}"' for c in self.columns)
        placeholders = ', '.join(['%s'] * len(self.columns))
        sql = f'INSERT INTO "{schema}"."{DEST_TABLE}" ({col_names}) VALUES %s'

        for attempt in range(max_retries):
            try:
                cur = pg_conn.cursor()
                execute_values(cur,
                    sql.replace(' VALUES %s', ' VALUES %s'),
                    [self._convert_row(r) for r in rows],
                    page_size=2000)
                pg_conn.commit()
                return
            except Exception:
                pg_conn.rollback()
                if attempt < max_retries - 1:
                    mid = len(rows) // 2
                    if mid > 0:
                        self._insert_values(pg_conn, rows[:mid], batch_num)
                        self._insert_values(pg_conn, rows[mid:], batch_num)
                        return
                    time.sleep(0.5)
                else:
                    raise

    def _update_progress(self, batch_rows: list, batch_num: int, t0: float,
                         last_db_key_in_batch: Optional[bytes]):
        n = len(batch_rows)
        self.progress.rows_migrated += n
        self.progress.current_batch  = batch_num
        self.progress.last_db_key    = last_db_key_in_batch   # checkpoint

        elapsed   = time.time() - t0
        migrated  = self.progress.rows_migrated
        total     = self.progress.total_rows
        speed     = migrated / elapsed if elapsed > 0 else 0
        remaining = total - migrated
        eta       = remaining / speed if speed > 0 else 0

        self.progress.speed_rows_per_sec = speed
        self.progress.eta_seconds        = eta
        self.progress.elapsed_seconds    = elapsed
        self.progress.updated_at         = datetime.now().isoformat()

        pct    = (migrated / total * 100) if total > 0 else 0
        filled = int(30 * pct / 100)
        bar    = '█' * filled + '░' * (30 - filled)
        self.log.info(
            f'  [{bar}] {pct:5.1f}% | {migrated:>12,}/{total:>12,} | '
            f'Lote {batch_num:>5,} | {speed:>10,.0f} lin/s | '
            f'ETA {_fmt_dur(eta):>10} | {_fmt_dur(elapsed)}')
        self.state.log_batch(batch_num, n, migrated, speed, eta)

    def run(self):
        cfg_m      = self.config['migration']
        batch_size = cfg_m.get('batch_size', 5000)

        self.log.info(f'[T{self.tid}] Iniciando | '
                      f'RDB$DB_KEY >= {self.range_low.hex() if self.range_low else "?"}'
                      + ('' if self.is_last
                         else f' | < {self.range_high.hex()}')
                      + f' | ~{self._total_rows_in_range:,} linhas')

        # Checkpoint?
        saved      = self.state.load_progress()
        is_restart = (saved is not None
                      and saved.status in ('running', 'paused', 'error')
                      and saved.rows_migrated > 0)

        if not is_restart:
            self.state.reset()

        total = self._total_rows_in_range
        total_batches = max(1, (total + batch_size - 1) // batch_size)

        if is_restart:
            self.progress = saved
            self.progress.status = 'running'
            self.progress.phase  = 'migrating'
            self.log.info(f'[T{self.tid}] RESTART — {saved.rows_migrated:,} linhas migradas')
        else:
            self.progress = MigrationProgress(
                source_table=SOURCE_TABLE, dest_table=f'{DEST_TABLE}_t{self.tid}',
                total_rows=total, batch_size=batch_size,
                total_batches=total_batches, use_db_key=True,
                status='running', phase='migrating',
                started_at=datetime.now().isoformat(),
                constraints_disabled=True)
        self.state.save_progress(self.progress)

        fb_conn = self._fb_conn()
        pg_conn = self._pg_conn()

        try:
            self._optimize_pg(pg_conn)

            fb_cur = fb_conn.cursor()
            fb_cur.arraysize = cfg_m.get('fetch_array_size', 5000)

            select_sql, select_params = self._build_select(
                saved if is_restart else None)
            self.log.info(f'[T{self.tid}] Query: {select_sql[:120]}')
            fb_cur.execute(select_sql, select_params)

            t0         = time.time()
            last_save  = t0
            batch_num  = self.progress.current_batch
            batch_buf  = []
            last_dbkey = None   # RDB$DB_KEY da última linha no buffer

            # Índice da coluna RDB$DB_KEY no result set (é retornado pelo SELECT *)
            # No Firebird, RDB$DB_KEY não está nas colunas normais — ele é
            # selecionado explicitamente quando citado na query ORDER BY.
            # Como fazemos ORDER BY RDB$DB_KEY, o cursor retorna as colunas
            # normais da tabela (sem DB_KEY). Precisamos de uma segunda query
            # para capturar o DB_KEY do checkpoint.
            # SOLUÇÃO: Usamos a posição da última linha + contagem para checkpoint.
            # Como o worker tem range fixo, o checkpoint é o próprio last_db_key
            # que obtemos via FIRST 1 SKIP para cada batch.
            # Simplificação prática: após cada batch bem-sucedido, capturamos
            # o DB_KEY da próxima linha a ser lida.

            # NOTA IMPORTANTE sobre RDB$DB_KEY em SELECT *:
            # O Firebird NÃO inclui RDB$DB_KEY em SELECT * automaticamente.
            # Para checkpoint preciso usamos FIRST 1 SKIP para obter o DB_KEY
            # da próxima linha após cada batch. Isso é mais limpo que tentar
            # extrair DB_KEY do result set.

            rows_processed = (self.progress.rows_migrated
                              if is_restart else 0)

            while not self.shutdown.is_set():
                row = fb_cur.fetchone()
                if row is None:
                    if batch_buf:
                        batch_num += 1
                        insert_fn = self._insert_values if self.use_insert else self._insert_copy
                        insert_fn(pg_conn, batch_buf, batch_num)
                        rows_processed += len(batch_buf)
                        last_dbkey = self._fetch_dbkey_at(rows_processed)
                        self._update_progress(batch_buf, batch_num, t0, last_dbkey)
                        self.state.save_progress(self.progress)
                        batch_buf = []
                    break

                batch_buf.append(row)

                if len(batch_buf) >= batch_size:
                    batch_num += 1
                    insert_fn = self._insert_values if self.use_insert else self._insert_copy
                    insert_fn(pg_conn, batch_buf, batch_num)
                    rows_processed += len(batch_buf)
                    # Captura DB_KEY do checkpoint para este ponto
                    last_dbkey = self._fetch_dbkey_at(rows_processed)
                    self._update_progress(batch_buf, batch_num, t0, last_dbkey)
                    batch_buf = []
                    gc.collect()

                    if time.time() - last_save > 10:
                        self.state.save_progress(self.progress)
                        last_save = time.time()

            if self.shutdown.is_set():
                self.progress.status = 'paused'
                self.progress.phase  = 'paused'
                self.progress.updated_at = datetime.now().isoformat()
                self.state.save_progress(self.progress)
                self.log.warning(f'[T{self.tid}] PAUSADO. Execute novamente para continuar.')
            else:
                elapsed = time.time() - t0
                self.progress.status       = 'loaded'
                self.progress.phase        = 'loaded'
                self.progress.completed_at = datetime.now().isoformat()
                self.progress.elapsed_seconds = elapsed
                if elapsed > 0:
                    self.progress.speed_rows_per_sec = (
                        self.progress.rows_migrated / elapsed)
                self.state.save_progress(self.progress)
                self.log.info(
                    f'[T{self.tid}] CONCLUÍDO — '
                    f'{self.progress.rows_migrated:,} linhas | '
                    f'{_fmt_dur(elapsed)} | '
                    f'{self.progress.speed_rows_per_sec:,.0f} lin/s')

        except Exception as exc:
            self.exception = exc
            self.progress.status        = 'error'
            self.progress.error_message = str(exc)[:500]
            self.progress.updated_at    = datetime.now().isoformat()
            try:
                self.state.save_progress(self.progress)
            except Exception:
                pass
            self.log.error(f'[T{self.tid}] ERRO: {exc}', exc_info=True)
        finally:
            fb_conn.close()
            self._restore_pg(pg_conn)
            pg_conn.close()

    def _fetch_dbkey_at(self, row_offset: int) -> Optional[bytes]:
        """
        Obtém o RDB$DB_KEY da linha na posição row_offset dentro do range
        deste worker. Usado como checkpoint para retomada exata.
        """
        upper_cond   = '' if self.is_last else ' AND RDB$DB_KEY < ?'
        upper_params = [] if self.is_last else [self.range_high]
        skip = row_offset - 1  # SKIP posiciona ANTES da linha desejada

        if skip < 0:
            return self.range_low

        sql = (f'SELECT FIRST 1 SKIP ? RDB$DB_KEY '
               f'FROM "{SOURCE_TABLE}" '
               f'WHERE RDB$DB_KEY >= ?{upper_cond} '
               f'ORDER BY RDB$DB_KEY')
        params = tuple([skip, self.range_low] + upper_params)

        try:
            conn = self._fb_conn()
            cur  = conn.cursor()
            cur.execute(sql, params)
            row = cur.fetchone()
            conn.close()
            if row and row[0] is not None:
                return bytes(row[0]) if isinstance(row[0], memoryview) else row[0]
        except Exception:
            pass
        return None


# ═══════════════════════════════════════════════════════════════
#  LOGGER PRINCIPAL
# ═══════════════════════════════════════════════════════════════

def _setup_main_logger(log_file: str) -> logging.Logger:
    log = logging.getLogger('orchestrator')
    log.setLevel(logging.INFO)
    fmt = logging.Formatter(
        '%(asctime)s [%(levelname)-7s] %(name)s: %(message)s',
        datefmt='%H:%M:%S')
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(fmt)
    log.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    log.addHandler(ch)
    return log


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description=f'Migra {SOURCE_TABLE} (sem PK) em threads paralelas via RDB$DB_KEY',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Particionamento:
  Usa RDB$DB_KEY (ponteiro físico Firebird) para dividir a tabela em N ranges
  independentes. Cada thread migra seu slice com checkpoint/resume individual.
  Não altera --threads entre execuções sem usar --reset.

Uso:
  python migrator_log_eventos.py --threads 8
  python migrator_log_eventos.py --threads 8 --reset
  python migrator_log_eventos.py --threads 8 --dry-run
  python migrator_log_eventos.py --threads 4 --batch-size 5000
  python migrator_log_eventos.py --generate-scripts-only

Monitor:
  python monitor.py               →  vê todas as threads automaticamente
  python monitor.py --big-tables  →  filtra só as tabelas grandes
        """)
    ap.add_argument('-c', '--config', default='config.yaml')
    ap.add_argument('-t', '--threads', type=int, default=8, metavar='N',
                    help='Número de threads paralelas (padrão: 8)')
    ap.add_argument('--reset', action='store_true',
                    help='Descarta todos os checkpoints e reinicia do zero')
    ap.add_argument('--dry-run', action='store_true',
                    help='Mostra contagens e ranges sem escrever dados')
    ap.add_argument('--batch-size', type=int, default=None)
    ap.add_argument('--use-insert', action='store_true',
                    help='Usa INSERT em vez de COPY')
    ap.add_argument('--generate-scripts-only', action='store_true',
                    help='Apenas gera scripts SQL de constraints')
    args = ap.parse_args()

    if not Path(args.config).exists():
        print(f'ERRO: {args.config} não encontrado.')
        sys.exit(1)

    log = _setup_main_logger(
        str(BASE_DIR / f'migration_{DEST_TABLE}_parallel.log'))

    with open(args.config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    if args.batch_size:
        config['migration']['batch_size'] = args.batch_size

    n_threads = max(1, args.threads)
    cfg_pg    = config['postgresql']
    schema    = cfg_pg.get('schema', 'public')
    pg_params = {
        'host': cfg_pg['host'], 'port': cfg_pg.get('port', 5432),
        'database': cfg_pg['database'],
        'user': cfg_pg['user'], 'password': cfg_pg['password'],
    }

    log.info('=' * 70)
    log.info(f'  MIGRAÇÃO PARALELA: {SOURCE_TABLE} → {DEST_TABLE}')
    log.info(f'  Threads       : {n_threads}')
    log.info(f'  Partição      : RDB$DB_KEY (sem PK)')
    log.info(f'  Batch         : {config["migration"].get("batch_size", 5000):,}')
    log.info(f'  Modo          : {"COPY" if not args.use_insert else "INSERT"}')
    log.info('=' * 70)

    # ══════════════════════════════════════════════════════════
    #  FASE 0 — Constraints
    # ══════════════════════════════════════════════════════════
    log.info('')
    log.info('━' * 70)
    log.info('  Fase 0 — Coleta de DDLs e scripts de constraints')
    log.info('━' * 70)

    cman  = ConstraintManager(pg_params, schema, DEST_TABLE)
    n_obj = cman.collect_all()

    state_path   = BASE_DIR / f'constraint_state_{DEST_TABLE}.json'
    disable_path = BASE_DIR / f'disable_constraints_{DEST_TABLE}.sql'
    enable_path  = BASE_DIR / f'enable_constraints_{DEST_TABLE}.sql'

    cman.save_state(str(state_path))
    disable_path.write_text(cman.generate_disable_script(), encoding='utf-8')
    enable_path.write_text(cman.generate_enable_script(), encoding='utf-8')
    log.info(f'  {n_obj} objetos — scripts: {disable_path.name}, {enable_path.name}')

    if args.generate_scripts_only:
        log.info('Modo --generate-scripts-only. Encerrando.')
        return

    if not args.dry_run:
        log.info('  Desabilitando constraints...')
        cman.load_state(str(state_path))
        cman.disable_all()
        log.info('  Constraints desabilitadas.')

    # ══════════════════════════════════════════════════════════
    #  FASE 1 — Preparação e particionamento
    # ══════════════════════════════════════════════════════════
    log.info('')
    log.info('━' * 70)
    log.info('  Fase 1 — Preparação e particionamento por RDB$DB_KEY')
    log.info('━' * 70)

    columns   = discover_columns(config)
    blob_cols = [c.name for c in columns if c.is_blob]
    log.info(f'  Colunas: {len(columns)} | BLOBs: {blob_cols or "nenhum"}')

    total_rows = count_rows(config, log)

    worker_db_paths = [
        BASE_DIR / f'migration_state_{DEST_TABLE}_t{i}.db'
        for i in range(n_threads)
    ]

    if args.dry_run:
        log.info('')
        log.info(f'  DRY-RUN: {total_rows:,} linhas, {n_threads} thread(s)')
        log.info('  Calculando ranges de RDB$DB_KEY...')
        ranges = compute_dbkey_ranges(config, n_threads, total_rows, log)
        for i, r in enumerate(ranges):
            low_hex  = r['low'].hex()[:16] + '...' if r['low'] else '?'
            high_hex = (r['high'].hex()[:16] + '...' if r['high'] else 'FIM')
            log.info(f'  Thread {i}: DB_KEY [{low_hex} → {high_hex}]  '
                     f'(~{r["rows"]:,} linhas)')
        return

    log.info(f'  Calculando {n_threads} ranges de RDB$DB_KEY...')
    ranges    = compute_dbkey_ranges(config, n_threads, total_rows, log)
    n_threads = len(ranges)

    for i, r in enumerate(ranges):
        low_hex  = r['low'].hex()[:16] + '...' if r['low'] else '?'
        high_hex = (r['high'].hex()[:16] + '...' if r['high'] else 'FIM')
        log.info(f'  Thread {i}: DB_KEY [{low_hex} → {high_hex}]  '
                 f'(~{r["rows"]:,} linhas)')

    # Detectar resume
    any_restart = False
    if not args.reset:
        for db_path in worker_db_paths[:len(ranges)]:
            if not db_path.exists():
                continue
            try:
                conn = sqlite3.connect(str(db_path), timeout=3)
                row  = conn.execute(
                    'SELECT progress_json FROM migration_state WHERE id=1').fetchone()
                conn.close()
                if row:
                    prog = MigrationProgress.from_dict(json.loads(row[0]))
                    if prog.status in ('running', 'paused', 'error') and prog.rows_migrated > 0:
                        any_restart = True
                        break
            except Exception:
                pass

    if args.reset:
        log.info('  --reset: limpando checkpoints...')
        for db_path in worker_db_paths:
            if db_path.exists():
                StateManager(db_path).reset()
        master_path = BASE_DIR / f'migration_state_{DEST_TABLE}.db'
        if master_path.exists():
            StateManager(master_path).reset()

    if any_restart:
        log.info('  Retomando de checkpoint — TRUNCATE ignorado.')
    else:
        log.info('')
        log.info(f'  Truncando {DEST_TABLE}...')
        truncate_table(config, log)

    # ══════════════════════════════════════════════════════════
    #  FASE 2 — Carga paralela
    # ══════════════════════════════════════════════════════════
    log.info('')
    log.info('━' * 70)
    log.info(f'  Fase 2 — Carga paralela ({n_threads} threads)')
    log.info('━' * 70)

    master_state_path = BASE_DIR / f'migration_state_{DEST_TABLE}.db'
    master_state      = StateManager(master_state_path)
    if not any_restart:
        master_state.reset()
    master_state.save_progress(MigrationProgress(
        source_table=SOURCE_TABLE, dest_table=DEST_TABLE,
        total_rows=total_rows, status='running', phase='migrating',
        started_at=datetime.now().isoformat(),
        constraints_disabled=True, use_db_key=True))

    shutdown = threading.Event()

    def _on_signal(signum, _frame):
        log.warning(f'Sinal {signum} recebido — aguardando fim dos batches...')
        shutdown.set()

    signal.signal(signal.SIGINT,  _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    workers = [
        WorkerThread(
            thread_id=i, config=config, columns=columns,
            range_low=ranges[i]['low'],
            range_high=ranges[i]['high'],
            is_last=ranges[i]['is_last'],
            total_rows_in_range=ranges[i]['rows'],
            shutdown_event=shutdown,
            use_insert=args.use_insert,
        )
        for i in range(n_threads)
    ]

    aggregator = AggregatorThread(
        worker_db_paths=worker_db_paths[:n_threads],
        master_state=master_state,
        shutdown_event=shutdown,
        total_rows=total_rows,
    )
    aggregator.start()

    t_start = time.time()
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    shutdown.set()
    aggregator.join(timeout=8)

    elapsed        = time.time() - t_start
    total_migrated = sum(w.progress.rows_migrated for w in workers)
    total_failed   = sum(w.progress.rows_failed   for w in workers)
    error_workers  = [w for w in workers if w.exception]
    paused_workers = [w for w in workers if w.progress.status == 'paused']

    final_status = ('error'     if error_workers  else
                    'paused'    if paused_workers  else
                    'loaded')

    master_state.save_progress(MigrationProgress(
        source_table=SOURCE_TABLE, dest_table=DEST_TABLE,
        total_rows=total_rows, rows_migrated=total_migrated,
        rows_failed=total_failed, status=final_status, phase=final_status,
        elapsed_seconds=elapsed, completed_at=datetime.now().isoformat(),
        use_db_key=True, constraints_disabled=True,
        speed_rows_per_sec=(total_migrated / elapsed if elapsed > 0 else 0),
        updated_at=datetime.now().isoformat()))

    log.info('')
    log.info('=' * 70)
    log.info(f'  RESULTADO: {final_status.upper()}')
    log.info(f'  Total migrado: {total_migrated:,} / {total_rows:,}')
    log.info(f'  Falhas:        {total_failed:,}')
    log.info(f'  Tempo total:   {_fmt_dur(elapsed)}')
    log.info(f'  Velocidade:    {total_migrated/elapsed:,.0f} lin/s' if elapsed > 0 else '')

    if error_workers:
        log.error(f'  Threads com erro: {[w.tid for w in error_workers]}')
        for w in error_workers:
            log.error(f'    T{w.tid}: {w.exception}')

    if final_status == 'loaded':
        log.info('')
        log.info(f'  Recriar constraints:')
        log.info(f'    psql -f {enable_path.name}')
    log.info('=' * 70)


if __name__ == '__main__':
    main()
