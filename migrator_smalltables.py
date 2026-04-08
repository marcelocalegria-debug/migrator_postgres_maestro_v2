#!/usr/bin/env python3
"""
migrator.py
===========
Migra uma tabela inteira do Firebird 3 → PostgreSQL 18.

Correções nesta versão:
  - SELECT COUNT(*) com sintaxe corrigida
  - BLOB TEXT convertido de WIN1252 → UTF-8 antes de inserir no PG
  - RDB$DB_KEY como paginação fallback (sem PK) — reiniciável e rápido
  - Inserção via COPY protocol (3-5× mais rápido que execute_values)
  - Sub-batching automático em caso de erro
  - WAL optimization antes da carga
  - Limpeza de memória explícita entre batches
  - fetch_array_size aumentado para 10000
  - Tratamento de memóriaview de BLOBs

Uso:
    python migrator.py                          # inicia ou recomeça
    python migrator.py --reset                  # recomeça do zero
    python migrator.py --dry-run                # simulação
    python migrator.py --generate-scripts-only  # só gera SQL
    python migrator.py --batch-size 10000       # sobrescreve batch
    python migrator.py --use-insert             # usa INSERT em vez de COPY
"""

import sys
import os
import io
import json
import gc
import time
import signal
import sqlite3
import logging
import argparse
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Any, Tuple, Generator
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

import yaml
import psycopg2
from psycopg2 import sql as pgsql

if os.name == 'nt' and hasattr(os, 'add_dll_directory'):
    try:
        # Add the directory of the script to the DLL search path
        # This helps find fbclient.dll if it's in the same directory as the script
        os.add_dll_directory(os.path.abspath(os.path.dirname(__file__) or '.'))
    except Exception:
        pass

import fdb

if os.name == 'nt':
    fb_paths = [
        # Prioritize fbclient.dll in the script's directory
        os.path.abspath(os.path.join(os.path.dirname(__file__) or '.', "fbclient.dll")),
        r"C:\Program Files\Firebird\Firebird_3_0\fbclient.dll",
        r"C:\Program Files\Firebird\Firebird_4_0\fbclient.dll",
        r"C:\Program Files\Firebird\Firebird_5_0\fbclient.dll",
        r"C:\Program Files\Firebird\Firebird_2_5\bin\fbclient.dll",
        r"C:\Program Files (x86)\Firebird\Firebird_3_0\fbclient.dll",
        r"C:\Program Files (x86)\Firebird\Firebird_2_5\bin\fbclient.dll",
    ]
    for p in fb_paths:
        if os.path.exists(p):
            try:
                fdb.load_api(p)
                break
            except Exception:
                pass

from pg_constraints import ConstraintManager

BASE_DIR = Path(__file__).parent


# ═══════════════════════════════════════════════════════════════
#  ESTRUTURAS DE DADOS
# ═══════════════════════════════════════════════════════════════

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
    last_pk_value: Any = None          # restart checkpoint
    pk_columns: List[str] = field(default_factory=list)
    use_db_key: bool = False           # fallback sem PK
    last_db_key: bytes = None          # checkpoint RDB$DB_KEY
    status: str = "idle"
    started_at: Optional[str] = None
    updated_at: Optional[str] = None
    completed_at: Optional[str] = None
    elapsed_seconds: float = 0.0
    speed_rows_per_sec: float = 0.0
    eta_seconds: Optional[float] = None
    error_message: Optional[str] = None
    constraints_disabled: bool = False
    phase: str = "idle"

    def to_dict(self):
        d = asdict(self)
        # bytes não são JSON-serializáveis
        if isinstance(d.get('last_db_key'), (bytes, memoryview)):
            d['last_db_key'] = d['last_db_key'].hex() if d['last_db_key'] else None
        if isinstance(d.get('last_pk_value'), (bytes, memoryview)):
            d['last_pk_value'] = d['last_pk_value'].hex()
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
    blob_subtype: int = 0      # 0=binary, 1=text
    fb_charset: str = 'NONE'
    nullable: bool = True
    position: int = 0


# ═══════════════════════════════════════════════════════════════
#  STATE MANAGER (SQLite)
# ═══════════════════════════════════════════════════════════════

class StateManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _conn(self):
        return sqlite3.connect(self.db_path, timeout=10)

    def _init_db(self):
        conn = self._conn()
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
        conn = self._conn()
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
        conn = self._conn()
        row = conn.execute(
            "SELECT progress_json FROM migration_state WHERE id=1"
        ).fetchone()
        conn.close()
        if row:
            return MigrationProgress.from_dict(json.loads(row[0]))
        return None

    def log_batch(self, batch, rows, total, speed, eta, msg=""):
        conn = self._conn()
        conn.execute("""
            INSERT INTO migration_log
                (timestamp, batch_number, rows_in_batch, total_rows,
                 speed_rps, eta_seconds, message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now().isoformat(), batch, rows, total, speed, eta, msg))
        conn.commit()
        conn.close()

    def get_recent(self, n=30) -> list:
        conn = self._conn()
        rows = conn.execute("""
            SELECT timestamp, batch_number, rows_in_batch, total_rows,
                   speed_rps, eta_seconds, message
            FROM migration_log ORDER BY id DESC LIMIT ?
        """, (n,)).fetchall()
        conn.close()
        return rows

    def reset(self):
        conn = self._conn()
        conn.executescript("""
            DELETE FROM migration_state;
            DELETE FROM migration_log;
        """)
        conn.commit()
        conn.close()


# ═══════════════════════════════════════════════════════════════
#  MASTER STATE MANAGER (estado global das 901 tabelas pequenas)
# ═══════════════════════════════════════════════════════════════

class MasterStateManager:
    """
    Rastreia o status global da migração paralela das tabelas pequenas.
    Armazenado em migration_state_smalltables_master.db (SQLite).

    Status possíveis por tabela: pending / running / completed / failed
    Em re-execução, tabelas 'completed' são puladas automaticamente.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _conn(self):
        return sqlite3.connect(self.db_path, timeout=15)

    def _init_db(self):
        conn = self._conn()
        conn.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS table_status (
                source_table  TEXT PRIMARY KEY,
                dest_table    TEXT NOT NULL,
                status        TEXT DEFAULT 'pending',
                rows_migrated INTEGER DEFAULT 0,
                elapsed_sec   REAL,
                error_msg     TEXT,
                started_at    TEXT,
                completed_at  TEXT
            );
        """)
        conn.commit()
        conn.close()

    def register_tables(self, tables: list, reset: bool = False):
        """Registra tabelas no estado master. Com reset=True, limpa tudo antes."""
        conn = self._conn()
        if reset:
            conn.execute("DELETE FROM table_status")
        for t in tables:
            conn.execute("""
                INSERT INTO table_status (source_table, dest_table, status)
                VALUES (?, ?, 'pending')
                ON CONFLICT(source_table) DO NOTHING
            """, (t['source'], t['dest']))
        conn.commit()
        conn.close()

    def get_pending(self) -> list:
        """Tabelas pending ou failed (para retry em re-execução)."""
        conn = self._conn()
        rows = conn.execute("""
            SELECT source_table, dest_table
            FROM table_status
            WHERE status IN ('pending', 'failed')
            ORDER BY source_table
        """).fetchall()
        conn.close()
        return [{'source': r[0], 'dest': r[1]} for r in rows]

    def get_completed_for_reenable(self) -> list:
        """Tabelas completed — usadas para re-enable de constraints."""
        conn = self._conn()
        rows = conn.execute("""
            SELECT source_table, dest_table
            FROM table_status
            WHERE status = 'completed'
            ORDER BY source_table
        """).fetchall()
        conn.close()
        return [{'source': r[0], 'dest': r[1]} for r in rows]

    def mark_running(self, source_table: str):
        conn = self._conn()
        conn.execute("""
            UPDATE table_status SET status='running', started_at=?
            WHERE source_table=?
        """, (datetime.now().isoformat(), source_table))
        conn.commit()
        conn.close()

    def mark_completed(self, source_table: str, rows: int, elapsed: float):
        conn = self._conn()
        conn.execute("""
            UPDATE table_status
            SET status='completed', rows_migrated=?, elapsed_sec=?, completed_at=?
            WHERE source_table=?
        """, (rows, elapsed, datetime.now().isoformat(), source_table))
        conn.commit()
        conn.close()

    def mark_failed(self, source_table: str, error: str):
        conn = self._conn()
        conn.execute("""
            UPDATE table_status
            SET status='failed', error_msg=?
            WHERE source_table=?
        """, (error[:500], source_table))
        conn.commit()
        conn.close()

    def get_summary(self) -> dict:
        conn = self._conn()
        rows = conn.execute(
            "SELECT status, COUNT(*) FROM table_status GROUP BY status"
        ).fetchall()
        conn.close()
        s = {'pending': 0, 'running': 0, 'completed': 0, 'failed': 0, 'total': 0}
        for status, count in rows:
            if status in s:
                s[status] = count
            s['total'] += count
        return s

    def print_summary(self, log=None):
        s = self.get_summary()
        msg = (f"Resumo final: {s['total']} tabelas | "
               f"Concluídas: {s['completed']} | "
               f"Falhas: {s['failed']} | "
               f"Pendentes: {s['pending']}")
        if log:
            log.info(msg)
        else:
            print(msg)


# ═══════════════════════════════════════════════════════════════
#  MAPEAMENTO DE TIPOS FIREBIRD → POSTGRESQL
# ═══════════════════════════════════════════════════════════════

# RDB$FIELD_TYPE codes
_FB = {
    7:   'SMALLINT',
    8:   'INTEGER',
    10:  'REAL',
    12:  'DATE',
    13:  'TIME',
    14:  'CHAR',
    16:  'BIGINT',
    27:  'DOUBLE PRECISION',
    35:  'TIMESTAMP',
    37:  'VARCHAR',
    261: 'BLOB',
}


def map_fb_to_pg(type_code: int, subtype: int, length: int,
                 precision: int, scale: int) -> Tuple[str, bool]:
    """Retorna (pg_type, is_blob)."""
    if type_code == 261:
        return ('TEXT', True) if subtype == 1 else ('BYTEA', True)

    if type_code in (8, 16) and precision > 0:
        return f'NUMERIC({precision},{abs(scale)})', False

    base = _FB.get(type_code, 'TEXT')
    if type_code in (14, 37):
        return f'{base}({length})', False
    return base, False


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _fmt_dur(seconds: float) -> str:
    if seconds is None or seconds < 0:
        return 'N/A'
    seconds = int(seconds)
    h, r = divmod(seconds, 3600)
    m, s = divmod(r, 60)
    if h:
        return f'{h}h{m:02d}m{s:02d}s'
    if m:
        return f'{m}m{s:02d}s'
    return f'{s}s'


# Mapa Firebird charset → codec Python para decodificação de BLOB TEXT
_FB_CHARSET_TO_PYTHON: dict = {
    'WIN1252':    'cp1252',
    'WIN1250':    'cp1250',
    'WIN1251':    'cp1251',
    'WIN1253':    'cp1253',
    'WIN1254':    'cp1254',
    'ISO8859_1':  'iso-8859-1',
    'ISO8859_2':  'iso-8859-2',
    'ISO8859_15': 'iso-8859-15',
    'UTF8':       'utf-8',
    'UNICODE_FSS':'utf-8',
    'ASCII':      'ascii',
    'NONE':       'latin-1',   # NONE = raw bytes, latin-1 é seguro
}

# Mapa de nomes alternativos (config.yaml) → nome Firebird para fdb.connect()
_CONFIG_CHARSET_TO_FB: dict = {
    'iso-8859-1':   'ISO8859_1',
    'iso8859-1':    'ISO8859_1',
    'iso_8859-1':   'ISO8859_1',
    'latin1':       'ISO8859_1',
    'latin-1':      'ISO8859_1',
    'win1252':      'WIN1252',
    'windows-1252': 'WIN1252',
    'cp1252':       'WIN1252',
    'utf-8':        'UTF8',
    'utf8':         'UTF8',
}


def _fb_charset_for_connect(raw: str) -> str:
    """Normaliza o charset do config.yaml para o nome que o Firebird aceita em fdb.connect()."""
    return _CONFIG_CHARSET_TO_FB.get(raw.lower(), raw.upper())


def _fb_charset_to_python(fb_charset: str) -> str:
    """Converte charset Firebird (de RDB$FIELDS) para codec Python."""
    return _FB_CHARSET_TO_PYTHON.get(fb_charset.upper(), 'latin-1')


def _convert_blob(val, blob_subtype: int, charset: str) -> Optional[bytes]:
    """
    [BUG FIX] Converte BLOB do Firebird para bytes adequados ao PostgreSQL.
    - BLOB TEXT (subtype 1): lê bytes e decodifica charset Firebird → UTF-8.
    - BLOB BINARY (subtype 0): lê bytes diretamente.
    - memoryview: converte para bytes.
    """
    if val is None:
        return None

    # Ler conteúdo se for file-like (fdb BLOB locator)
    if hasattr(val, 'read'):
        val = val.read()
    elif isinstance(val, memoryview):
        val = bytes(val)

    if blob_subtype == 1:
        # BLOB TEXT: Firebird armazena no charset da coluna
        # Precisamos de uma string limpa para o PostgreSQL
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
        # NULL bytes (0x00) são rejeitados pelo PostgreSQL em campos TEXT
        return text.replace('\x00', '') if '\x00' in text else text

    # BLOB BINARY
    if isinstance(val, bytes):
        return val
    if isinstance(val, str):
        return val.encode('latin-1')  # preservação byte-a-byte
    return bytes(val) if val is not None else None


def _copy_escape(val) -> str:
    """Escapa valor para protocolo COPY text do PostgreSQL."""
    if val is None:
        return '\\N'
    if isinstance(val, bytes):
        return '\\\\x' + val.hex()
    if isinstance(val, bool):
        return 't' if val else 'f'
    if isinstance(val, (int, float)):
        return str(val)
    if hasattr(val, 'isoformat'):  # date, time, datetime
        return val.isoformat()
    s = str(val)
    # Sanitize for PostgreSQL LATIN1 databases strictly rejecting WIN1252 chars
    s = s.encode('latin-1', errors='replace').decode('latin-1')
    # NULL bytes (0x00) são inválidos no protocolo COPY text do PostgreSQL
    # mas podem existir em BLOB TEXT do Firebird — remove silenciosamente
    if '\x00' in s:
        s = s.replace('\x00', '')
    return (s.replace('\\', '\\\\')
             .replace('\t', '\\t')
             .replace('\n', '\\n')
             .replace('\r', '\\r'))


def _copy_row_str(row: tuple, col_count: int) -> str:
    """Converte uma tupla em linha COPY text: col1\tcol2\t...colN\n"""
    parts = [_copy_escape(row[i]) if i < len(row) else '\\N'
             for i in range(col_count)]
    return '\t'.join(parts) + '\n'


# ═══════════════════════════════════════════════════════════════
#  WORKER FUNCTION (top-level — picklável para ProcessPoolExecutor)
# ═══════════════════════════════════════════════════════════════

def _worker_migrate_table(args: tuple) -> dict:
    """
    Função de worker para ProcessPoolExecutor.
    Deve ser top-level (não método de classe) para ser picklável.
    Cria uma instância isolada do Migrator e migra uma única tabela.
    Cada worker tem suas próprias conexões FB e PG — sem compartilhamento.
    """
    config_path, source_table, dest_table, state_db = args
    t0 = time.time()
    try:
        log_file = f'migration_{dest_table}.log'
        migrator = FirebirdToPgMigrator(
            config_path,
            override_log_file=log_file,
        )
        tbl_cfg = {
            'source':   source_table,
            'dest':     dest_table,
            'state_db': state_db,
        }
        migrator._load_table(tbl_cfg, dry_run=False)
        rows = migrator.progress.rows_migrated if migrator.progress else 0
        return {
            'table':         source_table,
            'status':        'completed',
            'rows_migrated': rows,
            'elapsed_sec':   time.time() - t0,
        }
    except Exception as e:
        return {
            'table':       source_table,
            'status':      'failed',
            'error':       str(e),
            'elapsed_sec': time.time() - t0,
        }


# ═══════════════════════════════════════════════════════════════
#  MIGRADOR
# ═══════════════════════════════════════════════════════════════

class FirebirdToPgMigrator:

    def __init__(self, config_path: str, override_batch_size: int = None,
                 use_insert: bool = False, override_table: str = None,
                 override_log_file: str = None):
        self._config_path = str(Path(config_path).resolve())
        self.config = self._load_config(config_path)
        if override_batch_size:
            self.config['migration']['batch_size'] = override_batch_size
        self.use_insert = use_insert  # True = execute_values, False = COPY

        # --table: sobrescreve lista de tabelas do config
        if override_table:
            table = override_table.strip()
            self.config['migration']['_override_table'] = {
                'source':   table.upper(),
                'dest':     table.lower(),
                'state_db': str(BASE_DIR / f'migration_state_{table.lower()}.db'),
            }

        # --log-file: sobrescreve path do log (para execução paralela)
        if override_log_file:
            self.config.setdefault('logging', {})['file'] = override_log_file

        self.progress = MigrationProgress()
        self.columns: List[ColumnMeta] = []
        self._shutdown = False
        self._state: Optional[StateManager] = None   # inicializado por tabela em run()
        self._cman: Optional[ConstraintManager] = None

        self._setup_logging()
        self.log = logging.getLogger('migrator')

        signal.signal(signal.SIGINT,  self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

    # ─── config ─────────────────────────────────────────────

    @staticmethod
    def _load_config(path: str) -> dict:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _setup_logging(self):
        cfg = self.config.get('logging', {})
        level = getattr(logging, cfg.get('level', 'INFO'), logging.INFO)
        fmt = logging.Formatter(
            '%(asctime)s [%(levelname)-7s] %(name)s: %(message)s',
            datefmt='%H:%M:%S')
        root = logging.getLogger()
        root.setLevel(level)

        fh = logging.FileHandler(cfg.get('file', 'migration.log'),
                                 encoding='utf-8')
        fh.setFormatter(fmt)
        root.addHandler(fh)

        if cfg.get('console', True):
            ch = logging.StreamHandler(sys.stdout)
            ch.setFormatter(fmt)
            root.addHandler(ch)

    def _on_signal(self, signum, frame):
        self.log.warning(f'⚠ Sinal {signum} — finalizando batch atual...')
        self._shutdown = True

    # ─── resolução de tabelas ────────────────────────────────

    def _resolve_tables(self) -> List[dict]:
        """
        Retorna lista de dicts {source, dest, state_db} a partir do config.
        Prioridade: --table CLI > tables: [...] > source_table/dest_table legado.
        """
        cfg_m = self.config['migration']

        # --table CLI sobrescreve tudo
        if '_override_table' in cfg_m:
            return [cfg_m['_override_table']]

        if 'tables' in cfg_m:
            result = []
            for t in cfg_m['tables']:
                dest = t['dest']
                state_db = t.get('state_db') or str(BASE_DIR / f'migration_state_{dest}.db')
                result.append({
                    'source':   t['source'],
                    'dest':     dest,
                    'state_db': state_db,
                })
            return result

        # Backward compat: formato antigo com source_table / dest_table
        return [{
            'source':   cfg_m['source_table'],
            'dest':     cfg_m['dest_table'],
            'state_db': cfg_m.get('state_db') or str(BASE_DIR / 'migration_state.db'),
        }]

    def _build_table_map(self, tables: List[dict]) -> dict:
        """Mapeia nome Firebird (MAIÚSCULAS) → nome PG destino."""
        return {t['source'].upper(): t['dest'] for t in tables}

    # ─── conexões ───────────────────────────────────────────

    def _fb_conn(self):
        c = self.config['firebird']
        return fdb.connect(
            host=c['host'], port=c.get('port', 3050),
            database=c['database'],
            user=c['user'], password=c['password'],
            charset=_fb_charset_for_connect(c.get('charset', 'WIN1252')))

    def _pg_conn(self):
        c = self.config['postgresql']
        conn = psycopg2.connect(
            host=c['host'], port=c.get('port', 5432),
            database=c['database'],
            user=c['user'], password=c['password'])
        conn.set_client_encoding('UTF8')
        conn.autocommit = False
        return conn

    # ─── metadados ──────────────────────────────────────────

    def _discover_columns(self) -> List[ColumnMeta]:
        """Mapeia colunas do Firebird para PostgreSQL."""
        conn = self._fb_conn()
        try:
            cur = conn.cursor()
            tbl = self.config['migration']['source_table'].upper()

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
            """, (tbl,))

            cols = []
            for row in cur:
                name = row[0].strip()
                tc = row[1]
                st = row[2]
                ln = row[3]
                pr = row[4]
                sc = row[5]
                nullable = row[6] is None
                pos = row[7]
                charset = row[8].strip()

                pg_type, is_blob = map_fb_to_pg(tc, st, ln, pr, sc)

                # [FIX] Force binary mapping for known binary fields that might be mislabeled as TEXT in Firebird
                if is_blob and name.upper() in ('DADO', 'TE_IMAGEM_REDUZIDA', 'IMAGEM'):
                    is_blob = True
                    st = 0  # Force Binary Subtype
                    pg_type = 'BYTEA'

                cols.append(ColumnMeta(
                    name=name, fb_type_code=tc, pg_type=pg_type,
                    is_blob=is_blob, blob_subtype=st,
                    fb_charset=charset, nullable=nullable, position=pos))
        finally:
            conn.close()
        return cols

    def _discover_pk(self) -> List[str]:
        """Colunas da PK no Firebird."""
        conn = self._fb_conn()
        try:
            cur = conn.cursor()
            tbl = self.config['migration']['source_table'].upper()
            cur.execute("""
                SELECT sg.RDB$FIELD_NAME
                FROM RDB$RELATION_CONSTRAINTS rc
                JOIN RDB$INDEX_SEGMENTS sg
                    ON sg.RDB$INDEX_NAME = rc.RDB$INDEX_NAME
                WHERE rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
                  AND rc.RDB$RELATION_NAME = ?
                ORDER BY sg.RDB$FIELD_POSITION
            """, (tbl,))
            pks = [r[0].strip() for r in cur]
        finally:
            conn.close()
        return pks

    def _count_rows(self) -> int:
        """[BUG FIX] SELECT COUNT(*) com sintaxe corrigida."""
        self.log.info('Contando linhas (pode demorar)...')
        conn = self._fb_conn()
        try:
            cur = conn.cursor()
            tbl = self.config['migration']['source_table'].upper()
            cur.execute(f'SELECT COUNT(*) FROM "{tbl}"')
            total = cur.fetchone()[0]
        finally:
            conn.close()
        self.log.info(f'Total: {total:,} linhas')
        return total

    def _col_index(self, name: str) -> int:
        for i, c in enumerate(self.columns):
            if c.name == name:
                return i
        return 0

    # ─── destino ────────────────────────────────────────────

    def _check_dest_table(self):
        cfg = self.config['postgresql']
        schema = cfg.get('schema', 'public')
        table = self.config['migration']['dest_table']
        conn = self._pg_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema=%s AND table_name=%s)
        """, (schema, table))
        if not cur.fetchone()[0]:
            cur.close()
            conn.close()
            raise RuntimeError(
                f'Tabela "{schema}.{table}" não existe no PostgreSQL. '
                f'Crie-a antes de executar a migração.')
        cur.close()
        conn.close()
        self.log.info(f'Destino "{schema}.{table}" OK.')

    def _truncate_dest(self):
        cfg = self.config['postgresql']
        schema = cfg.get('schema', 'public')
        table = self.config['migration']['dest_table']
        conn = self._pg_conn()
        cur = conn.cursor()
        cur.execute(f'TRUNCATE TABLE "{schema}"."{table}" CASCADE')
        conn.commit()
        cur.close()
        conn.close()
        self.log.info('Destino truncado.')

    # ─── otimização WAL ─────────────────────────────────────

    def _optimize_pg(self, conn):
        """
        [PERF] Otimizações PostgreSQL para carga em massa.
        Reduz WAL e melhora throughput.
        """
        cur = conn.cursor()
        perf = self.config.get('performance', {})

        cur.execute(f"SET work_mem = '{perf.get('work_mem', '256MB')}'")
        cur.execute(f"SET maintenance_work_mem = '{perf.get('maintenance_work_mem', '512MB')}'")
        cur.execute("SET synchronous_commit = off")
        cur.execute("SET jit = off")
        cur.execute("SET constraint_exclusion = on")

        # Se o usuário tiver permissão, tenta reduzir WAL
        try:
            cur.execute(f"ALTER TABLE \"{self.config['postgresql'].get('schema','public')}\""
                        f".\"{self.config['migration']['dest_table']}\" "
                        f"SET (autovacuum_enabled = false)")
            conn.commit()
            self.log.info('Autovacuum desabilitado na tabela destino.')
        except Exception:
            conn.rollback()
            self.log.debug('Não foi possível desabilitar autovacuum (sem permissão?).')

        conn.commit()

    def _restore_pg(self, conn):
        """Restaura configurações após carga."""
        cur = conn.cursor()
        cur.execute("SET synchronous_commit = on")
        cur.execute("SET jit = on")
        try:
            cur.execute(f"ALTER TABLE \"{self.config['postgresql'].get('schema','public')}\""
                        f".\"{self.config['migration']['dest_table']}\" "
                        f"SET (autovacuum_enabled = true)")
        except Exception:
            pass
        conn.commit()

    # ─── inserção: COPY protocol ────────────────────────────

    def _insert_copy(self, pg_conn, rows: list, batch_num: int):
        """
        [PERF] Insere via COPY protocol — 3-5× mais rápido que INSERT.
        [BUG FIX] BLOBs convertidos antes de serializar.
        """
        cur = pg_conn.cursor()
        cfg_pg = self.config['postgresql']
        schema = cfg_pg.get('schema', 'public')
        table = self.config['migration']['dest_table']
        col_names = ', '.join(f'"{c.name.lower()}"' for c in self.columns)
        copy_sql = f'COPY "{schema}"."{table}" ({col_names}) FROM STDIN'

        max_retries = self.config['migration'].get('max_retries', 3)
        col_count = len(self.columns)

        for attempt in range(max_retries):
            try:
                buf = io.StringIO()
                for row in rows:
                    converted = self._convert_row(row)
                    buf.write(_copy_row_str(converted, col_count))
                buf.seek(0)

                cur.copy_expert(copy_sql, buf)
                pg_conn.commit()
                return

            except Exception as e:
                pg_conn.rollback()
                if attempt < max_retries - 1:
                    self.log.warning(
                        f'Batch {batch_num}: tentativa {attempt+1} falhou ({e}). '
                        f'Dividindo em sub-batches...')
                    # Sub-batch: divide em 2 e tenta cada metade
                    mid = len(rows) // 2
                    if mid > 0:
                        self._insert_copy(pg_conn, rows[:mid], batch_num)
                        self._insert_copy(pg_conn, rows[mid:], batch_num)
                        return
                else:
                    self.log.error(
                        f'Batch {batch_num}: FALHA após {max_retries} tentativas: {e}')
                    self.progress.rows_failed += len(rows)
                    raise

    def _insert_values(self, pg_conn, rows: list, batch_num: int):
        """
        Fallback: INSERT via execute_values.
        Mais lento mas compatível com mais versões.
        """
        from psycopg2.extras import execute_values

        cur = pg_conn.cursor()
        cfg_pg = self.config['postgresql']
        schema = cfg_pg.get('schema', 'public')
        table = self.config['migration']['dest_table']
        col_names = ', '.join(f'"{c.name.lower()}"' for c in self.columns)
        template = f'INSERT INTO "{schema}"."{table}" ({col_names}) VALUES %s'

        max_retries = self.config['migration'].get('max_retries', 3)

        for attempt in range(max_retries):
            try:
                converted = [self._convert_row(r) for r in rows]
                execute_values(cur, template, converted, page_size=2000)
                pg_conn.commit()
                return
            except Exception as e:
                pg_conn.rollback()
                if attempt < max_retries - 1:
                    mid = len(rows) // 2
                    if mid > 0:
                        self._insert_values(pg_conn, rows[:mid], batch_num)
                        self._insert_values(pg_conn, rows[mid:], batch_num)
                        return
                else:
                    self.progress.rows_failed += len(rows)
                    raise

    def _insert_batch(self, pg_conn, rows: list, batch_num: int):
        """Despacha para COPY ou INSERT conforme configuração."""
        if self.use_insert:
            self._insert_values(pg_conn, rows, batch_num)
        else:
            self._insert_copy(pg_conn, rows, batch_num)

    # ─── conversão de linha ─────────────────────────────────

    def _convert_row(self, row: tuple) -> tuple:
        """
        [BUG FIX] Converte linha do Firebird para formato PG.
        BLOB TEXT: WIN1252→UTF-8. BLOB BINARY: bytes puros.
        """
        out = []
        for i, col in enumerate(self.columns):
            val = row[i] if i < len(row) else None

            if val is None:
                out.append(None)
                continue

            if col.is_blob:
                out.append(_convert_blob(val, col.blob_subtype, col.fb_charset))
            else:
                out.append(val)
        return tuple(out)

    # ─── query de leitura ───────────────────────────────────

    def _build_select_query(self, saved: Optional[MigrationProgress] = None
                            ) -> Tuple[str, tuple]:
        """
        Monta SELECT com paginação para restart.
        [BUG FIX] RDB$DB_KEY para tabelas sem PK — reiniciável.
        """
        tbl = self.config['migration']['source_table'].upper()
        pk_cols = self.progress.pk_columns

        if pk_cols and saved and saved.last_pk_value:
            # Restart com PK.
            # Para PK simples: WHERE pk > ?
            # Para PK composta: WHERE (a, b) > (?, ?) via OR-expansion explícita,
            # pois Firebird não suporta comparação de tuplas.
            # Ex: (a > ?) OR (a = ? AND b > ?)
            order = ', '.join(f'"{pk}"' for pk in pk_cols)
            params = saved.last_pk_value
            if not isinstance(params, (list, tuple)):
                params = [params]
            params = list(params)

            if len(pk_cols) == 1:
                cond = f'"{pk_cols[0]}" > ?'
                return (f'SELECT * FROM "{tbl}" WHERE {cond} ORDER BY {order}',
                        (params[0],))

            # Expansão OR para PK composta: (a > ?) OR (a = ? AND b > ?) OR ...
            clauses = []
            clause_params = []
            for i in range(len(pk_cols)):
                parts = []
                p = []
                for j in range(i):
                    parts.append(f'"{pk_cols[j]}" = ?')
                    p.append(params[j])
                parts.append(f'"{pk_cols[i]}" > ?')
                p.append(params[i])
                clauses.append('(' + ' AND '.join(parts) + ')')
                clause_params.extend(p)
            cond = ' OR '.join(clauses)
            return (f'SELECT * FROM "{tbl}" WHERE {cond} ORDER BY {order}',
                    tuple(clause_params))

        if pk_cols:
            order = ', '.join(f'"{pk}"' for pk in pk_cols)
            return f'SELECT * FROM "{tbl}" ORDER BY {order}', ()

        # Sem PK — usa RDB$DB_KEY
        if saved and saved.last_db_key:
            sql = f'SELECT * FROM "{tbl}" WHERE RDB$DB_KEY > ? ORDER BY RDB$DB_KEY'
            return sql, (saved.last_db_key,)

        return f'SELECT * FROM "{tbl}" ORDER BY RDB$DB_KEY', ()

    # ─── modo tabelas pequenas ──────────────────────────────

    def _discover_small_tables(self) -> list:
        """
        Auto-descobre todas as tabelas de usuário no Firebird via RDB$RELATIONS,
        excluindo as tabelas grandes configuradas em exclude_tables.
        Retorna lista de dicts {source, dest, state_db}.
        """
        cfg_m = self.config['migration']
        exclude = {t.strip().upper() for t in cfg_m.get('exclude_tables', [])}

        fb_conn = self._fb_conn()
        try:
            cur = fb_conn.cursor()
            cur.execute("""
                SELECT TRIM(r.RDB$RELATION_NAME)
                FROM RDB$RELATIONS r
                WHERE r.RDB$SYSTEM_FLAG = 0
                  AND r.RDB$VIEW_BLR IS NULL
                ORDER BY r.RDB$RELATION_NAME
            """)
            tables = []
            for row in cur.fetchall():
                src = row[0].strip().upper()
                if src not in exclude:
                    dest = src.lower()
                    state_db = str(BASE_DIR / f'migration_state_{dest}.db')
                    tables.append({
                        'source':   src,
                        'dest':     dest,
                        'state_db': state_db,
                    })
        finally:
            fb_conn.close()

        return tables

    def run_small_tables(self, dry_run: bool = False, reset: bool = False,
                         n_workers: int = None):
        """
        Migração paralela das ~901 tabelas pequenas em 4 fases:
          Fase 0 — Auto-descoberta via RDB$RELATIONS + disable de constraints
          Fase 1 — Carga paralela com ProcessPoolExecutor(n_workers)
          Fase 2 — Re-enable de constraints (ordem topológica)
          Fase 3 — Sumário final

        Em re-execução, tabelas já 'completed' no master state são puladas.
        Use --reset para recomeçar do zero.
        """
        cfg_m  = self.config['migration']
        cfg_pg = self.config['postgresql']
        schema = cfg_pg.get('schema', 'public')
        pg_params = {
            'host':     cfg_pg['host'],
            'port':     cfg_pg.get('port', 5432),
            'database': cfg_pg['database'],
            'user':     cfg_pg['user'],
            'password': cfg_pg['password'],
        }

        if n_workers is None:
            n_workers = cfg_m.get('parallel_workers', 4)
        master_db = str(BASE_DIR / cfg_m.get(
            'master_state_db', 'migration_state_smalltables_master.db'))

        self.log.info('=' * 70)
        self.log.info('  MIGRAÇÃO PARALELA DE TABELAS PEQUENAS')
        self.log.info(f'  Workers: {n_workers}')
        self.log.info('=' * 70)

        # ══════════════════════════════════════════════════════
        #  FASE 0 — Descoberta de tabelas
        # ══════════════════════════════════════════════════════
        self.log.info('')
        self.log.info('━' * 70)
        self.log.info('  FASE 0 — Descoberta de tabelas via RDB$RELATIONS')
        self.log.info('━' * 70)

        all_tables = self._discover_small_tables()
        self.log.info(f'  Tabelas descobertas: {len(all_tables)} '
                      f'(excluídas: {len(self.config["migration"].get("exclude_tables", []))})')

        master = MasterStateManager(master_db)
        master.register_tables(all_tables, reset=reset)

        if dry_run:
            self.log.info('  DRY-RUN: listando tabelas que seriam migradas:')
            for t in all_tables:
                self.log.info(f'    {t["source"]} → {t["dest"]}')
            self.log.info(f'  Total: {len(all_tables)} tabelas, {n_workers} workers')
            return

        pending = master.get_pending()
        self.log.info(f'  Pendentes: {len(pending)} | '
                      f'Já concluídas: {len(all_tables) - len(pending)}')

        if not pending:
            self.log.info('  Nenhuma tabela pendente. Use --reset para reprocessar.')
            return

        # ══════════════════════════════════════════════════════
        #  FASE 0b — Disable constraints
        # ══════════════════════════════════════════════════════
        self.log.info('')
        self.log.info('━' * 70)
        self.log.info(f'  FASE 0 — Desabilitando constraints ({len(pending)} tabelas)...')
        self.log.info('━' * 70)

        disable_warn = 0
        for i, t in enumerate(pending, 1):
            dest = t['dest']
            try:
                cman = ConstraintManager(pg_params, schema, dest)
                n_obj = cman.collect_all()
                state_path   = str(BASE_DIR / f'constraint_state_{dest}.json')
                disable_path = str(BASE_DIR / f'disable_constraints_{dest}.sql')
                enable_path  = str(BASE_DIR / f'enable_constraints_{dest}.sql')
                cman.save_state(state_path)
                with open(disable_path, 'w', encoding='utf-8') as f:
                    f.write(cman.generate_disable_script())
                with open(enable_path, 'w', encoding='utf-8') as f:
                    f.write(cman.generate_enable_script())
                if n_obj > 0:
                    cman.disable_all()
                if i % 100 == 0:
                    self.log.info(f'  [{i}/{len(pending)}] constraints processadas...')
            except Exception as e:
                disable_warn += 1
                self.log.warning(f'  [{dest}] Aviso no disable: {e}')

        self.log.info(f'  Constraints desabilitadas. '
                      f'Avisos: {disable_warn}')

        # ══════════════════════════════════════════════════════
        #  FASE 1 — Carga paralela
        # ══════════════════════════════════════════════════════
        self.log.info('')
        self.log.info('━' * 70)
        self.log.info(f'  FASE 1 — Carga paralela ({n_workers} workers, '
                      f'{len(pending)} tabelas)')
        self.log.info(f'  Monitor: python monitor.py --small-tables')
        self.log.info('━' * 70)

        args_list = [
            (self._config_path, t['source'], t['dest'],
             str(BASE_DIR / f'migration_state_{t["dest"]}.db'))
            for t in pending
        ]

        completed_count = 0
        failed_count = 0
        t_fase1 = time.time()

        mp_ctx = multiprocessing.get_context('spawn')
        with ProcessPoolExecutor(max_workers=n_workers, mp_context=mp_ctx) as pool:
            futures = {pool.submit(_worker_migrate_table, a): a for a in args_list}

            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as e:
                    a = futures[future]
                    result = {'table': a[1], 'status': 'failed',
                              'error': str(e), 'elapsed_sec': 0,
                              'rows_migrated': 0}

                tbl    = result['table']
                status = result.get('status', 'failed')

                if status == 'completed':
                    master.mark_completed(
                        tbl, result.get('rows_migrated', 0),
                        result.get('elapsed_sec', 0))
                    completed_count += 1
                    self.log.info(
                        f'  ✓ [{tbl}] {result.get("rows_migrated",0):,} linhas | '
                        f'{_fmt_dur(result.get("elapsed_sec",0))} | '
                        f'{completed_count}/{len(pending)}')
                else:
                    master.mark_failed(tbl, result.get('error', '?'))
                    failed_count += 1
                    self.log.error(
                        f'  ✗ [{tbl}] FALHOU: {result.get("error","?")[:120]}')

        elapsed_fase1 = time.time() - t_fase1
        self.log.info(f'  Fase 1 concluída em {_fmt_dur(elapsed_fase1)}. '
                      f'OK: {completed_count} | Falhas: {failed_count}')

        # ══════════════════════════════════════════════════════
        #  FASE 2 — Re-enable constraints
        # ══════════════════════════════════════════════════════
        self.log.info('')
        self.log.info('━' * 70)
        self.log.info('  FASE 2 — Reabilitando constraints...')
        self.log.info('━' * 70)

        completed_tables = master.get_completed_for_reenable()
        reenable_errors = []

        for i, t in enumerate(completed_tables, 1):
            dest = t['dest']
            state_path = str(BASE_DIR / f'constraint_state_{dest}.json')
            if not Path(state_path).exists():
                continue
            try:
                cman = ConstraintManager(pg_params, schema, dest)
                cman.load_state(state_path)
                if cman.dropped_objects:
                    cman.enable_all()
                if i % 100 == 0:
                    self.log.info(f'  [{i}/{len(completed_tables)}] constraints reabilitadas...')
            except Exception as e:
                reenable_errors.append(dest)
                self.log.warning(f'  [{dest}] Erro no re-enable: {e}')

        if reenable_errors:
            self.log.warning(
                f'  {len(reenable_errors)} tabelas com erro no re-enable.')
            self.log.warning(
                '  Execute manualmente: psql -f enable_constraints_{dest}.sql')
        else:
            self.log.info(f'  {len(completed_tables)} tabelas com constraints reabilitadas.')

        # ══════════════════════════════════════════════════════
        #  Sumário final
        # ══════════════════════════════════════════════════════
        self.log.info('')
        self.log.info('=' * 70)
        master.print_summary(self.log)
        if failed_count:
            self.log.warning(
                '  Tabelas com falha: execute novamente para retentar '
                '(completed são puladas automaticamente).')
        self.log.info('=' * 70)

    # ─── enable-constraints standalone ────────────────────────

    def run_enable_constraints(self, dry_run: bool = False):
        """
        Reabilita todos os constraints usando os constraint_state_*.json
        existentes no diretório de trabalho.

        Não depende do master state DB nem refaz nenhuma carga de dados.
        Usa os mesmos arquivos gerados pela FASE 0 do run_small_tables().

        Flags:
            --small-tables --enable-constraints           executa
            --small-tables --enable-constraints --dry-run lista o que faria
        """
        cfg_pg = self.config['postgresql']
        schema = cfg_pg.get('schema', 'public')
        pg_params = {
            'host':     cfg_pg['host'],
            'port':     cfg_pg.get('port', 5432),
            'database': cfg_pg['database'],
            'user':     cfg_pg['user'],
            'password': cfg_pg['password'],
        }

        state_files = sorted(BASE_DIR.glob('constraint_state_*.json'))
        if not state_files:
            self.log.error(
                '  Nenhum arquivo constraint_state_*.json encontrado. '
                'Execute primeiro a migração completa (FASE 0 gera esses arquivos).')
            return

        self.log.info('=' * 70)
        self.log.info('  RE-ENABLE DE CONSTRAINTS (modo standalone)')
        self.log.info(f'  Arquivos encontrados: {len(state_files)}')
        if dry_run:
            self.log.info('  DRY-RUN: nenhuma alteração será feita')
        self.log.info('=' * 70)

        ok_count    = 0
        skip_count  = 0
        error_count = 0
        errors      = []

        for i, state_path in enumerate(state_files, 1):
            # Extrai nome da tabela do nome do arquivo: constraint_state_{dest}.json
            dest = state_path.stem.removeprefix('constraint_state_')

            if i % 50 == 0 or i == len(state_files):
                self.log.info(f'  [{i:>4}/{len(state_files)}] processando {dest}...')

            if dry_run:
                continue

            try:
                cman = ConstraintManager(pg_params, schema, dest)
                cman.load_state(str(state_path))
                if not cman.dropped_objects:
                    skip_count += 1
                    continue
                cman.enable_all()
                ok_count += 1
            except Exception as e:
                error_count += 1
                errors.append((dest, str(e)))
                self.log.warning(f'  [{dest}] ERRO: {e}')

        self.log.info('')
        self.log.info('─' * 70)
        if dry_run:
            self.log.info(f'  DRY-RUN: {len(state_files)} tabelas seriam processadas.')
        else:
            self.log.info(f'  Reabilitadas : {ok_count}')
            self.log.info(f'  Sem objetos  : {skip_count}')
            self.log.info(f'  Erros        : {error_count}')
            if errors:
                self.log.warning('  Tabelas com erro (execute manualmente):')
                for dest, msg in errors:
                    self.log.warning(f'    psql -f enable_constraints_{dest}.sql')
                    self.log.warning(f'    Motivo: {msg}')
        self.log.info('─' * 70)

    # ─── loop principal ─────────────────────────────────────

    def run(self, dry_run=False, scripts_only=False):
        """
        Orquestra a migração em 3 fases globais:
          Fase 0 — Coleta DDLs do PG e desabilita constraints de TODAS as tabelas.
          Fase 1 — Carrega dados de cada tabela (sequencialmente).
          Fase 2 — Recria constraints de TODAS as tabelas ao final.

        Arquivos gerados por tabela (safety net para reexecução manual):
          disable_constraints_{dest}.sql
          enable_constraints_{dest}.sql
          constraint_state_{dest}.json
          migration_state_{dest}.db
        """
        self.log.info('=' * 70)
        self.log.info('  MIGRAÇÃO FIREBIRD 3 → POSTGRESQL')
        self.log.info('=' * 70)

        tables = self._resolve_tables()
        self.log.info(f'Tabelas configuradas: {len(tables)}')
        for t in tables:
            self.log.info(f'  {t["source"]} → {t["dest"]}')

        cfg_pg = self.config['postgresql']
        schema = cfg_pg.get('schema', 'public')
        pg_params = {
            'host':     cfg_pg['host'],
            'port':     cfg_pg.get('port', 5432),
            'database': cfg_pg['database'],
            'user':     cfg_pg['user'],
            'password': cfg_pg['password'],
        }

        # ══════════════════════════════════════════════════════
        #  FASE 0 — Coletar DDLs e desabilitar constraints
        # ══════════════════════════════════════════════════════
        self.log.info('')
        self.log.info('━' * 70)
        self.log.info('  FASE 0 — Coleta de DDLs e desabilitação de constraints')
        self.log.info('━' * 70)

        prep = []   # lista de (tbl_cfg, cman, state_path)
        for tbl_cfg in tables:
            dest = tbl_cfg['dest']
            self.log.info(f'  [{dest}] Coletando constraints do PostgreSQL...')

            cman = ConstraintManager(pg_params, schema, dest)
            n_obj = cman.collect_all()

            state_path   = f'constraint_state_{dest}.json'
            disable_path = f'disable_constraints_{dest}.sql'
            enable_path  = f'enable_constraints_{dest}.sql'

            cman.save_state(state_path)
            with open(disable_path, 'w', encoding='utf-8') as f:
                f.write(cman.generate_disable_script())
            with open(enable_path, 'w', encoding='utf-8') as f:
                f.write(cman.generate_enable_script())

            self.log.info(f'  [{dest}] {n_obj} objetos — scripts salvos: '
                          f'{disable_path}, {enable_path}')
            prep.append((tbl_cfg, cman, state_path))

        if scripts_only:
            self.log.info('Modo --generate-scripts-only. Encerrando.')
            return

        if not dry_run:
            self.log.info('')
            self.log.info('Desabilitando constraints em todas as tabelas...')
            for tbl_cfg, cman, state_path in prep:
                dest = tbl_cfg['dest']
                self.log.info(f'  [{dest}] Removendo constraints/índices...')
                cman.load_state(state_path)
                cman.disable_all()

        # ══════════════════════════════════════════════════════
        #  FASE 1 — Carga de dados (uma tabela por vez)
        # ══════════════════════════════════════════════════════
        self.log.info('')
        self.log.info('━' * 70)
        self.log.info('  FASE 1 — Carga de dados')
        self.log.info('━' * 70)

        for tbl_cfg, _cman, _state_path in prep:
            if self._shutdown:
                break
            self._load_table(tbl_cfg, dry_run)

        # ── Resumo final ─────────────────────────────────────
        self.log.info('')
        self.log.info('=' * 70)
        if self._shutdown:
            self.log.warning('⚠  Migração interrompida.')
            self.log.warning('   Constraints ainda desabilitadas.')
        elif not dry_run:
            self.log.info('  CARGA CONCLUÍDA ✓')
        self.log.info('  Scripts para recriar constraints manualmente:')
        for tbl_cfg, _cman, _state_path in prep:
            dest = tbl_cfg['dest']
            self.log.info(f'    psql -f enable_constraints_{dest}.sql')
        self.log.info('=' * 70)

    def _load_table(self, tbl_cfg: dict, dry_run: bool):
        """
        Fase 1 para uma tabela: checkpoint, truncate, carga de dados.
        Assume que constraints já foram desabilitadas na Fase 0.
        """
        source    = tbl_cfg['source']
        dest      = tbl_cfg['dest']
        state_db  = tbl_cfg['state_db']

        cfg_m  = self.config['migration']
        cfg_pg = self.config['postgresql']
        schema     = cfg_pg.get('schema', 'public')
        batch_size = cfg_m.get('batch_size', 5000)

        # Ajusta config para métodos internos que lêem source/dest_table
        cfg_m['source_table'] = source
        cfg_m['dest_table']   = dest

        self._state   = StateManager(state_db)
        self.progress = MigrationProgress()

        self.log.info('')
        self.log.info(f'  ── {source} → {dest} ──')

        # ── Metadados ────────────────────────────────────────
        self.columns = self._discover_columns()
        blob_cols = [c for c in self.columns if c.is_blob]
        self.log.info(f'  Colunas: {len(self.columns)} '
                      f'(BLOBs: {[c.name for c in blob_cols]})')

        pk_cols    = self._discover_pk()
        use_db_key = not pk_cols
        self.log.info(
            f'  PK: {pk_cols if pk_cols else "(nenhuma — usando RDB$DB_KEY)"}')

        self._check_dest_table()

        # ── Checkpoint ───────────────────────────────────────
        saved      = self._state.load_progress()
        is_restart = False

        if (saved and saved.status in ('running', 'paused', 'error')
                and saved.source_table == source and saved.rows_migrated > 0):
            is_restart = True
            self.log.info('')
            self.log.info('  ╔' + '═' * 56 + '╗')
            self.log.info(f'  ║  RESTART — {saved.rows_migrated:>10,} linhas migradas'
                          + ' ' * (19 - len(f'{saved.rows_migrated:,}')) + '║')
            self.log.info(f'  ║  Batch: {saved.current_batch:>8,}  '
                          f'Status: {saved.status:<12}    ║')
            self.log.info('  ╚' + '═' * 56 + '╝')
            self.log.info('')

            self.log.info('  Retomando automaticamente do checkpoint.')

        if not is_restart:
            self._state.reset()

        # ── Contagem ─────────────────────────────────────────
        if not is_restart or not saved.total_rows:
            total_rows = self._count_rows()
        else:
            total_rows = saved.total_rows

        total_batches = max(1, (total_rows + batch_size - 1) // batch_size)

        if dry_run:
            self.log.info(f'  DRY-RUN: {total_rows:,} linhas, '
                          f'{total_batches:,} batches × {batch_size:,}')
            self.log.info(f'  Modo: {"COPY" if not self.use_insert else "INSERT"}')
            return

        # ── Truncar (somente em run fresh) ───────────────────
        if not is_restart:
            self._truncate_dest()

        # ── Progresso inicial ─────────────────────────────────
        if is_restart:
            self.progress = saved
            self.progress.status = 'running'
            self.progress.phase  = 'migrating'
        else:
            self.progress = MigrationProgress(
                source_table=source, dest_table=dest,
                total_rows=total_rows, batch_size=batch_size,
                total_batches=total_batches, pk_columns=pk_cols,
                use_db_key=use_db_key,
                status='running', phase='migrating',
                started_at=datetime.now().isoformat(),
                constraints_disabled=True)
        self._state.save_progress(self.progress)

        # ── Carga ────────────────────────────────────────────
        self.log.info(f'  Modo: {"COPY" if not self.use_insert else "INSERT"} | '
                      f'Batch: {batch_size:,} | Total: {total_batches:,}')
        if is_restart:
            self.log.info(f'  Retomando do batch {self.progress.current_batch + 1}')

        fb_conn = self._fb_conn()
        pg_conn = self._pg_conn()

        try:
            self._optimize_pg(pg_conn)

            fb_cur = fb_conn.cursor()
            fb_cur.arraysize = cfg_m.get('fetch_array_size', 10000)

            select_sql, select_params = self._build_select_query(
                saved if is_restart else None)
            self.log.info(f'  Query: {select_sql[:100]}...')
            fb_cur.execute(select_sql, select_params)

            if is_restart and use_db_key and not saved.last_db_key:
                skip = saved.rows_migrated
                if skip > 0:
                    self.log.warning(f'  Descartando {skip:,} linhas (sem PK)...')
                    for _ in range(skip):
                        fb_cur.fetchone()

            t0        = time.time()
            last_save = t0
            batch_num = self.progress.current_batch
            batch_buf = []

            while not self._shutdown:
                row = fb_cur.fetchone()

                if row is None:
                    if batch_buf:
                        batch_num += 1
                        self._insert_batch(pg_conn, batch_buf, batch_num)
                        self._update_progress(batch_buf, batch_num, t0)
                        batch_buf = []
                    break

                batch_buf.append(row)

                if len(batch_buf) >= batch_size:
                    batch_num += 1
                    self._insert_batch(pg_conn, batch_buf, batch_num)
                    self._update_progress(batch_buf, batch_num, t0)
                    batch_buf = []
                    gc.collect()

                    if time.time() - last_save > 10:
                        self._state.save_progress(self.progress)
                        last_save = time.time()

            # ── Finalização da carga ──────────────────────────
            if self._shutdown:
                self.progress.status = 'paused'
                self.progress.phase  = 'paused'
                self.progress.updated_at = datetime.now().isoformat()
                self._state.save_progress(self.progress)
                self.log.warning(f'  ⏸  [{source}] PAUSADO. '
                                 'Execute novamente para continuar.')
            else:
                elapsed = time.time() - t0
                # status 'loaded': dados ok, constraints ainda desabilitadas
                # (serão recriadas na Fase 2)
                self.progress.status       = 'loaded'
                self.progress.phase        = 'loaded'
                self.progress.completed_at = datetime.now().isoformat()
                self.progress.elapsed_seconds = elapsed
                if elapsed > 0:
                    self.progress.speed_rows_per_sec = (
                        self.progress.rows_migrated / elapsed)
                self._state.save_progress(self.progress)

                self.log.info('')
                self.log.info('  ╔' + '═' * 56 + '╗')
                self.log.info(f'  ║  [{source}] CARGA CONCLUÍDA'
                              + ' ' * 26 + '║')
                self.log.info(f'  ║  Linhas: {self.progress.rows_migrated:>10,}  '
                              f'Erros: {self.progress.rows_failed:>6,}'
                              + ' ' * 8 + '║')
                self.log.info(f'  ║  Tempo: {_fmt_dur(elapsed):<14}  '
                              f'Vel: {self.progress.speed_rows_per_sec:>10,.0f} l/s  ║')
                self.log.info('  ╚' + '═' * 56 + '╝')

        finally:
            fb_conn.close()
            self._restore_pg(pg_conn)
            pg_conn.close()

    def _update_progress(self, batch_rows: list, batch_num: int, t0: float):
        """Atualiza progresso, calcula ETA, faz log."""
        n = len(batch_rows)
        self.progress.rows_migrated += n
        self.progress.current_batch = batch_num

        # Checkpoint PK
        if self.progress.pk_columns:
            last = batch_rows[-1]
            self.progress.last_pk_value = [
                last[self._col_index(pk)] for pk in self.progress.pk_columns
            ]
        elif self.progress.use_db_key:
            # RDB$DB_KEY é sempre a última coluna se selecionada,
            # mas como usamos SELECT * e ORDER BY RDB$DB_KEY,
            # guardamos a posição atual
            pass  # restart sem PK faz skip por contagem

        elapsed = time.time() - t0
        migrated = self.progress.rows_migrated
        total = self.progress.total_rows
        speed = migrated / elapsed if elapsed > 0 else 0
        remaining = total - migrated
        eta = remaining / speed if speed > 0 else 0

        self.progress.speed_rows_per_sec = speed
        self.progress.eta_seconds = eta
        self.progress.elapsed_seconds = elapsed
        self.progress.updated_at = datetime.now().isoformat()

        pct = (migrated / total * 100) if total > 0 else 0
        bar_len = 30
        filled = int(bar_len * pct / 100)
        bar = '█' * filled + '░' * (bar_len - filled)

        self.log.info(
            f'  [{bar}] {pct:5.1f}% | '
            f'{migrated:>12,}/{total:>12,} | '
            f'Lote {batch_num:>5,} | '
            f'{speed:>10,.0f} lin/s | '
            f'ETA {_fmt_dur(eta):>10} | '
            f'{_fmt_dur(elapsed)}')

        self._state.log_batch(batch_num, n, migrated, speed, eta)


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(
        description='Migra Firebird 3 -> PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos (tabelas grandes — modo original):
  python migrator_smalltables.py --table CONTROLEVERSAO
  python migrator_smalltables.py --table LOG_EVENTOS --dry-run
  python migrator_smalltables.py --table CONTROLEVERSAO --generate-scripts-only

Exemplos (901 tabelas pequenas — modo paralelo):
  python migrator_smalltables.py --small-tables
  python migrator_smalltables.py --small-tables --dry-run
  python migrator_smalltables.py --small-tables --reset
  python migrator_smalltables.py --small-tables --workers 8
  python migrator_smalltables.py --small-tables -c config_smalltables.yaml

  # Monitor em outra janela:
  python monitor.py --small-tables
        """)
    p.add_argument('-c', '--config', default='config.yaml',
                   help='Arquivo de configuração (padrão: config.yaml). '
                        'Para --small-tables use config_smalltables.yaml.')
    p.add_argument('--table', type=str, default=None,
                   metavar='NOME',
                   help='Nome da tabela a migrar. '
                        'Firebird usa MAIÚSCULAS, PostgreSQL usa minúsculas '
                        '(conversão automática). Sobrescreve o config.yaml.')
    p.add_argument('--small-tables', action='store_true',
                   help='Migra as ~901 tabelas pequenas em paralelo. '
                        'Use config_smalltables.yaml como configuração. '
                        'Tabelas completed são puladas automaticamente.')
    p.add_argument('--enable-constraints', action='store_true',
                   help='Reabilita constraints usando os constraint_state_*.json '
                        'existentes (requer --small-tables). '
                        'Útil quando a FASE 2 não chegou a executar. '
                        'Não refaz nenhuma carga de dados.')
    p.add_argument('--workers', type=int, default=None,
                   metavar='N',
                   help='Número de processos paralelos para --small-tables '
                        '(sobrescreve parallel_workers do config).')
    p.add_argument('--log-file', type=str, default=None,
                   metavar='ARQUIVO',
                   help='Arquivo de log (padrão: migration_{tabela}.log quando '
                        '--table é usado, ou o valor do config.yaml).')
    p.add_argument('--reset', action='store_true',
                   help='Descarta checkpoint e reinicia do zero. '
                        'Para --small-tables: reseta o master state completo.')
    p.add_argument('--dry-run', action='store_true',
                   help='Mostra estatísticas sem escrever dados.')
    p.add_argument('--generate-scripts-only', action='store_true',
                   help='Apenas gera os scripts SQL de constraints.')
    p.add_argument('--batch-size', type=int, default=None,
                   help='Linhas por batch (sobrescreve config.yaml).')
    p.add_argument('--use-insert', action='store_true',
                   help='Usa INSERT em vez de COPY (mais lento, mais compatível).')
    args = p.parse_args()

    # Para --small-tables, usar config_smalltables.yaml por padrão
    config_file = args.config
    if args.small_tables and config_file == 'config.yaml':
        config_file = 'config_smalltables.yaml'

    if not Path(config_file).exists():
        print(f'ERRO: {config_file} não encontrado.')
        sys.exit(1)

    # ── Modo --small-tables: migração paralela das 901 tabelas ──
    if args.small_tables:
        m = FirebirdToPgMigrator(config_file,
                                  override_batch_size=args.batch_size,
                                  use_insert=args.use_insert)
        try:
            if args.enable_constraints:
                m.run_enable_constraints(dry_run=args.dry_run)
            else:
                m.run_small_tables(
                    dry_run=args.dry_run,
                    reset=args.reset,
                    n_workers=args.workers,
                )
        except KeyboardInterrupt:
            print('\nInterrompido.')
            sys.exit(130)
        except Exception as e:
            logging.getLogger('migrator').error(f'Fatal: {e}', exc_info=True)
            sys.exit(1)
        return

    # ── Modo original: --table ou lista do config ────────────────
    # Derivar log file automaticamente quando --table é usado
    log_file = args.log_file
    if log_file is None and args.table:
        log_file = f'migration_{args.table.lower()}.log'

    m = FirebirdToPgMigrator(config_file,
                              override_batch_size=args.batch_size,
                              use_insert=args.use_insert,
                              override_table=args.table,
                              override_log_file=log_file)

    if args.reset:
        for tbl_cfg in m._resolve_tables():
            StateManager(tbl_cfg['state_db']).reset()
        if not (args.dry_run or args.generate_scripts_only):
            print('Checkpoints resetados. Iniciando...')

    try:
        m.run(dry_run=args.dry_run, scripts_only=args.generate_scripts_only)
    except KeyboardInterrupt:
        print('\nInterrompido.')
        sys.exit(130)
    except Exception as e:
        logging.getLogger('migrator').error(f'Fatal: {e}', exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
