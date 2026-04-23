#!/usr/bin/env python3
"""
migrator_parallel_doc_oper.py
==============================
Migra DOCUMENTO_OPERACAO (Firebird 3 -> PostgreSQL) em N threads paralelas.

Particionamento por range de NU_OPERACAO (chave líder da PK composta).
Cada thread migra um slice com checkpoint independente e log próprio.
O monitor.py acompanha o progresso agregado via migration_state_documento_operacao.db.

Uso:
    source .venv/bin/activate
    export PYTHONIOENCODING=utf-8
    python migrator_parallel_doc_oper.py --threads 4
    python migrator_parallel_doc_oper.py --threads 4 --reset
    python migrator_parallel_doc_oper.py --threads 4 --dry-run
    python migrator_parallel_doc_oper.py --threads 4 --batch-size 5000
    python migrator_parallel_doc_oper.py --threads 4 --use-insert
    python migrator_parallel_doc_oper.py --threads 4 --generate-scripts-only

Arquivos gerados:
    migration_state_documento_operacao.db          -> monitor.py (progresso agregado)
    migration_state_documento_operacao_tN.db       -> checkpoint individual por thread
    migration_documento_operacao_tN.log            -> log individual por thread
    migration_documento_operacao_parallel.log      -> log do orquestrador
    disable_constraints_documento_operacao.sql
    enable_constraints_documento_operacao.sql
    constraint_state_documento_operacao.json
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

if os.name == "nt":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    _fb_paths = [
        os.path.join(script_dir, "fbclient.dll"),
        os.path.abspath("fbclient.dll"),
        r"C:\Program Files\Firebird\Firebird_3_0\fbclient.dll",
        r"C:\Program Files\Firebird\Firebird_4_0\fbclient.dll",
        r"C:\Program Files\Firebird\Firebird_5_0\fbclient.dll",
        r"C:\Program Files\Firebird\Firebird_2_5\bin\fbclient.dll",
        r"C:\Program Files (x86)\Firebird\Firebird_3_0\fbclient.dll",
        r"C:\Program Files (x86)\Firebird\Firebird_2_5\bin\fbclient.dll",
    ]
    for _p in _fb_paths:
        if os.path.exists(_p):
            try:
                fdb.load_api(_p)
                break
            except Exception:
                pass

                break
            except Exception:
                pass

from pg_constraints import ConstraintManager
from lib.state import StateManager, MigrationProgress

BASE_DIR     = Path(__file__).parent
WORK_DIR     = BASE_DIR / 'work'
LOG_DIR      = BASE_DIR / 'logs'
WORK_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)
SOURCE_TABLE = 'DOCUMENTO_OPERACAO'
DEST_TABLE   = 'documento_operacao'
PK_COLS      = ['NU_OPERACAO', 'NU_DOCUMENTO']
PART_COL     = 'NU_OPERACAO'          # coluna líder para particionamento por range


# ═══════════════════════════════════════════════════════════════
#  ESTRUTURAS DE DADOS
# ═══════════════════════════════════════════════════════════════

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
#  MAPEAMENTO DE TIPOS FIREBIRD -> POSTGRESQL
# ═══════════════════════════════════════════════════════════════

_FB_TYPES = {
    7: 'SMALLINT', 8: 'INTEGER', 10: 'REAL', 12: 'DATE',
    13: 'TIME', 14: 'CHAR', 16: 'BIGINT', 27: 'DOUBLE PRECISION',
    35: 'TIMESTAMP', 37: 'VARCHAR', 261: 'BLOB',
}


def map_fb_to_pg(type_code: int, subtype: int, length: int,
                 precision: int, scale: int) -> Tuple[str, bool]:
    if type_code == 261:
        return ('TEXT', True) if subtype == 1 else ('BYTEA', True)
    if type_code in (8, 16) and precision > 0:
        return f'NUMERIC({precision},{abs(scale)})', False
    base = _FB_TYPES.get(type_code, 'TEXT')
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


_FB_CHARSET_TO_PYTHON = {
    'WIN1252': 'cp1252', 'WIN1250': 'cp1250', 'WIN1251': 'cp1251',
    'WIN1253': 'cp1253', 'WIN1254': 'cp1254',
    'ISO8859_1': 'iso-8859-1', 'ISO8859_2': 'iso-8859-2', 'ISO8859_15': 'iso-8859-15',
    'UTF8': 'utf-8', 'UNICODE_FSS': 'utf-8', 'ASCII': 'ascii', 'NONE': 'latin-1',
}

_CONFIG_CHARSET_TO_FB = {
    'iso-8859-1': 'ISO8859_1', 'iso8859-1': 'ISO8859_1', 'iso_8859-1': 'ISO8859_1',
    'latin1': 'ISO8859_1', 'latin-1': 'ISO8859_1',
    'win1252': 'WIN1252', 'windows-1252': 'WIN1252', 'cp1252': 'WIN1252',
    'utf-8': 'UTF8', 'utf8': 'UTF8',
}


def _fb_charset_for_connect(raw: str) -> str:
    return _CONFIG_CHARSET_TO_FB.get(raw.lower(), raw.upper())


def _fb_charset_to_python(fb_charset: str) -> str:
    return _FB_CHARSET_TO_PYTHON.get(fb_charset.upper(), 'latin-1')


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
        return text.replace('\x00', '') if '\x00' in text else text
    if isinstance(val, bytes):
        return val
    if isinstance(val, str):
        return val.encode('latin-1')
    return bytes(val)


def _copy_escape(val) -> str:
    if val is None:
        return '\\N'
    if isinstance(val, bytes):
        return '\\\\x' + val.hex()
    if isinstance(val, bool):
        return 't' if val else 'f'
    if isinstance(val, (int, float)):
        return str(val)
    if hasattr(val, 'isoformat'):
        return val.isoformat()
    s = str(val).encode('latin-1', errors='replace').decode('latin-1')
    s = s.replace('\x00', '')
    return (s.replace('\\', '\\\\')
             .replace('\t', '\\t')
             .replace('\n', '\\n')
             .replace('\r', '\\r'))


def _copy_row_str(row: tuple, col_count: int) -> str:
    return '\t'.join(
        _copy_escape(row[i]) if i < len(row) else '\\N'
        for i in range(col_count)
    ) + '\n'


# ═══════════════════════════════════════════════════════════════
#  CONEXÕES (helpers de módulo)
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
#  DESCOBERTA DE METADADOS
# ═══════════════════════════════════════════════════════════════

def discover_columns(config: dict) -> List[ColumnMeta]:
    """Lê metadados de colunas de SOURCE_TABLE no Firebird."""
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
            name             = row[0].strip()
            tc, st           = row[1], row[2]
            ln, pr, sc       = row[3], row[4], row[5]
            nullable         = row[6] is None
            pos              = row[7]
            charset          = row[8].strip()
            pg_type, is_blob = map_fb_to_pg(tc, st, ln, pr, sc)
            # força BYTEA para colunas binárias conhecidas
            if is_blob and name.upper() in ('DADO', 'TE_IMAGEM_REDUZIDA', 'IMAGEM'):
                is_blob, st, pg_type = True, 0, 'BYTEA'
            cols.append(ColumnMeta(
                name=name, fb_type_code=tc, pg_type=pg_type,
                is_blob=is_blob, blob_subtype=st,
                fb_charset=charset, nullable=nullable, position=pos))
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


def compute_pk_ranges(config: dict, n_threads: int, total_rows: int,
                      log: logging.Logger) -> List[dict]:
    """
    Calcula N ranges balanceados de PART_COL via percentile queries.

    Usa FIRST 1 SKIP K para obter pontos de corte equidistantes por contagem,
    garantindo ranges com aproximadamente o mesmo número de linhas independente
    da distribuição de valores.

    Retorna lista de dicts: {start, end, is_last, rows}.
    """
    conn = _fb_conn(config)
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT MIN("{PART_COL}") FROM "{SOURCE_TABLE}"')
        min_val = cur.fetchone()[0]

        if n_threads == 1:
            return [{'start': min_val, 'end': None, 'is_last': True, 'rows': total_rows}]

        # Obtém N-1 pontos de corte equidistantes por posição de linha
        step = total_rows // n_threads
        split_points: List[Any] = []
        seen: set = set()
        for i in range(1, n_threads):
            skip = step * i
            cur.execute(
                f'SELECT FIRST 1 SKIP ? "{PART_COL}" '
                f'FROM "{SOURCE_TABLE}" ORDER BY "{PART_COL}"',
                (skip,))
            row = cur.fetchone()
            if row and row[0] is not None and row[0] not in seen:
                seen.add(row[0])
                split_points.append(row[0])
    finally:
        conn.close()

    if len(split_points) < n_threads - 1:
        log.warning(
            f'Apenas {len(split_points) + 1} ranges distintos disponíveis '
            f'(solicitado: {n_threads}). Ajustando para {len(split_points) + 1} threads.')

    # starts = [min_val, sp[0], sp[1], ...]
    # ends   = [sp[0],   sp[1], ...,   None]
    starts    = [min_val] + split_points
    ends      = split_points + [None]
    rows_each = max(1, total_rows // len(starts))

    return [
        {'start': s, 'end': e, 'is_last': e is None, 'rows': rows_each}
        for s, e in zip(starts, ends)
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
#  AGGREGATOR THREAD
# ═══════════════════════════════════════════════════════════════

class AggregatorThread(threading.Thread):
    """
    Lê os state DBs de cada worker a cada 2s e escreve o progresso
    agregado em migration_state_documento_operacao.db para o monitor.py.

    Usa daemon=True para não bloquear o processo se o main thread sair inesperadamente.
    """

    def __init__(self, worker_db_paths: List[Path], master_state: StateManager,
                 shutdown_event: threading.Event, total_rows: int):
        super().__init__(name='aggregator', daemon=True)
        self.worker_db_paths = worker_db_paths
        self.master          = master_state
        self.shutdown        = shutdown_event
        self.total_rows      = total_rows

    def _read_worker(self, db_path: Path) -> Optional[MigrationProgress]:
        """Lê progresso de um worker DB sem criar StateManager (evita criar arquivos)."""
        if not db_path.exists():
            return None
        try:
            conn = sqlite3.connect(str(db_path), timeout=3)
            conn.execute('PRAGMA journal_mode=WAL')
            row = conn.execute(
                'SELECT progress_json FROM migration_state WHERE id=1'
            ).fetchone()
            conn.close()
            return MigrationProgress.from_dict(json.loads(row[0])) if row else None
        except Exception:
            return None

    def aggregate(self):
        total_migrated = 0
        total_failed   = 0
        total_speed    = 0.0
        max_eta        = 0.0
        max_elapsed    = 0.0
        any_running    = False
        any_error      = False

        for db_path in self.worker_db_paths:
            p = self._read_worker(db_path)
            if p is None:
                any_running = True   # worker ainda não escreveu estado inicial
                continue
            total_migrated += p.rows_migrated
            total_failed   += p.rows_failed
            total_speed    += (p.speed_rows_per_sec or 0.0)
            # elapsed_seconds was not in MigrationProgress in some versions, but it is in our new one
            max_elapsed     = max(max_elapsed, getattr(p, 'elapsed_seconds', 0.0) or 0.0)
            if p.eta_seconds:
                max_eta = max(max_eta, p.eta_seconds)
            if p.status == 'running':
                any_running = True
            elif p.status == 'error':
                any_error = True

        status = 'running' if any_running else ('error' if any_error else 'completed')

        self.master.save_progress(MigrationProgress(
            source_table=SOURCE_TABLE, dest_table=DEST_TABLE,
            total_rows=self.total_rows,
            rows_migrated=total_migrated, rows_failed=total_failed,
            status=status,
            speed_rows_per_sec=total_speed,
            eta_seconds=max_eta,
            # elapsed_seconds=max_elapsed, # Removed as it's not in lib.state.MigrationProgress
            updated_at=datetime.now().isoformat(),
            pk_columns=PK_COLS,
            category='parallel_pk'
        ))

    def run(self):
        while not self.shutdown.is_set():
            try:
                self.aggregate()
            except Exception:
                pass
            # wait() retorna imediatamente se shutdown for setado -> saída limpa
            self.shutdown.wait(2.0)
        # Agregação final após todos os workers terminarem
        try:
            self.aggregate()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
#  WORKER THREAD
# ═══════════════════════════════════════════════════════════════

class WorkerThread(threading.Thread):
    """
    Migra o slice WHERE NU_OPERACAO >= range_start [AND NU_OPERACAO < range_end].

    Cada worker tem:
      - Conexão FB e PG próprias (não thread-safe para compartilhamento)
      - State DB próprio para checkpoint/resume
      - Logger próprio gravando em arquivo individual
    """

    def __init__(self, thread_id: int, config: dict, columns: List[ColumnMeta],
                 range_start: Any, range_end: Optional[Any], is_last: bool,
                 total_rows_in_range: int, shutdown_event: threading.Event,
                 use_insert: bool = False):
        super().__init__(name=f'worker-{thread_id}', daemon=False)
        self.tid                  = thread_id
        self.config               = config
        self.columns              = columns
        self.range_start          = range_start
        self.range_end            = range_end
        self.is_last              = is_last
        self._total_rows_in_range = total_rows_in_range
        self.shutdown             = shutdown_event
        self.use_insert           = use_insert

        self.state_db_path = WORK_DIR / f'migration_state_{DEST_TABLE}_t{thread_id}.db'
        self.log_file      = LOG_DIR  / f'migration_{DEST_TABLE}_t{thread_id}.log'
        self.state         = StateManager(self.state_db_path) # Workers always use local DB for their own slice
        self.progress      = MigrationProgress()
        self.exception: Optional[Exception] = None

        # Índices das colunas PK — calculados uma vez
        self._pk_idx_op  = next((i for i, c in enumerate(columns) if c.name == 'NU_OPERACAO'),  0)
        self._pk_idx_doc = next((i for i, c in enumerate(columns) if c.name == 'NU_DOCUMENTO'), 1)

        self._setup_logger()

    def _setup_logger(self):
        self.log = logging.getLogger(f'worker-{self.tid}')
        self.log.setLevel(logging.INFO)
        self.log.propagate = False   # não borbulha para o root logger
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

    # ── conexões ─────────────────────────────────────────────

    def _fb_conn(self):
        return _fb_conn(self.config)

    def _pg_conn(self):
        return _pg_conn(self.config)

    # ── otimizações PG ───────────────────────────────────────

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
            self.log.debug('Autovacuum desabilitado.')
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
        except Exception as e:
            self.log.warning(f'Falha ao reabilitar autovacuum em '
                             f'"{DEST_TABLE}": {e}. Verificar manualmente.')
        conn.commit()

    # ── conversão de linha ───────────────────────────────────

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

    # ── query com range + checkpoint ─────────────────────────

    def _build_select(self, saved: Optional[MigrationProgress]) -> Tuple[str, tuple]:
        """
        Monta SELECT filtrando pelo range de NU_OPERACAO deste worker,
        com cláusula de checkpoint para resume quando disponível.

        Sem checkpoint (fresh start):
            WHERE "NU_OPERACAO" >= start [AND "NU_OPERACAO" < end]
            ORDER BY "NU_OPERACAO", "NU_DOCUMENTO"

        Com checkpoint (resume):
            WHERE ("NU_OPERACAO" > last_op
                   OR ("NU_OPERACAO" = last_op AND "NU_DOCUMENTO" > last_doc))
              [AND "NU_OPERACAO" < end]
            ORDER BY "NU_OPERACAO", "NU_DOCUMENTO"
        """
        order        = '"NU_OPERACAO", "NU_DOCUMENTO"'
        upper_cond   = '' if self.is_last else ' AND "NU_OPERACAO" < ?'
        upper_params = [] if self.is_last else [self.range_end]

        # Checkpoint válido?
        has_ck = (
            saved is not None
            and saved.last_pk_value is not None
            and isinstance(saved.last_pk_value, (list, tuple))
            and len(saved.last_pk_value) >= 2
            and saved.rows_migrated > 0
        )

        if not has_ck:
            return (
                f'SELECT * FROM "{SOURCE_TABLE}" '
                f'WHERE "NU_OPERACAO" >= ?{upper_cond} '
                f'ORDER BY {order}',
                tuple([self.range_start] + upper_params),
            )

        last_op, last_doc = saved.last_pk_value[0], saved.last_pk_value[1]
        ck_cond   = '("NU_OPERACAO" > ? OR ("NU_OPERACAO" = ? AND "NU_DOCUMENTO" > ?))'
        ck_params = [last_op, last_op, last_doc]

        return (
            f'SELECT * FROM "{SOURCE_TABLE}" '
            f'WHERE {ck_cond}{upper_cond} '
            f'ORDER BY {order}',
            tuple(ck_params + upper_params),
        )

    # ── inserção: COPY ou INSERT ─────────────────────────────

    def _insert_copy(self, pg_conn, rows: list, batch_num: int):
        cur       = pg_conn.cursor()
        schema    = self.config['postgresql'].get('schema', 'public')
        col_names = ', '.join(f'"{c.name.lower()}"' for c in self.columns)
        copy_sql  = f'COPY "{schema}"."{DEST_TABLE}" ({col_names}) FROM STDIN'
        col_count = len(self.columns)
        max_ret   = self.config['migration'].get('max_retries', 3)

        for attempt in range(max_ret):
            try:
                buf = io.StringIO()
                for row in rows:
                    buf.write(_copy_row_str(self._convert_row(row), col_count))
                buf.seek(0)
                cur.copy_expert(copy_sql, buf)
                pg_conn.commit()
                return
            except Exception as e:
                pg_conn.rollback()
                if attempt < max_ret - 1:
                    self.log.warning(
                        f'Batch {batch_num}: tentativa {attempt+1} falhou ({e}). Subdividindo...')
                    mid = len(rows) // 2
                    if mid > 0:
                        errors = []
                        try:
                            self._insert_copy(pg_conn, rows[:mid], batch_num)
                        except Exception as e1:
                            errors.append(('first_half', len(rows[:mid]), e1))
                        try:
                            self._insert_copy(pg_conn, rows[mid:], batch_num)
                        except Exception as e2:
                            errors.append(('second_half', len(rows[mid:]), e2))
                        if errors:
                            total_lost = sum(x[1] for x in errors)
                            self.log.error(
                                f'Batch {batch_num}: {len(errors)} sub-batch(es) falharam, '
                                f'{total_lost} linhas afetadas: {errors}')
                        return
                else:
                    self.progress.rows_failed += len(rows)
                    self.log.error(
                        f'Batch {batch_num}: FALHA após {max_ret} tentativas: {e}')
                    raise

    def _insert_values(self, pg_conn, rows: list, batch_num: int):
        from psycopg2.extras import execute_values
        cur       = pg_conn.cursor()
        schema    = self.config['postgresql'].get('schema', 'public')
        col_names = ', '.join(f'"{c.name.lower()}"' for c in self.columns)
        tmpl      = f'INSERT INTO "{schema}"."{DEST_TABLE}" ({col_names}) VALUES %s'
        max_ret   = self.config['migration'].get('max_retries', 3)

        for attempt in range(max_ret):
            try:
                converted = [self._convert_row(r) for r in rows]
                execute_values(cur, tmpl, converted, page_size=2000)
                pg_conn.commit()
                return
            except Exception as e:
                pg_conn.rollback()
                if attempt < max_ret - 1:
                    mid = len(rows) // 2
                    if mid > 0:
                        errors = []
                        try:
                            self._insert_values(pg_conn, rows[:mid], batch_num)
                        except Exception as e1:
                            errors.append(('first_half', len(rows[:mid]), e1))
                        try:
                            self._insert_values(pg_conn, rows[mid:], batch_num)
                        except Exception as e2:
                            errors.append(('second_half', len(rows[mid:]), e2))
                        if errors:
                            total_lost = sum(x[1] for x in errors)
                            self.log.error(
                                f'Batch {batch_num}: {len(errors)} sub-batch(es) falharam, '
                                f'{total_lost} linhas afetadas: {errors}')
                        return
                else:
                    self.progress.rows_failed += len(rows)
                    raise

    def _insert_batch(self, pg_conn, rows: list, batch_num: int):
        if self.use_insert:
            self._insert_values(pg_conn, rows, batch_num)
        else:
            self._insert_copy(pg_conn, rows, batch_num)

    # ── atualização de progresso ─────────────────────────────

    def _update_progress(self, batch_rows: list, batch_num: int, t0: float):
        n = len(batch_rows)
        self.progress.rows_migrated += n
        self.progress.current_batch  = batch_num

        # Checkpoint PK: último (NU_OPERACAO, NU_DOCUMENTO) do batch
        last = batch_rows[-1]
        self.progress.last_pk_value = [
            last[self._pk_idx_op],
            last[self._pk_idx_doc],
        ]

        elapsed  = time.time() - t0
        migrated = self.progress.rows_migrated
        total    = self.progress.total_rows
        speed    = migrated / elapsed if elapsed > 0 else 0.0
        eta      = (total - migrated) / speed if speed > 0 else 0.0

        self.progress.speed_rows_per_sec = speed
        self.progress.eta_seconds        = eta
        self.progress.elapsed_seconds    = elapsed
        self.progress.updated_at         = datetime.now().isoformat()

        pct    = (migrated / total * 100) if total else 0.0
        filled = int(30 * pct / 100)
        bar    = '█' * filled + '░' * (30 - filled)

        self.log.info(
            f'[{bar}] {pct:5.1f}% | '
            f'{migrated:>10,}/{total:>10,} | '
            f'Lote {batch_num:>5,} | '
            f'{speed:>8,.0f} lin/s | '
            f'ETA {_fmt_dur(eta):>10} | '
            f'{_fmt_dur(elapsed)}')

        self.state.log_batch(batch_num, n, migrated, speed, eta)

    # ── loop principal ───────────────────────────────────────

    def run(self):
        cfg_m      = self.config['migration']
        batch_size = cfg_m.get('batch_size', 5000)
        range_desc = (f'{self.range_start} -> ∞'
                      if self.is_last
                      else f'{self.range_start} -> {self.range_end} (excl.)')

        self.log.info('=' * 64)
        self.log.info(f'  Thread {self.tid} | NU_OPERACAO: {range_desc}')
        self.log.info(f'  Estimativa: ~{self._total_rows_in_range:,} linhas')
        self.log.info('=' * 64)

        # ── Checkpoint ───────────────────────────────────────
        saved      = self.state.load_progress()
        is_restart = bool(
            saved
            and saved.status in ('running', 'paused', 'error')
            and saved.rows_migrated > 0)

        if is_restart:
            total_rows = saved.total_rows or self._total_rows_in_range
            self.log.info(f'  RESTART — {saved.rows_migrated:,} linhas já migradas no range')
        else:
            total_rows = self._total_rows_in_range
            self.state.reset()

        total_batches = max(1, (total_rows + batch_size - 1) // batch_size)

        if is_restart:
            self.progress            = saved
            self.progress.status     = 'running'
            self.progress.total_rows = total_rows
        else:
            self.progress = MigrationProgress(
                source_table=SOURCE_TABLE, dest_table=DEST_TABLE,
                total_rows=total_rows, batch_size=batch_size,
                total_batches=total_batches, pk_columns=PK_COLS,
                status='running',
                started_at=datetime.now().isoformat(),
                worker_id=f't{self.tid}',
                category='parallel_pk')

        self.state.save_progress(self.progress)

        fb_conn = self._fb_conn()
        pg_conn = self._pg_conn()

        try:
            self._optimize_pg(pg_conn)

            fb_cur = fb_conn.cursor()
            fb_cur.arraysize = cfg_m.get('fetch_array_size', 10000)

            select_sql, select_params = self._build_select(saved if is_restart else None)
            self.log.info(f'  Query: {select_sql}')
            self.log.info(f'  Params: {select_params}')
            fb_cur.execute(select_sql, select_params)

            t0        = time.time()
            last_save = t0
            batch_num = self.progress.current_batch
            batch_buf = []

            while not self.shutdown.is_set():
                row = fb_cur.fetchone()

                if row is None:
                    # EOF — flush batch parcial
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
                        self.state.save_progress(self.progress)
                        last_save = time.time()

            # ── Finalização ──────────────────────────────────
            if self.shutdown.is_set():
                self.progress.status     = 'paused'
                self.progress.updated_at = datetime.now().isoformat()
                self.state.save_progress(self.progress)
                self.log.warning(
                    f'  Thread {self.tid}: PAUSADA with {self.progress.rows_migrated:,} rows. '
                    f'Execute novamente para retomar.')
            else:
                elapsed = time.time() - t0
                self.progress.status          = 'completed'
                self.progress.completed_at    = datetime.now().isoformat()
                if elapsed > 0:
                    self.progress.speed_rows_per_sec = self.progress.rows_migrated / elapsed
                self.state.save_progress(self.progress)
                self.log.info(
                    f'  Thread {self.tid}: CONCLUÍDA — '
                    f'{self.progress.rows_migrated:,} linhas | {_fmt_dur(elapsed)}')

        except Exception as e:
            self.exception              = e
            self.progress.status        = 'error'
            self.progress.error_message = str(e)[:200]
            self.progress.updated_at    = datetime.now().isoformat()
            self.state.save_progress(self.progress)
            self.log.error(f'  Thread {self.tid}: ERRO — {e}', exc_info=True)
        finally:
            fb_conn.close()
            self._restore_pg(pg_conn)
            pg_conn.close()


# ═══════════════════════════════════════════════════════════════
#  LOGGER DO ORQUESTRADOR
# ═══════════════════════════════════════════════════════════════

def _setup_main_logger(log_file: str) -> logging.Logger:
    log = logging.getLogger('main')
    log.setLevel(logging.INFO)
    log.propagate = False
    fmt = logging.Formatter(
        '%(asctime)s [%(levelname)-7s] %(name)s: %(message)s', datefmt='%H:%M:%S')
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(fmt)
    log.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    log.addHandler(ch)
    return log


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description=f'Migra {SOURCE_TABLE} -> {DEST_TABLE} em paralelo (Firebird -> PostgreSQL)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Exemplos:
  python migrator_parallel_doc_oper.py --threads 4
  python migrator_parallel_doc_oper.py --threads 4 --reset
  python migrator_parallel_doc_oper.py --threads 4 --dry-run
  python migrator_parallel_doc_oper.py --threads 2 --batch-size 5000
  python migrator_parallel_doc_oper.py --threads 4 --generate-scripts-only

Observabilidade:
  python monitor.py   ->  coluna "{DEST_TABLE}" mostra progresso agregado das N threads

Checkpoint/Resume:
  Se interrompido com Ctrl+C, cada thread salva seu ponto de parada.
  Execute novamente sem --reset para retomar de onde parou.
  Use --reset para descartar checkpoints e reiniciar do zero.

Nota: não mude --threads entre execuções sem usar --reset.
        """)
    ap.add_argument('-c', '--config', default='config.yaml',
                    help='Arquivo de configuração YAML (padrão: config.yaml)')
    ap.add_argument('-t', '--threads', type=int, default=4, metavar='N',
                    help='Número de threads paralelas (padrão: 4)')
    ap.add_argument('--reset', action='store_true',
                    help='Descarta todos os checkpoints e reinicia do zero')
    ap.add_argument('--dry-run', action='store_true',
                    help='Mostra contagens e ranges sem escrever dados')
    ap.add_argument('--master-db', type=str, default=None,
                    help='Caminho para o banco de dados mestre SQLite')
    ap.add_argument('--migration-id', type=int, default=None,
                    help='ID da migração no banco mestre')
    ap.add_argument('--work-dir', type=str, default=None,
                    help='Diretório de trabalho para checkpoints e logs')
    ap.add_argument('--batch-size', type=int, default=None,
                    help='Linhas por batch (sobrescreve config.yaml)')
    ap.add_argument('--use-insert', action='store_true',
                    help='Usa INSERT em vez de COPY (mais lento, mais compatível)')
    ap.add_argument('--generate-scripts-only', action='store_true',
                    help='Apenas gera scripts SQL de constraints, sem migrar')
    args = ap.parse_args()

    # Configuração de diretórios
    if args.work_dir:
        global WORK_DIR, LOG_DIR
        WORK_DIR = Path(args.work_dir)
        LOG_DIR = WORK_DIR / 'logs'
        WORK_DIR.mkdir(exist_ok=True, parents=True)
        LOG_DIR.mkdir(exist_ok=True, parents=True)

    if not Path(args.config).exists():
        print(f'ERRO: {args.config} não encontrado.')
        sys.exit(1)

    log = _setup_main_logger(
        str(LOG_DIR / f'migration_{DEST_TABLE}_parallel.log'))

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
    log.info(f'  MIGRAÇÃO PARALELA: {SOURCE_TABLE} -> {DEST_TABLE}')
    log.info(f'  Threads : {n_threads}')
    log.info(f'  Batch   : {config["migration"].get("batch_size", 5000):,}')
    log.info(f'  Modo    : {"COPY" if not args.use_insert else "INSERT"}')
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

    state_path   = WORK_DIR / f'constraint_state_{DEST_TABLE}.json'
    disable_path = WORK_DIR / f'disable_constraints_{DEST_TABLE}.sql'
    enable_path  = WORK_DIR / f'enable_constraints_{DEST_TABLE}.sql'

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
    #  FASE 1 — Preparação
    # ══════════════════════════════════════════════════════════
    log.info('')
    log.info('━' * 70)
    log.info('  Fase 1 — Preparação e particionamento')
    log.info('━' * 70)

    columns   = discover_columns(config)
    blob_cols = [c.name for c in columns if c.is_blob]
    log.info(f'  Colunas: {len(columns)} | BLOBs: {blob_cols or "nenhum"}')

    total_rows = count_rows(config, log)

    # Caminho dos state DBs individuais das threads
    worker_db_paths = [
        WORK_DIR / f'migration_state_{DEST_TABLE}_t{i}.db'
        for i in range(n_threads)
    ]

    if args.dry_run:
        log.info('')
        log.info(f'  DRY-RUN: {total_rows:,} linhas, {n_threads} thread(s)')
        ranges = compute_pk_ranges(config, n_threads, total_rows, log)
        for i, r in enumerate(ranges):
            bound = f'< {r["end"]}' if not r['is_last'] else '(sem limite)'
            log.info(f'  Thread {i}: {PART_COL} >= {r["start"]} {bound}  '
                     f'(~{r["rows"]:,} linhas)')
        return

    # ── Calcular ranges ──────────────────────────────────────
    log.info(f'  Calculando {n_threads} ranges de {PART_COL}...')
    ranges    = compute_pk_ranges(config, n_threads, total_rows, log)
    n_threads = len(ranges)   # pode ter sido reduzido por falta de ranges distintos

    for i, r in enumerate(ranges):
        bound = f'< {r["end"]}' if not r['is_last'] else '(sem limite)'
        log.info(f'  Thread {i}: {PART_COL} >= {r["start"]} {bound}  '
                 f'(~{r["rows"]:,} linhas)')

    # ── TRUNCATE (somente em fresh start) ────────────────────
    # Verifica se alguma thread tem checkpoint válido -> resume
    any_restart = False
    if not args.reset:
        for db_path in worker_db_paths:
            if not db_path.exists():
                continue
            try:
                conn = sqlite3.connect(str(db_path), timeout=3)
                row  = conn.execute(
                    'SELECT progress_json FROM migration_state WHERE id=1'
                ).fetchone()
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
        master_path = args.master_db if args.master_db else WORK_DIR / f'migration_state_{DEST_TABLE}.db'
        if Path(master_path).exists():
            StateManager(master_path, migration_id=args.migration_id, table_name=SOURCE_TABLE).reset()

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

    # Estado agregado para monitor.py
    master_state_path = args.master_db if args.master_db else WORK_DIR / f'migration_state_{DEST_TABLE}.db'
    master_state      = StateManager(master_state_path, migration_id=args.migration_id, table_name=SOURCE_TABLE)
    if not any_restart:
        master_state.reset()
    master_state.save_progress(MigrationProgress(
        source_table=SOURCE_TABLE, dest_table=DEST_TABLE,
        total_rows=total_rows, status='running',
        started_at=datetime.now().isoformat(),
        pk_columns=PK_COLS,
        category='parallel_pk'))

    # Evento de shutdown compartilhado
    shutdown = threading.Event()

    def _on_signal(signum, _frame):
        log.warning(f'Sinal {signum} recebido — aguardando fim dos batches atuais...')
        shutdown.set()

    signal.signal(signal.SIGINT,  _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    # Cria workers
    workers = [
        WorkerThread(
            thread_id=i, config=config, columns=columns,
            range_start=ranges[i]['start'],
            range_end=ranges[i]['end'],
            is_last=ranges[i]['is_last'],
            total_rows_in_range=ranges[i]['rows'],
            shutdown_event=shutdown,
            use_insert=args.use_insert,
        )
        for i in range(n_threads)
    ]

    # Aggregator (daemon — não bloqueia saída em caso de falha catastrófica)
    aggregator = AggregatorThread(
        worker_db_paths=worker_db_paths,
        master_state=master_state,
        shutdown_event=shutdown,
        total_rows=total_rows,
    )
    aggregator.start()

    t_start = time.time()
    for w in workers:
        w.start()

    # Aguarda todas as threads concluírem
    for w in workers:
        w.join()

    # Para o aggregator e aguarda a agregação final
    shutdown.set()
    aggregator.join(timeout=8)

    # ── Resumo ───────────────────────────────────────────────
    elapsed        = time.time() - t_start
    total_migrated = sum(w.progress.rows_migrated for w in workers)
    total_failed   = sum(w.progress.rows_failed   for w in workers)
    error_workers  = [w for w in workers if w.exception]
    paused_workers = [w for w in workers if w.progress.status == 'paused']

    final_status = ('error'     if error_workers  else
                    'paused'    if paused_workers  else
                    'completed')

    # Atualiza estado agregado final
    master_state.save_progress(MigrationProgress(
        source_table=SOURCE_TABLE, dest_table=DEST_TABLE,
        total_rows=total_rows,
        rows_migrated=total_migrated, rows_failed=total_failed,
        status=final_status,
        phase='loaded' if final_status == 'completed' else final_status,
        elapsed_seconds=elapsed,
        speed_rows_per_sec=(total_migrated / elapsed if elapsed > 0 else 0.0),
        completed_at=(datetime.now().isoformat() if final_status == 'completed' else None),
        updated_at=datetime.now().isoformat(),
        constraints_disabled=True, pk_columns=PK_COLS,
    ))

    log.info('')
    log.info('=' * 70)
    log.info(f'  MIGRAÇÃO PARALELA — '
             f'{"CONCLUÍDA ✓" if final_status == "completed" else final_status.upper()}')
    log.info(f'  Linhas migradas : {total_migrated:>14,}')
    log.info(f'  Linhas com erro : {total_failed:>14,}')
    log.info(f'  Tempo total     : {_fmt_dur(elapsed):>14}')
    if elapsed > 0 and total_migrated:
        log.info(f'  Velocidade total: {total_migrated / elapsed:>14,.0f} lin/s')
    if error_workers:
        log.error(f'  Threads com erro: {[w.tid for w in error_workers]}')
        for w in error_workers:
            log.error(f'    Thread {w.tid}: {w.exception}')
        log.error(f'  Verifique: migration_{DEST_TABLE}_tN.log')
    if paused_workers:
        log.warning(f'  Threads pausadas: {[w.tid for w in paused_workers]}')
        log.warning('  Execute novamente (sem --reset) para retomar.')
    log.info('')
    log.info('  Scripts gerados por este script:')
    log.info(f'    {disable_path.name}   (já executado)')
    log.info(f'    {enable_path.name}    (execute quando TODAS as cargas terminarem)')
    log.info(f'    psql -f {enable_path.name}')
    log.info('=' * 70)

    sys.exit(1 if error_workers else 0)


if __name__ == '__main__':
    main()
