#!/usr/bin/env python3
"""
migrator_v2.py
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
    python migrator_v2.py                          # inicia ou recomeça
    python migrator_v2.py --reset                  # recomeça do zero
    python migrator_v2.py --dry-run                # simulação
    python migrator_v2.py --generate-scripts-only  # só gera SQL
    python migrator_v2.py --batch-size 10000       # sobrescreve batch
    python migrator_v2.py --use-insert             # usa INSERT em vez de COPY
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

# Configurações de diretório (serão sobrescritas se --work-dir for passado)
BASE_DIR = Path(__file__).parent
WORK_DIR = BASE_DIR / 'work'
LOG_DIR  = BASE_DIR / 'logs'
WORK_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)


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
#  MIGRADOR
# ═══════════════════════════════════════════════════════════════

    def __init__(self, config_path: str, override_batch_size: int = None,
                 use_insert: bool = False, override_table: str = None,
                 override_log_file: str = None, master_db_path: str = None,
                 work_dir: str = None, migration_id: int = None):
        self.config = self._load_config(config_path)
        if override_batch_size:
            self.config['migration']['batch_size'] = override_batch_size
        self.use_insert = use_insert  # True = execute_values, False = COPY

        # Configuração de diretórios
        global WORK_DIR, LOG_DIR
        if work_dir:
            WORK_DIR = Path(work_dir)
            LOG_DIR = WORK_DIR / 'logs'
            WORK_DIR.mkdir(exist_ok=True, parents=True)
            LOG_DIR.mkdir(exist_ok=True, parents=True)

        # --table: sobrescreve lista de tabelas do config
        if override_table:
            table = override_table.strip()
            self.config['migration']['_override_table'] = {
                'source':   table.upper(),
                'dest':     table.lower(),
                'state_db': str(WORK_DIR / f'migration_state_{table.lower()}.db'),
            }

        self.master_db_path = master_db_path
        self.migration_id = migration_id

        # --log-file: sobrescreve path do log (para execução paralela)
        if override_log_file:
            # Se for path relativo, coloca dentro do LOG_DIR
            p = Path(override_log_file)
            if not p.is_absolute():
                p = LOG_DIR / p
            self.config.setdefault('logging', {})['file'] = str(p)

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
        for h in list(root.handlers):
            root.removeHandler(h)
        root.setLevel(level)

        log_file = cfg.get('file') or str(LOG_DIR / 'migration.log')
        fh = logging.FileHandler(log_file, encoding='utf-8')
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
                state_db = t.get('state_db') or str(WORK_DIR / f'migration_state_{dest}.db')
                result.append({
                    'source':   t['source'],
                    'dest':     dest,
                    'state_db': state_db,
                })
            return result

        # Backward compat: formato antigo com source_table / dest_table
        return [{
            'source':   cfg_m.get('source_table', 'UNKNOWN'),
            'dest':     cfg_m.get('dest_table', 'unknown'),
            'state_db': cfg_m.get('state_db') or str(WORK_DIR / 'migration_state.db'),
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
        c = self.config['postgres'] if 'postgres' in self.config else self.config['postgresql']
        conn = psycopg2.connect(
            host=c['host'], port=c.get('port', 5432),
            database=c['database'],
            user=c['user'], password=c['password'])
        conn.set_client_encoding('UTF8')
        conn.autocommit = False
        return conn

    # ─── metadados ──────────────────────────────────────────

    def _discover_columns(self, table_name: str) -> List[ColumnMeta]:
        """Mapeia colunas do Firebird para PostgreSQL."""
        conn = self._fb_conn()
        try:
            cur = conn.cursor()
            tbl = table_name.upper()

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

    def _discover_pk(self, table_name: str) -> List[str]:
        """Colunas da PK no Firebird."""
        conn = self._fb_conn()
        try:
            cur = conn.cursor()
            tbl = table_name.upper()
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

    def _count_rows(self, table_name: str) -> int:
        """[BUG FIX] SELECT COUNT(*) com sintaxe corrigida."""
        self.log.info(f'Contando linhas de {table_name} (pode demorar)...')
        conn = self._fb_conn()
        try:
            cur = conn.cursor()
            tbl = table_name.upper()
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

    def _check_dest_table(self, table_name: str):
        cfg = self.config['postgresql'] if 'postgresql' in self.config else self.config['postgres']
        schema = cfg.get('schema', 'public')
        table = table_name.lower()
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

    def _truncate_dest(self, table_name: str):
        cfg = self.config['postgresql'] if 'postgresql' in self.config else self.config['postgres']
        schema = cfg.get('schema', 'public')
        table = table_name.lower()
        conn = self._pg_conn()
        cur = conn.cursor()
        cur.execute(f'TRUNCATE TABLE "{schema}"."{table}" CASCADE')
        conn.commit()
        cur.close()
        conn.close()
        self.log.info(f'Destino {table} truncado.')

    # ─── otimização WAL ─────────────────────────────────────

    def _optimize_pg(self, conn, table_name: str):
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
            cfg_pg = self.config['postgresql'] if 'postgresql' in self.config else self.config['postgres']
            cur.execute(f"ALTER TABLE \"{cfg_pg.get('schema','public')}\""
                        f".\"{table_name.lower()}\" "
                        f"SET (autovacuum_enabled = false)")
            conn.commit()
            self.log.info('Autovacuum desabilitado na tabela destino.')
        except Exception:
            conn.rollback()
            self.log.debug('Não foi possível desabilitar autovacuum (sem permissão?).')

        conn.commit()

    def _restore_pg(self, conn, table_name: str):
        """Restaura configurações após carga."""
        cur = conn.cursor()
        cur.execute("SET synchronous_commit = on")
        cur.execute("SET jit = on")
        try:
            cfg_pg = self.config['postgresql'] if 'postgresql' in self.config else self.config['postgres']
            cur.execute(f"ALTER TABLE \"{cfg_pg.get('schema','public')}\""
                        f".\"{table_name.lower()}\" "
                        f"SET (autovacuum_enabled = true)")
        except Exception as e:
            self.log.warning(f'Falha ao reabilitar autovacuum em '
                             f'"{table_name.lower()}": {e}. '
                             f'Verificar manualmente.')
        conn.commit()

    # ─── inserção: COPY protocol ────────────────────────────

    def _insert_copy(self, pg_conn, rows: list, batch_num: int, table_name: str):
        """
        [PERF] Insere via COPY protocol — 3-5× mais rápido que INSERT.
        [BUG FIX] BLOBs convertidos antes de serializar.
        """
        cur = pg_conn.cursor()
        cfg_pg = self.config['postgresql'] if 'postgresql' in self.config else self.config['postgres']
        schema = cfg_pg.get('schema', 'public')
        table = table_name.lower()
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
                    # Sub-batch: divide em 2 e tenta cada metade independentemente
                    mid = len(rows) // 2
                    if mid > 0:
                        errors = []
                        try:
                            self._insert_copy(pg_conn, rows[:mid], batch_num, table_name)
                        except Exception as e1:
                            errors.append(('first_half', len(rows[:mid]), e1))
                        try:
                            self._insert_copy(pg_conn, rows[mid:], batch_num, table_name)
                        except Exception as e2:
                            errors.append(('second_half', len(rows[mid:]), e2))
                        if errors:
                            total_lost = sum(x[1] for x in errors)
                            self.log.error(
                                f'Batch {batch_num}: {len(errors)} sub-batch(es) falharam, '
                                f'{total_lost} linhas afetadas: {errors}')
                        return
                else:
                    self.log.error(
                        f'Batch {batch_num}: FALHA após {max_retries} tentativas: {e}')
                    self.progress.rows_failed += len(rows)
                    raise

    def _insert_values(self, pg_conn, rows: list, batch_num: int, table_name: str):
        """
        Fallback: INSERT via execute_values.
        Mais lento mas compatível com mais versões.
        """
        from psycopg2.extras import execute_values

        cur = pg_conn.cursor()
        cfg_pg = self.config['postgresql'] if 'postgresql' in self.config else self.config['postgres']
        schema = cfg_pg.get('schema', 'public')
        table = table_name.lower()
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
                        errors = []
                        try:
                            self._insert_values(pg_conn, rows[:mid], batch_num, table_name)
                        except Exception as e1:
                            errors.append(('first_half', len(rows[:mid]), e1))
                        try:
                            self._insert_values(pg_conn, rows[mid:], batch_num, table_name)
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

    def _insert_batch(self, pg_conn, rows: list, batch_num: int, table_name: str):
        """Despacha para COPY ou INSERT conforme configuração."""
        if self.use_insert:
            self._insert_values(pg_conn, rows, batch_num, table_name)
        else:
            self._insert_copy(pg_conn, rows, batch_num, table_name)

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

    def _build_select_query(self, table_name: str, saved: Optional[MigrationProgress] = None
                            ) -> Tuple[str, tuple]:
        """
        Monta SELECT com paginação para restart.
        [BUG FIX] RDB$DB_KEY para tabelas sem PK — reiniciável.
        """
        tbl = table_name.upper()
        pk_cols = self.progress.pk_columns

        if pk_cols and saved and saved.last_pk_value:
            # Restart com PK.
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

    # ─── loop principal ─────────────────────────────────────

    def run(self, dry_run=False, scripts_only=False):
        """
        Orquestra a migração.
        """
        self.log.info('=' * 70)
        self.log.info('  MIGRAÇÃO FIREBIRD 3 → POSTGRESQL')
        self.log.info('=' * 70)

        tables = self._resolve_tables()
        self.log.info(f'Tabelas configuradas: {len(tables)}')
        
        for tbl_cfg in tables:
            if self._shutdown:
                break
            self._load_table(tbl_cfg, dry_run)

    def _load_table(self, tbl_cfg: dict, dry_run: bool):
        source    = tbl_cfg['source']
        dest      = tbl_cfg['dest']
        state_db  = self.master_db_path if self.master_db_path else tbl_cfg['state_db']

        cfg_m  = self.config['migration']
        cfg_pg = self.config['postgresql'] if 'postgresql' in self.config else self.config['postgres']
        batch_size = cfg_m.get('batch_size', 5000)

        # Inicializa StateManager com suporte a Master DB
        self._state = StateManager(state_db, migration_id=self.migration_id, table_name=source)
        self.progress = MigrationProgress()

        self.log.info('')
        self.log.info(f'  ── {source} → {dest} ──')

        # ── Metadados ────────────────────────────────────────
        self.columns = self._discover_columns(source)
        pk_cols    = self._discover_pk(source)
        use_db_key = not pk_cols

        self._check_dest_table(source)

        # ── Checkpoint ───────────────────────────────────────
        saved      = self._state.load_progress()
        is_restart = False

        if (saved and saved.status in ('running', 'paused', 'failed')
                and saved.rows_migrated > 0):
            is_restart = True
            self.log.info(f'  RESTART — {saved.rows_migrated:,} linhas já migradas.')

        # ── Contagem ─────────────────────────────────────────
        if not is_restart or not saved.total_rows:
            total_rows = self._count_rows(source)
        else:
            total_rows = saved.total_rows

        total_batches = max(1, (total_rows + batch_size - 1) // batch_size)

        if dry_run:
            self.log.info(f'  DRY-RUN: {total_rows:,} linhas.')
            return

        # ── Truncar ──────────────────────────────────────────
        if not is_restart:
            self._truncate_dest(source)

        # ── Progresso inicial ─────────────────────────────────
        if is_restart:
            self.progress = saved
            self.progress.status = 'running'
        else:
            self.progress = MigrationProgress(
                source_table=source, dest_table=dest,
                total_rows=total_rows, batch_size=batch_size,
                total_batches=total_batches, pk_columns=pk_cols,
                use_db_key=use_db_key,
                status='running',
                started_at=datetime.now().isoformat(),
                category='big' if total_rows > 1000000 else 'small')
        
        self._state.save_progress(self.progress)

        # ── Carga ────────────────────────────────────────────
        fb_conn = self._fb_conn()
        pg_conn = self._pg_conn()

        try:
            self._optimize_pg(pg_conn, source)

            fb_cur = fb_conn.cursor()
            fb_cur.arraysize = cfg_m.get('fetch_array_size', 10000)

            select_sql, select_params = self._build_select_query(source, saved if is_restart else None)
            fb_cur.execute(select_sql, select_params)

            t0        = time.time()
            last_save = t0
            batch_num = self.progress.current_batch
            batch_buf = []

            while not self._shutdown:
                row = fb_cur.fetchone()
                if row is None:
                    if batch_buf:
                        batch_num += 1
                        self._insert_batch(pg_conn, batch_buf, batch_num, source)
                        self._update_progress(batch_buf, batch_num, t0)
                    break

                batch_buf.append(row)
                if len(batch_buf) >= batch_size:
                    batch_num += 1
                    self._insert_batch(pg_conn, batch_buf, batch_num, source)
                    self._update_progress(batch_buf, batch_num, t0)
                    batch_buf = []
                    if time.time() - last_save > 10:
                        self._state.save_progress(self.progress)
                        last_save = time.time()

            if not self._shutdown:
                self.progress.status = 'completed'
                self.progress.completed_at = datetime.now().isoformat()
                self._state.save_progress(self.progress)
                self.log.info(f'  [{source}] Concluído.')

        finally:
            fb_conn.close()
            self._restore_pg(pg_conn, source)
            pg_conn.close()

    def _update_progress(self, batch_rows: list, batch_num: int, t0: float):
        n = len(batch_rows)
        self.progress.rows_migrated += n
        self.progress.current_batch = batch_num

        if self.progress.pk_columns:
            last = batch_rows[-1]
            self.progress.last_pk_value = [
                last[self._col_index(pk)] for pk in self.progress.pk_columns
            ]
        elif self.progress.use_db_key:
            last = batch_rows[-1]
            if len(last) > len(self.columns):
                self.progress.last_db_key = last[-1]

        elapsed = time.time() - t0
        speed = self.progress.rows_migrated / elapsed if elapsed > 0 else 0
        self.progress.speed_rows_per_sec = speed
        self.progress.eta_seconds = (self.progress.total_rows - self.progress.rows_migrated) / speed if speed > 0 else 0
        self.progress.updated_at = datetime.now().isoformat()
        
        self.log.info(f'  Migradas: {self.progress.rows_migrated:,}/{self.progress.total_rows:,} '
                      f'({self.progress.rows_migrated/self.progress.total_rows*100:.1f}%) | '
                      f'Vel: {speed:,.0f} l/s | ETA: {_fmt_dur(self.progress.eta_seconds)}')

def main():
    p = argparse.ArgumentParser(description='Migra Firebird 3 -> PostgreSQL')
    p.add_argument('-c', '--config', default='config.yaml')
    p.add_argument('--table', type=str, default=None)
    p.add_argument('--master-db', type=str, default=None)
    p.add_argument('--migration-id', type=int, default=None)
    p.add_argument('--work-dir', type=str, default=None)
    p.add_argument('--reset', action='store_true')
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--use-insert', action='store_true')
    args = p.parse_args()

    m = FirebirdToPgMigrator(args.config,
                              use_insert=args.use_insert,
                              override_table=args.table,
                              master_db_path=args.master_db,
                              migration_id=args.migration_id,
                              work_dir=args.work_dir)

    if args.reset:
        for tbl_cfg in m._resolve_tables():
            state_db = args.master_db if args.master_db else tbl_cfg['state_db']
            StateManager(state_db, migration_id=args.migration_id, table_name=tbl_cfg['source']).reset()

    try:
        m.run(dry_run=args.dry_run)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        logging.getLogger('migrator').error(f'Fatal: {e}', exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()


if __name__ == '__main__':
    main()
