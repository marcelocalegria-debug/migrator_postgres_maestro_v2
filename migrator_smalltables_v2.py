#!/usr/bin/env python3
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

# Configurações de diretório
BASE_DIR = Path(__file__).parent
WORK_DIR = BASE_DIR / 'work'
LOG_DIR  = BASE_DIR / 'logs'
WORK_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# ═══════════════════════════════════════════════════════════════
#  MAPEAMENTO DE TIPOS FIREBIRD -> POSTGRESQL
# ═══════════════════════════════════════════════════════════════

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

def map_fb_to_pg(type_code: int, subtype: int, length: int,
                 precision: int, scale: int) -> Tuple[str, bool]:
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

_FB_CHARSET_TO_PYTHON: dict = {
    'WIN1252': 'cp1252', 'UTF8': 'utf-8', 'NONE': 'latin-1',
}
_CONFIG_CHARSET_TO_FB: dict = {
    'iso-8859-1': 'ISO8859_1', 'iso8859-1': 'ISO8859_1', 'iso_8859-1': 'ISO8859_1',
    'latin1': 'ISO8859_1', 'latin-1': 'ISO8859_1',
    'win1252': 'WIN1252', 'windows-1252': 'WIN1252', 'cp1252': 'WIN1252',
    'utf-8': 'UTF8', 'utf8': 'UTF8',
}

def _fb_charset_for_connect(raw: str) -> str:
    return _CONFIG_CHARSET_TO_FB.get(raw.lower(), raw.upper())

def _fb_charset_to_python(fb_charset: str) -> str:
    return _FB_CHARSET_TO_PYTHON.get(fb_charset.upper(), 'latin-1')

def _convert_blob(val, blob_subtype: int, charset: str) -> Optional[bytes]:
    if val is None: return None
    if hasattr(val, 'read'):
        blob_obj = val
        try:
            val = blob_obj.read()
        finally:
            if hasattr(blob_obj, 'close'):
                blob_obj.close()
    elif isinstance(val, memoryview): val = bytes(val)
    if blob_subtype == 1:
        enc = _fb_charset_to_python(charset)
        if isinstance(val, bytes):
            try: text = val.decode(enc, errors='replace')
            except: text = val.decode('latin-1', errors='replace')
        else: text = str(val)
        return text.replace('\x00', '')
    return bytes(val)

def _copy_escape(val) -> str:
    if val is None: return '\\N'
    if isinstance(val, bytes): return '\\\\x' + val.hex()
    if isinstance(val, bool): return 't' if val else 'f'
    if isinstance(val, (int, float)): return str(val)
    if hasattr(val, 'isoformat'): return val.isoformat()
    s = str(val)
    # Sanitize for PostgreSQL LATIN1 databases strictly rejecting WIN1252 chars
    s = s.encode('latin-1', errors='replace').decode('latin-1')
    if '\x00' in s: s = s.replace('\x00', '')
    return (s.replace('\\', '\\\\').replace('\t', '\\t')
             .replace('\n', '\\n').replace('\r', '\\r'))

def _copy_row_str(row: tuple, col_count: int) -> str:
    parts = [_copy_escape(row[i]) if i < len(row) else '\\N' for i in range(col_count)]
    return '\t'.join(parts) + '\n'

# ═══════════════════════════════════════════════════════════════
#  WORKER FUNCTION
# ═══════════════════════════════════════════════════════════════

def _worker_migrate_table(args: tuple) -> dict:
    config_path, source_table, dest_table, master_db, migration_id, work_dir = args
    t0 = time.time()
    try:
        log_file = f'migration_{dest_table}.log'
        migrator = FirebirdToPgMigrator(
            config_path, override_log_file=log_file,
            master_db_path=master_db, migration_id=migration_id, work_dir=work_dir
        )
        migrator._load_table({'source': source_table, 'dest': dest_table}, dry_run=False)
        rows = migrator.progress.rows_migrated if migrator.progress else 0
        return {'table': source_table, 'status': 'completed', 'rows_migrated': rows, 'elapsed_sec': time.time() - t0}
    except Exception as e:
        return {'table': source_table, 'status': 'failed', 'error': str(e), 'elapsed_sec': time.time() - t0}

# ═══════════════════════════════════════════════════════════════
#  MIGRADOR
# ═══════════════════════════════════════════════════════════════

class FirebirdToPgMigrator:
    def __init__(self, config_path: str, override_batch_size: int = None,
                 use_insert: bool = False, override_table: str = None,
                 override_log_file: str = None, master_db_path: str = None,
                 work_dir: str = None, migration_id: int = None):
        self._config_path = str(Path(config_path).resolve())
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        self.use_insert = use_insert
        
        global WORK_DIR, LOG_DIR
        if work_dir:
            WORK_DIR = Path(work_dir)
            LOG_DIR = WORK_DIR / 'logs'
            WORK_DIR.mkdir(exist_ok=True, parents=True)
            LOG_DIR.mkdir(exist_ok=True, parents=True)

        if override_table:
            self.config['migration']['_override_table'] = {
                'source': override_table.upper(), 'dest': override_table.lower()
            }

        self.master_db_path = master_db_path
        self.migration_id = migration_id

        if override_log_file:
            p = Path(override_log_file)
            if not p.is_absolute(): p = LOG_DIR / p
            self.config.setdefault('logging', {})['file'] = str(p)

        self._setup_logging()
        self.log = logging.getLogger('migrator')
        self.progress = MigrationProgress()
        self._shutdown = False
        signal.signal(signal.SIGINT, lambda s,f: setattr(self, '_shutdown', True))

    def _setup_logging(self):
        cfg = self.config.get('logging', {})
        level = getattr(logging, cfg.get('level', 'INFO'), logging.INFO)
        fmt = logging.Formatter('%(asctime)s [%(levelname)-7s] %(name)s: %(message)s', '%H:%M:%S')
        root = logging.getLogger()
        for h in list(root.handlers): root.removeHandler(h)
        root.setLevel(level)
        fh = logging.FileHandler(cfg.get('file') or str(LOG_DIR / 'migration.log'), encoding='utf-8')
        fh.setFormatter(fmt)
        root.addHandler(fh)
        if cfg.get('console', True):
            ch = logging.StreamHandler(sys.stdout); ch.setFormatter(fmt); root.addHandler(ch)

    def _fb_conn(self):
        c = self.config['firebird']
        return fdb.connect(host=c['host'], port=c.get('port', 3050), database=c['database'],
                           user=c['user'], password=c['password'],
                           charset=_fb_charset_for_connect(c.get('charset', 'WIN1252')))

    def _pg_conn(self):
        c = self.config.get('postgres') or self.config.get('postgresql')
        conn = psycopg2.connect(host=c['host'], port=c.get('port', 5432), database=c['database'],
                                user=c['user'], password=c['password'])
        conn.set_client_encoding('UTF8'); conn.autocommit = False
        return conn

    def _truncate_dest(self, table_name: str):
        cfg = self.config.get('postgres') or self.config.get('postgresql')
        schema = cfg.get('schema', 'public')
        table = table_name.lower()
        conn = self._pg_conn()
        cur = conn.cursor()
        try:
            # [SAFETY] Removido CASCADE para evitar limpeza acidental de tabelas relacionadas.
            # Se houver erro de FK, o usuário deve rodar o pg_constraints.py primeiro.
            cur.execute(f'TRUNCATE TABLE "{schema}"."{table}"')
            conn.commit()
            self.log.info(f'  [{table_name}] Destino truncado.')
        except Exception as e:
            conn.rollback()
            self.log.warning(f'  [{table_name}] Falha ao truncar (pode haver FKs ativas): {e}')
        finally:
            cur.close(); conn.close()

    def _discover_columns(self, table_name: str) -> List[ColumnMeta]:
        conn = self._fb_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT rf.RDB$FIELD_NAME, f.RDB$FIELD_TYPE, COALESCE(f.RDB$FIELD_SUB_TYPE, 0),
                       COALESCE(f.RDB$FIELD_LENGTH, 0), COALESCE(f.RDB$FIELD_PRECISION, 0),
                       COALESCE(f.RDB$FIELD_SCALE, 0), rf.RDB$NULL_FLAG, rf.RDB$FIELD_POSITION,
                       COALESCE(cs.RDB$CHARACTER_SET_NAME, 'NONE')
                FROM RDB$RELATION_FIELDS rf
                JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = rf.RDB$FIELD_SOURCE
                LEFT JOIN RDB$CHARACTER_SETS cs ON cs.RDB$CHARACTER_SET_ID = f.RDB$CHARACTER_SET_ID
                WHERE rf.RDB$RELATION_NAME = ? ORDER BY rf.RDB$FIELD_POSITION
            """, (table_name.upper(),))
            cols = []
            for r in cur:
                pg_type, is_blob = map_fb_to_pg(r[1], r[2], r[3], r[4], r[5])
                cols.append(ColumnMeta(name=r[0].strip(), fb_type_code=r[1], pg_type=pg_type,
                                       is_blob=is_blob, blob_subtype=r[2], fb_charset=r[8].strip(),
                                       nullable=r[6] is None, position=r[7]))
            return cols
        finally: conn.close()

    def _count_rows(self, table_name: str) -> int:
        conn = self._fb_conn()
        try:
            cur = conn.cursor(); cur.execute(f'SELECT COUNT(*) FROM "{table_name.upper()}"')
            return cur.fetchone()[0]
        finally: conn.close()

    def _load_table(self, tbl_cfg: dict, dry_run: bool):
        source, dest = tbl_cfg['source'], tbl_cfg['dest']
        state_db = self.master_db_path or str(WORK_DIR / f'migration_state_{dest}.db')
        self._state = StateManager(state_db, migration_id=self.migration_id, table_name=source)
        
        saved = self._state.load_progress()
        is_restart = False
        
        if saved and saved.status == 'completed':
            self.log.info(f"  [{source}] Já concluída.")
            return

        if saved and saved.status in ('running', 'paused', 'failed') and saved.rows_migrated > 0:
            is_restart = True
            self.log.info(f"  [{source}] RESTART - {saved.rows_migrated:,} linhas já migradas.")

        total_rows = self._count_rows(source)
        
        if not is_restart:
            self._truncate_dest(source)
            self.progress = MigrationProgress(source_table=source, dest_table=dest, total_rows=total_rows,
                                              status='running', started_at=datetime.now().isoformat(), category='small')
        else:
            self.progress = saved
            self.progress.status = 'running'

        self._state.save_progress(self.progress)

        cfg_m = self.config.get('migration', {})
        batch_size = cfg_m.get('batch_size', 5000)
        fetch_size = cfg_m.get('fetch_array_size', 5000)

        fb_conn, pg_conn = self._fb_conn(), self._pg_conn()
        try:
            self.columns = self._discover_columns(source)
            fb_cur = fb_conn.cursor()
            fb_cur.arraysize = fetch_size
            
            # Montagem da query com suporte a restart (RDB$DB_KEY)
            if is_restart and self.progress.last_db_key:
                sql = f'SELECT T.*, T.RDB$DB_KEY FROM "{source.upper()}" T WHERE T.RDB$DB_KEY > ? ORDER BY T.RDB$DB_KEY'
                params = (self.progress.last_db_key,)
            else:
                sql = f'SELECT T.*, T.RDB$DB_KEY FROM "{source.upper()}" T ORDER BY T.RDB$DB_KEY'
                params = ()
                
            fb_cur.execute(sql, params)
            batch = []
            while not self._shutdown:
                row = fb_cur.fetchone()
                if not row:
                    if batch:
                        self._insert_batch(pg_conn, batch, 0, source)
                        self.progress.rows_migrated += len(batch)
                        # No Firebird, RDB$DB_KEY é a última coluna "escondida" do SELECT *
                        # No smalltables, estamos fazendo SELECT * (que traz colunas + db_key se ordenado por ela)
                        self._state.save_progress(self.progress)
                    break
                batch.append(row)
                if len(batch) >= batch_size:
                    self._insert_batch(pg_conn, batch, 0, source)
                    self.progress.rows_migrated += len(batch)
                    # Atualiza RDB$DB_KEY do último registro do batch para o Maestro
                    if len(row) > len(self.columns):
                        self.progress.last_db_key = row[-1]
                    self._state.save_progress(self.progress)
                    batch = []
            if not self._shutdown:
                self.progress.status = 'completed'; self.progress.completed_at = datetime.now().isoformat()
                self._state.save_progress(self.progress)
        except Exception as e:
            self.progress.status = 'failed'
            self.progress.error_message = str(e)
            self._state.save_progress(self.progress)
            self.log.error(f"  FAIL [{source}] {str(e)}")
            raise
        finally: fb_conn.close(); pg_conn.close()

    def _insert_batch(self, pg_conn, rows: list, batch_num: int, table_name: str):
        cur = pg_conn.cursor()
        cfg_pg = self.config.get('postgres') or self.config.get('postgresql')
        schema = cfg_pg.get('schema', 'public')
        col_names = ', '.join(f'"{c.name.lower()}"' for c in self.columns)
        copy_sql = f'COPY "{schema}"."{table_name.lower()}" ({col_names}) FROM STDIN'
        buf = io.StringIO()
        for r in rows: buf.write(_copy_row_str(self._convert_row(r), len(self.columns)))
        buf.seek(0); cur.copy_expert(copy_sql, buf); pg_conn.commit()

    def _convert_row(self, row: tuple) -> tuple:
        out = []
        for i, col in enumerate(self.columns):
            val = row[i] if i < len(row) else None
            if val is None: out.append(None)
            elif col.is_blob: out.append(_convert_blob(val, col.blob_subtype, col.fb_charset))
            else: out.append(val)
        return tuple(out)

    def run_small_tables(self, n_workers: int = None, only_table: str = None) -> bool:
        cfg_m = self.config.get('migration', {})
        # [NOVO] Prioriza o argumento passado, senão busca do config.yaml, senão padrão 4
        actual_workers = n_workers if n_workers is not None else cfg_m.get('parallel_workers', 4)
        
        exclude = {t.strip().upper() for t in cfg_m.get('exclude_tables', [])}
        
        if only_table:
            pending = [{'source': only_table.upper(), 'dest': only_table.lower()}]
            actual_workers = 1
        else:
            fb_conn = self._fb_conn()
            cur = fb_conn.cursor(); cur.execute("SELECT TRIM(r.RDB$RELATION_NAME) FROM RDB$RELATIONS r WHERE r.RDB$SYSTEM_FLAG=0 AND r.RDB$VIEW_BLR IS NULL")
            pending = [{'source': r[0], 'dest': r[0].lower()} for r in cur.fetchall() if r[0].strip().upper() not in exclude]
            fb_conn.close()

        self.log.info(f"Migrando {len(pending)} tabelas com {actual_workers} workers.")
        args_list = [(self._config_path, t['source'], t['dest'], self.master_db_path, self.migration_id, str(WORK_DIR)) for t in pending]
        completed = 0
        any_failed = False
        with ProcessPoolExecutor(max_workers=actual_workers) as pool:
            futures = {pool.submit(_worker_migrate_table, a): a for a in args_list}
            for f in as_completed(futures):
                res = f.result(); completed += 1
                if res['status'] == 'completed':
                    self.log.info(f"  OK [{res['table']}] {res['rows_migrated']:,} linhas | {completed}/{len(pending)}")
                else:
                    self.log.error(f"  FAIL [{res['table']}] FALHOU: {res['error']}")
                    any_failed = True
        return any_failed

def main():
    p = argparse.ArgumentParser(description='Migrador Paralelo para Tabelas Pequenas')
    p.add_argument('--work-dir', type=str, required=True, help='Diretório da migração (ex: MIGRACAO_0001)')
    p.add_argument('-c', '--config', default=None, help='Caminho do config.yaml (padrão: work-dir/config.yaml)')
    p.add_argument('--small-tables', action='store_true', help='Migra todas as tabelas (exceto exclude)')
    p.add_argument('--table', type=str, help='Migra apenas UMA tabela específica')
    p.add_argument('--master-db', type=str)
    p.add_argument('--migration-id', type=int)
    p.add_argument('--workers', type=int, default=None)
    args = p.parse_args()

    # Define config padrão baseado no work-dir se não for informado
    config_file = args.config if args.config else os.path.join(args.work_dir, 'config.yaml')

    if not os.path.exists(config_file):
        print(f"\n[MIGRATOR SMALL TABLES] Erro: Arquivo de configuração não encontrado: {config_file}")
        sys.exit(1)

    if not args.small_tables and not args.table:
        print("\n[MIGRATOR SMALL TABLES] Erro: Você precisa informar --small-tables ou --table <NOME>.")
        print("\nExemplos de execução ADHOC:")
        print(f"  uv run {sys.argv[0]} --work-dir MIGRACAO_0001 --small-tables")
        print(f"  uv run {sys.argv[0]} --work-dir MIGRACAO_0001 --table NOME_DA_TABELA")
        sys.exit(1)

    m = FirebirdToPgMigrator(config_file, master_db_path=args.master_db, migration_id=args.migration_id, work_dir=args.work_dir)
    
    failed = m.run_small_tables(n_workers=args.workers, only_table=args.table)
    if failed:
        print("\n[ERROR] Algumas tabelas pequenas falharam. Verifique os logs.")
        sys.exit(1)
    else:
        print("\n[OK] Tabelas processadas.")
        sys.exit(0)

if __name__ == '__main__':
    main()
