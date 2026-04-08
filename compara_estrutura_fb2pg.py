"""
compara_estrutura_fb2pg.py
===========================
Compara estrutura completa (contagem + PKs + FKs + índices + constraints) 
entre Firebird e PostgreSQL.

Relatório detalhado:
  - Contagem de linhas
  - Primary Keys
  - Foreign Keys
  - Índices
  - Constraints (UNIQUE, CHECK, etc)

Uso:
    python compara_estrutura_fb2pg.py [--config config.yaml] [--schema public]
"""

import argparse
import os
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional

import yaml

# ─── Firebird DLL auto-discovery (Windows) ────────────────────────────────────
import fdb

if os.name == 'nt':
    _fb_paths = [
        os.path.abspath(os.path.join(os.path.dirname(__file__) or '.', 'fbclient.dll')),
        r'C:\Program Files\Firebird\Firebird_3_0\fbclient.dll',
        r'C:\Program Files\Firebird\Firebird_4_0\fbclient.dll',
        r'C:\Program Files\Firebird\Firebird_5_0\fbclient.dll',
        r'C:\Program Files\Firebird\Firebird_2_5\bin\fbclient.dll',
        r'C:\Program Files (x86)\Firebird\Firebird_3_0\fbclient.dll',
        r'C:\Program Files (x86)\Firebird\Firebird_2_5\bin\fbclient.dll',
    ]
    for _p in _fb_paths:
        if os.path.exists(_p):
            try:
                fdb.load_api(_p)
                break
            except Exception:
                pass

import psycopg2

# ─── Charset helpers ──────────────────────────────────────────────────────────
_CONFIG_CHARSET_TO_FB = {
    'iso-8859-1': 'ISO8859_1', 'iso8859-1': 'ISO8859_1',
    'iso_8859-1': 'ISO8859_1', 'latin1':    'ISO8859_1',
    'latin-1':    'ISO8859_1', 'win1252':   'WIN1252',
    'windows-1252': 'WIN1252', 'cp1252':    'WIN1252',
    'utf-8':      'UTF8',      'utf8':      'UTF8',
}

def _fb_charset(raw: str) -> str:
    return _CONFIG_CHARSET_TO_FB.get(raw.lower(), raw.upper())


# ─── Rich (opcional) ──────────────────────────────────────────────────────────
try:
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text
    from rich import box
    from rich.panel import Panel
    HAS_RICH = True
    console = Console()
except ImportError:
    HAS_RICH = False
    console = None


# ─── Conexões ─────────────────────────────────────────────────────────────────

def _fb_connect(cfg: dict):
    c = cfg['firebird']
    return fdb.connect(
        host=c['host'], port=c.get('port', 3050),
        database=c['database'],
        user=c['user'], password=c['password'],
        charset=_fb_charset(c.get('charset', 'WIN1252')),
    )


def _pg_connect(cfg: dict):
    c = cfg['postgresql']
    conn = psycopg2.connect(
        host=c['host'], port=c.get('port', 5432),
        database=c['database'],
        user=c['user'], password=c['password'],
    )
    conn.set_client_encoding('UTF8')
    conn.autocommit = True
    return conn


# ─── Listagem de tabelas ───────────────────────────────────────────────────────

def _fb_tables(conn) -> list[str]:
    """Retorna tabelas de usuário do Firebird (exclui tabelas de sistema)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT TRIM(r.RDB$RELATION_NAME)
        FROM RDB$RELATIONS r
        WHERE r.RDB$SYSTEM_FLAG = 0
          AND r.RDB$VIEW_BLR IS NULL
        ORDER BY r.RDB$RELATION_NAME
    """)
    return [row[0] for row in cur.fetchall()]


def _pg_tables(conn, schema: str) -> list[str]:
    """Retorna tabelas do schema PostgreSQL."""
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """, (schema,))
    return [row[0] for row in cur.fetchall()]


# ─── Contagens ────────────────────────────────────────────────────────────────

def _fb_count(conn, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM "{table}"')
    return cur.fetchone()[0]


def _pg_count(conn, schema: str, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
    return cur.fetchone()[0]


# ─── PRIMARY KEYS ─────────────────────────────────────────────────────────────

def _fb_get_pk(conn, table: str) -> Optional[Set[str]]:
    """Retorna conjunto de colunas da PK da tabela Firebird."""
    cur = conn.cursor()
    cur.execute("""
        SELECT TRIM(sg.RDB$FIELD_NAME)
        FROM RDB$RELATION_CONSTRAINTS rc
        JOIN RDB$INDEX_SEGMENTS sg ON rc.RDB$INDEX_NAME = sg.RDB$INDEX_NAME
        WHERE TRIM(rc.RDB$RELATION_NAME) = ?
          AND rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
        ORDER BY sg.RDB$FIELD_POSITION
    """, (table,))
    cols = [row[0] for row in cur.fetchall()]
    return set(cols) if cols else None


def _pg_get_pk(conn, schema: str, table: str) -> Optional[Set[str]]:
    """Retorna conjunto de colunas da PK da tabela PostgreSQL."""
    cur = conn.cursor()
    cur.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = %s
          AND tc.table_name = %s
          AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
    """, (schema, table))
    cols = [row[0] for row in cur.fetchall()]
    return set(cols) if cols else None


# ─── FOREIGN KEYS ─────────────────────────────────────────────────────────────

def _fb_get_fks(conn, table: str) -> Set[Tuple[str, str, str]]:
    """
    Retorna conjunto de FKs da tabela Firebird.
    Retorna: {(coluna_origem, tabela_destino, coluna_destino), ...}
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            TRIM(sg.RDB$FIELD_NAME) as col_origem,
            TRIM(ref.RDB$CONST_NAME_UQ) as pk_constraint,
            TRIM(rc2.RDB$RELATION_NAME) as tabela_destino,
            TRIM(sg2.RDB$FIELD_NAME) as col_destino
        FROM RDB$RELATION_CONSTRAINTS rc
        JOIN RDB$REF_CONSTRAINTS ref ON rc.RDB$CONSTRAINT_NAME = ref.RDB$CONSTRAINT_NAME
        JOIN RDB$RELATION_CONSTRAINTS rc2 ON ref.RDB$CONST_NAME_UQ = rc2.RDB$CONSTRAINT_NAME
        JOIN RDB$INDEX_SEGMENTS sg ON rc.RDB$INDEX_NAME = sg.RDB$INDEX_NAME
        JOIN RDB$INDEX_SEGMENTS sg2 ON rc2.RDB$INDEX_NAME = sg2.RDB$INDEX_NAME
        WHERE TRIM(rc.RDB$RELATION_NAME) = ?
          AND rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
          AND sg.RDB$FIELD_POSITION = sg2.RDB$FIELD_POSITION
        ORDER BY rc.RDB$CONSTRAINT_NAME, sg.RDB$FIELD_POSITION
    """, (table,))
    
    fks = set()
    for row in cur.fetchall():
        col_origem, pk_constraint, tabela_destino, col_destino = row
        fks.add((col_origem.lower(), tabela_destino.lower(), col_destino.lower()))
    
    return fks


def _pg_get_fks(conn, schema: str, table: str) -> Set[Tuple[str, str, str]]:
    """
    Retorna conjunto de FKs da tabela PostgreSQL.
    Retorna: {(coluna_origem, tabela_destino, coluna_destino), ...}
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            kcu.column_name as col_origem,
            ccu.table_name as tabela_destino,
            ccu.column_name as col_destino
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON tc.constraint_name = ccu.constraint_name
          AND tc.table_schema = ccu.table_schema
        WHERE tc.table_schema = %s
          AND tc.table_name = %s
          AND tc.constraint_type = 'FOREIGN KEY'
        ORDER BY kcu.ordinal_position
    """, (schema, table))
    
    fks = set()
    for row in cur.fetchall():
        col_origem, tabela_destino, col_destino = row
        fks.add((col_origem.lower(), tabela_destino.lower(), col_destino.lower()))
    
    return fks


# ─── ÍNDICES ──────────────────────────────────────────────────────────────────

def _fb_get_indexes(conn, table: str) -> Set[Tuple[str, bool]]:
    """
    Retorna conjunto de índices da tabela Firebird (exceto PKs e FKs).
    Retorna: {(colunas_concatenadas, is_unique), ...}
    """
    cur = conn.cursor()
    # Índices que NÃO são de constraints (PK/FK/UNIQUE via constraint)
    cur.execute("""
        SELECT 
            i.RDB$INDEX_NAME,
            i.RDB$UNIQUE_FLAG,
            TRIM(sg.RDB$FIELD_NAME) as col_name,
            sg.RDB$FIELD_POSITION
        FROM RDB$INDICES i
        JOIN RDB$INDEX_SEGMENTS sg ON i.RDB$INDEX_NAME = sg.RDB$INDEX_NAME
        WHERE TRIM(i.RDB$RELATION_NAME) = ?
          AND i.RDB$SYSTEM_FLAG = 0
          AND NOT EXISTS (
              SELECT 1 FROM RDB$RELATION_CONSTRAINTS rc
              WHERE rc.RDB$INDEX_NAME = i.RDB$INDEX_NAME
          )
        ORDER BY i.RDB$INDEX_NAME, sg.RDB$FIELD_POSITION
    """, (table,))
    
    indexes_dict = defaultdict(list)
    unique_dict = {}
    
    for row in cur.fetchall():
        idx_name, is_unique, col_name, position = row
        indexes_dict[idx_name].append(col_name.lower())
        unique_dict[idx_name] = bool(is_unique)
    
    indexes = set()
    for idx_name, cols in indexes_dict.items():
        cols_str = ','.join(sorted(cols))  # ordenar para comparação
        indexes.add((cols_str, unique_dict[idx_name]))
    
    return indexes


def _pg_get_indexes(conn, schema: str, table: str) -> Set[Tuple[str, bool]]:
    """
    Retorna conjunto de índices da tabela PostgreSQL (exceto PKs e FKs).
    Retorna: {(colunas_concatenadas, is_unique), ...}
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            i.indexname,
            ix.indisunique,
            a.attname
        FROM pg_indexes i
        JOIN pg_class c ON c.relname = i.tablename
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = i.schemaname
        JOIN pg_index ix ON ix.indexrelid = (
            SELECT oid FROM pg_class WHERE relname = i.indexname AND relnamespace = n.oid
        )
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(ix.indkey)
        WHERE i.schemaname = %s
          AND i.tablename = %s
          AND NOT EXISTS (
              SELECT 1 FROM information_schema.table_constraints tc
              WHERE tc.table_schema = i.schemaname
                AND tc.table_name = i.tablename
                AND tc.constraint_name = i.indexname
          )
        ORDER BY i.indexname, array_position(ix.indkey::int[], a.attnum::int)
    """, (schema, table))
    
    indexes_dict = defaultdict(list)
    unique_dict = {}
    
    for row in cur.fetchall():
        idx_name, is_unique, col_name = row
        indexes_dict[idx_name].append(col_name.lower())
        unique_dict[idx_name] = is_unique
    
    indexes = set()
    for idx_name, cols in indexes_dict.items():
        cols_str = ','.join(sorted(cols))
        indexes.add((cols_str, unique_dict[idx_name]))
    
    return indexes


# ─── UNIQUE CONSTRAINTS ───────────────────────────────────────────────────────

def _fb_get_uniques(conn, table: str) -> Set[str]:
    """Retorna conjunto de UNIQUE constraints (colunas concatenadas)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            rc.RDB$CONSTRAINT_NAME,
            TRIM(sg.RDB$FIELD_NAME) as col_name,
            sg.RDB$FIELD_POSITION
        FROM RDB$RELATION_CONSTRAINTS rc
        JOIN RDB$INDEX_SEGMENTS sg ON rc.RDB$INDEX_NAME = sg.RDB$INDEX_NAME
        WHERE TRIM(rc.RDB$RELATION_NAME) = ?
          AND rc.RDB$CONSTRAINT_TYPE = 'UNIQUE'
        ORDER BY rc.RDB$CONSTRAINT_NAME, sg.RDB$FIELD_POSITION
    """, (table,))
    
    uniques_dict = defaultdict(list)
    for row in cur.fetchall():
        const_name, col_name, position = row
        uniques_dict[const_name].append(col_name.lower())
    
    return {','.join(sorted(cols)) for cols in uniques_dict.values()}


def _pg_get_uniques(conn, schema: str, table: str) -> Set[str]:
    """Retorna conjunto de UNIQUE constraints (colunas concatenadas)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            tc.constraint_name,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = %s
          AND tc.table_name = %s
          AND tc.constraint_type = 'UNIQUE'
        ORDER BY tc.constraint_name, kcu.ordinal_position
    """, (schema, table))
    
    uniques_dict = defaultdict(list)
    for row in cur.fetchall():
        const_name, col_name = row
        uniques_dict[const_name].append(col_name.lower())
    
    return {','.join(sorted(cols)) for cols in uniques_dict.values()}


# ─── CHECK CONSTRAINTS ────────────────────────────────────────────────────────

def _fb_get_checks(conn, table: str) -> Set[str]:
    """Retorna conjunto de CHECK constraints via catálogo de constraints."""
    cur = conn.cursor()
    # Usa RDB$RELATION_CONSTRAINTS em vez de RDB$TRIGGERS para evitar confundir
    # triggers de usuário com triggers gerados por CHECK constraints.
    cur.execute("""
        SELECT TRIM(rc.RDB$CONSTRAINT_NAME)
        FROM RDB$RELATION_CONSTRAINTS rc
        WHERE TRIM(rc.RDB$RELATION_NAME) = ?
          AND rc.RDB$CONSTRAINT_TYPE = 'CHECK'
        ORDER BY rc.RDB$CONSTRAINT_NAME
    """, (table,))
    return {row[0].lower() for row in cur.fetchall()}


def _pg_get_checks(conn, schema: str, table: str) -> Set[str]:
    """Retorna conjunto de CHECK constraints."""
    cur = conn.cursor()
    cur.execute("""
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_schema = %s
          AND table_name = %s
          AND constraint_type = 'CHECK'
        ORDER BY constraint_name
    """, (schema, table))
    return {row[0].lower() for row in cur.fetchall()}


# ─── Comparação de Estruturas ─────────────────────────────────────────────────

def _compare_structure(fb_conn, pg_conn, schema: str, table_key: str, 
                       fb_name: str, pg_name: str) -> dict:
    """
    Compara toda a estrutura de uma tabela entre Firebird e PostgreSQL.
    """
    result = {
        'table': table_key,
        'count_ok': True,
        'pk_ok': True,
        'fk_ok': True,
        'idx_ok': True,
        'uniq_ok': True,
        'check_ok': True,
        'issues': []
    }
    
    # ─── Contagem ─────────────────────────────────────────────────────
    try:
        fb_count = _fb_count(fb_conn, fb_name)
        pg_count = _pg_count(pg_conn, schema, pg_name)
        
        if fb_count != pg_count:
            result['count_ok'] = False
            result['issues'].append(f"COUNT: FB={fb_count:,} vs PG={pg_count:,} (diff={pg_count-fb_count:+,})")
    except Exception as e:
        result['count_ok'] = False
        result['issues'].append(f"COUNT: ERRO - {e}")
    
    # ─── Primary Key ──────────────────────────────────────────────────
    try:
        fb_pk = _fb_get_pk(fb_conn, fb_name.upper())
        pg_pk = _pg_get_pk(pg_conn, schema, pg_name)
        
        if fb_pk != pg_pk:
            result['pk_ok'] = False
            fb_cols = ','.join(sorted(fb_pk)) if fb_pk else 'NONE'
            pg_cols = ','.join(sorted(pg_pk)) if pg_pk else 'NONE'
            result['issues'].append(f"PK: FB=[{fb_cols}] vs PG=[{pg_cols}]")
    except Exception as e:
        result['pk_ok'] = False
        result['issues'].append(f"PK: ERRO - {e}")
    
    # ─── Foreign Keys ─────────────────────────────────────────────────
    try:
        fb_fks = _fb_get_fks(fb_conn, fb_name.upper())
        pg_fks = _pg_get_fks(pg_conn, schema, pg_name)
        
        if fb_fks != pg_fks:
            result['fk_ok'] = False
            only_fb = fb_fks - pg_fks
            only_pg = pg_fks - fb_fks
            
            if only_fb:
                result['issues'].append(f"FK só no FB: {only_fb}")
            if only_pg:
                result['issues'].append(f"FK só no PG: {only_pg}")
    except Exception as e:
        result['fk_ok'] = False
        result['issues'].append(f"FK: ERRO - {e}")
    
    # ─── Índices ──────────────────────────────────────────────────────
    try:
        fb_idx = _fb_get_indexes(fb_conn, fb_name.upper())
        pg_idx = _pg_get_indexes(pg_conn, schema, pg_name)
        
        if fb_idx != pg_idx:
            result['idx_ok'] = False
            only_fb = fb_idx - pg_idx
            only_pg = pg_idx - fb_idx
            
            if only_fb:
                result['issues'].append(f"IDX só no FB: {only_fb}")
            if only_pg:
                result['issues'].append(f"IDX só no PG: {only_pg}")
    except Exception as e:
        result['idx_ok'] = False
        result['issues'].append(f"IDX: ERRO - {e}")
    
    # ─── UNIQUE Constraints ───────────────────────────────────────────
    try:
        fb_uniq = _fb_get_uniques(fb_conn, fb_name.upper())
        pg_uniq = _pg_get_uniques(pg_conn, schema, pg_name)
        
        if fb_uniq != pg_uniq:
            result['uniq_ok'] = False
            only_fb = fb_uniq - pg_uniq
            only_pg = pg_uniq - fb_uniq
            
            if only_fb:
                result['issues'].append(f"UNIQUE só no FB: {only_fb}")
            if only_pg:
                result['issues'].append(f"UNIQUE só no PG: {only_pg}")
    except Exception as e:
        result['uniq_ok'] = False
        result['issues'].append(f"UNIQUE: ERRO - {e}")

    # ─── CHECK Constraints ────────────────────────────────────────────
    try:
        fb_chk = _fb_get_checks(fb_conn, fb_name.upper())
        pg_chk = _pg_get_checks(pg_conn, schema, pg_name)

        if fb_chk != pg_chk:
            result['check_ok'] = False
            only_fb_chk = fb_chk - pg_chk
            only_pg_chk = pg_chk - fb_chk
            if only_fb_chk:
                result['issues'].append(f"CHECK só no FB: {only_fb_chk}")
            if only_pg_chk:
                result['issues'].append(f"CHECK só no PG: {only_pg_chk}")
    except Exception as e:
        result['check_ok'] = False
        result['issues'].append(f"CHECK: ERRO - {e}")

    return result


# ─── Relatórios ───────────────────────────────────────────────────────────────

def _print_summary_plain(results: List[dict], only_fb: List[str], only_pg: List[str]):
    """Relatório resumido em texto plano."""
    print()
    print('=' * 100)
    print('  RESUMO DA COMPARACAO DE ESTRUTURAS')
    print('=' * 100)
    
    total = len(results)
    perfect = sum(1 for r in results if all([
        r['count_ok'], r['pk_ok'], r['fk_ok'], r['idx_ok'], r['uniq_ok'], r['check_ok']
    ]))

    issues = [r for r in results if r['issues']]

    pct_ok  = f'{perfect*100/total:.1f}' if total > 0 else '0.0'
    pct_err = f'{len(issues)*100/total:.1f}' if total > 0 else '0.0'

    print(f'\n  Total de tabelas comparadas: {total}')
    print(f'  Tabelas 100% OK: {perfect} ({pct_ok}%)')
    print(f'  Tabelas com diferenças: {len(issues)} ({pct_err}%)')
    
    if only_fb:
        print(f'\n  Tabelas só no FIREBIRD: {len(only_fb)}')
        for t in only_fb[:10]:
            print(f'    - {t}')
        if len(only_fb) > 10:
            print(f'    ... e mais {len(only_fb)-10}')
    
    if only_pg:
        print(f'\n  Tabelas só no POSTGRESQL: {len(only_pg)}')
        for t in only_pg[:10]:
            print(f'    - {t}')
        if len(only_pg) > 10:
            print(f'    ... e mais {len(only_pg)-10}')
    
    # Detalhamento de problemas
    if issues:
        print(f'\n  DETALHAMENTO DE DIFERENCAS:')
        print('  ' + '-' * 96)
        
        for r in issues:
            status_icons = []
            if not r['count_ok']:  status_icons.append('COUNT')
            if not r['pk_ok']:     status_icons.append('PK')
            if not r['fk_ok']:     status_icons.append('FK')
            if not r['idx_ok']:    status_icons.append('IDX')
            if not r['uniq_ok']:   status_icons.append('UNIQ')
            if not r['check_ok']:  status_icons.append('CHECK')
            
            print(f'\n  [{", ".join(status_icons)}] {r["table"]}')
            for issue in r['issues']:
                print(f'      {issue}')
    
    print()


def _print_summary_rich(results: List[dict], only_fb: List[str], only_pg: List[str]):
    """Relatório resumido com Rich."""
    total = len(results)
    perfect = sum(1 for r in results if all([
        r['count_ok'], r['pk_ok'], r['fk_ok'], r['idx_ok'], r['uniq_ok'], r['check_ok']
    ]))

    issues = [r for r in results if r['issues']]

    pct_ok  = f'{perfect*100/total:.1f}' if total > 0 else '0.0'
    pct_err = f'{len(issues)*100/total:.1f}' if total > 0 else '0.0'

    # ─── Tabela resumo ────────────────────────────────────────────────
    tbl = Table(
        title='Resumo da Comparacao de Estruturas',
        box=box.DOUBLE,
        header_style='bold cyan',
    )
    tbl.add_column('Métrica', style='bold')
    tbl.add_column('Valor', justify='right')

    tbl.add_row('Total de tabelas', f'{total:,}')
    tbl.add_row('Tabelas 100% OK', f'[green]{perfect:,} ({pct_ok}%)[/]')
    tbl.add_row('Tabelas com diferenças', f'[red]{len(issues):,} ({pct_err}%)[/]')
    tbl.add_row('Só no Firebird', f'[yellow]{len(only_fb):,}[/]')
    tbl.add_row('Só no PostgreSQL', f'[yellow]{len(only_pg):,}[/]')
    
    console.print()
    console.print(tbl)
    
    # ─── Detalhamento de problemas ───────────────────────────────────
    if issues:
        console.print()
        console.print(Panel.fit(
            f'[bold red]{len(issues)} TABELAS COM DIFERENCAS[/]',
            border_style='red'
        ))
        
        for r in issues[:50]:  # Limitar a 50 para não poluir
            status = []
            if not r['count_ok']:  status.append('[red]COUNT[/]')
            if not r['pk_ok']:     status.append('[yellow]PK[/]')
            if not r['fk_ok']:     status.append('[yellow]FK[/]')
            if not r['idx_ok']:    status.append('[yellow]IDX[/]')
            if not r['uniq_ok']:   status.append('[yellow]UNIQ[/]')
            if not r['check_ok']:  status.append('[yellow]CHECK[/]')
            
            console.print(f'\n[bold]{r["table"]}[/] - {" ".join(status)}')
            for issue in r['issues']:
                console.print(f'  [dim]• {issue}[/]')
        
        if len(issues) > 50:
            console.print(f'\n[dim]... e mais {len(issues)-50} tabelas com diferenças[/]')
    
    console.print()


# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Compara estrutura completa (count + PKs + FKs + índices + constraints) Firebird vs PostgreSQL'
    )
    parser.add_argument('--config', default='config.yaml', help='Caminho do config.yaml')
    parser.add_argument('--schema', default=None, help='Schema PostgreSQL (default: lido do config)')
    parser.add_argument('--verbose', action='store_true', help='Mostrar progresso detalhado')
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        sys.exit(f'Erro: config não encontrado: {config_path}')

    with open(config_path, encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    schema = args.schema or cfg.get('postgresql', {}).get('schema', 'public')

    print('Conectando ao Firebird...')
    fb_conn = _fb_connect(cfg)
    print('Conectando ao PostgreSQL...')
    pg_conn = _pg_connect(cfg)

    print('Listando tabelas...')
    fb_tables_raw = _fb_tables(fb_conn)
    pg_tables_raw = _pg_tables(pg_conn, schema)

    # Normalizar para lowercase para comparação cruzada
    fb_map = {t.lower(): t for t in fb_tables_raw}
    pg_map = {t.lower(): t for t in pg_tables_raw}

    common_keys = sorted(set(fb_map) & set(pg_map))
    only_fb = sorted(set(fb_map) - set(pg_map))
    only_pg = sorted(set(pg_map) - set(fb_map))

    total = len(common_keys)
    print(f'\nComparando estrutura de {total} tabelas em comum...\n')

    results = []
    
    for i, key in enumerate(common_keys, 1):
        fb_name = fb_map[key]
        pg_name = pg_map[key]
        
        if args.verbose or i % 50 == 0:
            print(f'  [{i:>4}/{total}] {key}')
        
        result = _compare_structure(fb_conn, pg_conn, schema, key, fb_name, pg_name)
        results.append(result)

    fb_conn.close()
    pg_conn.close()

    print('\nGerando relatório...')
    
    if HAS_RICH:
        _print_summary_rich(results, only_fb, only_pg)
    else:
        _print_summary_plain(results, only_fb, only_pg)

    # Código de saída: 0 = tudo ok, 1 = há diferenças
    has_issues = any(r['issues'] for r in results) or only_fb or only_pg
    sys.exit(1 if has_issues else 0)


if __name__ == '__main__':
    main()