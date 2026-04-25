"""
compara_cont_fb2pg.py
=====================
Compara a contagem de linhas de todas as tabelas entre Firebird e PostgreSQL.

Relatório:
  - Tabela | COUNT Firebird | COUNT PostgreSQL | Diferença
  - Resumo: tabelas com diferença, só no Firebird, só no PostgreSQL

Uso:
    python compara_cont_fb2pg.py --work-dir MIGRACAO_XXXX [--config config.yaml] [--schema public]
"""

import argparse
import os
import sys
from pathlib import Path

import yaml

# ─── Firebird DLL auto-discovery (Windows) ────────────────────────────────────
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


# ─── Relatório ────────────────────────────────────────────────────────────────

def _print_plain(rows: list[dict], only_fb: list, only_pg: list, diffs: list):
    """Saída sem Rich."""
    print()
    print('=' * 80)
    print('  COMPARACAO DE CONTAGEM: FIREBIRD vs POSTGRESQL')
    print('=' * 80)
    print(f'  {"Tabela":<35} {"Firebird":>12} {"PostgreSQL":>12} {"Diferenca":>12}  Status')
    print('  ' + '-' * 76)
    for r in rows:
        fb   = r['fb'] if r['fb'] is not None else 'N/A'
        pg   = r['pg'] if r['pg'] is not None else 'N/A'
        diff = r['diff'] if r['diff'] is not None else 'N/A'
        fb_s  = f'{fb:>12,}' if isinstance(fb, int) else f'{"N/A":>12}'
        pg_s  = f'{pg:>12,}' if isinstance(pg, int) else f'{"N/A":>12}'
        diff_s = f'{diff:>+12,}' if isinstance(diff, int) else f'{"N/A":>12}'
        print(f'  {r["table"]:<35} {fb_s} {pg_s} {diff_s}  {r["status"]}')

    print()
    print('=' * 80)
    print('  RESUMO')
    print('=' * 80)
    if not diffs and not only_fb and not only_pg:
        print('  Todas as tabelas em comum estao com contagens iguais.')
    else:
        if diffs:
            print(f'\n  Tabelas com CONTAGENS DIFERENTES ({len(diffs)}):')
            for t in diffs:
                print(f'    - {t}')
        if only_fb:
            print(f'\n  Tabelas somente no FIREBIRD ({len(only_fb)}):')
            for t in only_fb:
                print(f'    - {t}')
        if only_pg:
            print(f'\n  Tabelas somente no POSTGRESQL ({len(only_pg)}):')
            for t in only_pg:
                print(f'    - {t}')
    print()


def _print_rich(rows: list[dict], only_fb: list, only_pg: list, diffs: list):
    """Saída com Rich (tabela colorida)."""
    tbl = Table(
        title='Comparacao de Contagem: Firebird vs PostgreSQL',
        box=box.ROUNDED,
        header_style='bold cyan',
        show_lines=False,
    )
    tbl.add_column('Tabela',      style='bold', min_width=30)
    tbl.add_column('Firebird',    justify='right', min_width=13)
    tbl.add_column('PostgreSQL',  justify='right', min_width=13)
    tbl.add_column('Diferenca',   justify='right', min_width=13)
    tbl.add_column('Status',      justify='center', min_width=12)

    for r in rows:
        fb   = r['fb']
        pg   = r['pg']
        diff = r['diff']
        st   = r['status']

        fb_s   = f'{fb:,}'   if isinstance(fb, int)   else 'N/A'
        pg_s   = f'{pg:,}'   if isinstance(pg, int)   else 'N/A'
        diff_s = f'{diff:+,}' if isinstance(diff, int) else 'N/A'

        if st == 'OK':
            color, diff_style = 'green', 'dim'
        elif st == 'SO_FB':
            color, diff_style = 'yellow', 'yellow'
        elif st == 'SO_PG':
            color, diff_style = 'yellow', 'yellow'
        else:
            color, diff_style = 'red', 'bold red'

        tbl.add_row(
            r['table'],
            fb_s,
            pg_s,
            Text(diff_s, style=diff_style),
            Text(st, style=f'bold {color}'),
        )

    console.print()
    console.print(tbl)

    # ─── Resumo ───────────────────────────────────────────────
    console.print()
    if not diffs and not only_fb and not only_pg:
        console.print('[bold green]  Todas as tabelas em comum estao com contagens iguais.[/]')
    else:
        if diffs:
            console.print(f'[bold red]  Tabelas com CONTAGENS DIFERENTES ({len(diffs)}):[/]')
            for t in diffs:
                console.print(f'    [red]- {t}[/]')
        if only_fb:
            console.print(f'\n[bold yellow]  Tabelas somente no FIREBIRD ({len(only_fb)}):[/]')
            for t in only_fb:
                console.print(f'    [yellow]- {t}[/]')
        if only_pg:
            console.print(f'\n[bold yellow]  Tabelas somente no POSTGRESQL ({len(only_pg)}):[/]')
            for t in only_pg:
                console.print(f'    [yellow]- {t}[/]')
    console.print()


# ─── Main ──────────────────────────────────────────────────────────────────────

class Tee:
    def __init__(self, *files):
        self.files = files
    def write(self, data):
        for f in self.files:
            f.write(data)
            f.flush()
    def flush(self):
        for f in self.files:
            f.flush()
    def isatty(self):
        return any(f.isatty() for f in self.files if hasattr(f, 'isatty'))

def main():
    parser = argparse.ArgumentParser(description='Compara contagem de tabelas Firebird vs PostgreSQL')
    parser.add_argument('--work-dir', required=True, help='Diretório de trabalho da migração')
    parser.add_argument('--config', default=None, help='Caminho do config.yaml (default: work-dir/config.yaml)')
    parser.add_argument('--schema', default=None, help='Schema PostgreSQL (default: lido do config)')
    args = parser.parse_args()

    work_dir = Path(args.work_dir)
    if not work_dir.exists():
        sys.exit(f'Erro: diretório de trabalho não encontrado: {work_dir}')

    config_path = Path(args.config) if args.config else work_dir / 'config.yaml'
    if not config_path.exists():
        sys.exit(f'Erro: config nao encontrado: {config_path}')

    # Configura log
    log_dir = work_dir / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / 'compare_counts.log'
    
    f_log = open(log_file, 'a', encoding='utf-8')
    sys.stdout = Tee(sys.stdout, f_log)
    sys.stderr = Tee(sys.stderr, f_log)

    # Re-inicializa console Rich para usar o novo stdout
    if HAS_RICH:
        global console
        if not sys.stdout.isatty():
            console = Console(file=sys.stdout, force_terminal=True)
        else:
            console = Console(file=sys.stdout)

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
    fb_map = {t.lower(): t for t in fb_tables_raw}   # lower -> nome original FB
    pg_map = {t.lower(): t for t in pg_tables_raw}   # lower -> nome original PG

    all_keys = sorted(set(fb_map) | set(pg_map))
    total = len(all_keys)

    rows = []
    only_fb = []
    only_pg = []
    diffs = []

    print(f'Comparando contagem de {total} tabelas...\n')

    for i, key in enumerate(all_keys, 1):
        in_fb = key in fb_map
        in_pg = key in pg_map

        fb_name = fb_map.get(key)
        pg_name = pg_map.get(key)

        label = f'[{i:>3}/{total}] {key}'

        if in_fb and in_pg:
            try:
                fb_cnt = _fb_count(fb_conn, fb_name)
            except Exception as e:
                print(f'  ERRO Firebird ({fb_name}): {e}')
                fb_cnt = None

            try:
                pg_cnt = _pg_count(pg_conn, schema, pg_name)
            except Exception as e:
                print(f'  ERRO PostgreSQL ({pg_name}): {e}')
                pg_cnt = None

            if fb_cnt is not None and pg_cnt is not None:
                diff = pg_cnt - fb_cnt
                status = 'OK' if diff == 0 else 'DIFF'
                if diff != 0:
                    diffs.append(key)
            else:
                diff = None
                status = 'ERRO'

            marker = '' if status == 'OK' else ' <--'
            print(f'  {label:<45} FB={fb_cnt if fb_cnt is not None else "ERR":>10,}  PG={pg_cnt if pg_cnt is not None else "ERR":>10,}{marker}')
            rows.append({'table': key, 'fb': fb_cnt, 'pg': pg_cnt, 'diff': diff, 'status': status})

        elif in_fb and not in_pg:
            try:
                fb_cnt = _fb_count(fb_conn, fb_name)
            except Exception:
                fb_cnt = None
            print(f'  {label:<45} FB={fb_cnt if fb_cnt is not None else "ERR":>10,}  PG={"N/A":>10}  [SÓ FB]')
            only_fb.append(key)
            rows.append({'table': key, 'fb': fb_cnt, 'pg': None, 'diff': None, 'status': 'SO_FB'})

        else:  # only PG
            try:
                pg_cnt = _pg_count(pg_conn, schema, pg_name)
            except Exception:
                pg_cnt = None
            print(f'  {label:<45} FB={"N/A":>10}  PG={pg_cnt if pg_cnt is not None else "ERR":>10,}  [SÓ PG]')
            only_pg.append(key)
            rows.append({'table': key, 'fb': None, 'pg': pg_cnt, 'diff': None, 'status': 'SO_PG'})

    fb_conn.close()
    pg_conn.close()

    print()
    if HAS_RICH:
        _print_rich(rows, only_fb, only_pg, diffs)
    else:
        _print_plain(rows, only_fb, only_pg, diffs)

    # Código de saída: 0 = tudo ok, 1 = há diferenças
    if diffs or only_fb or only_pg:
        sys.exit(1)


if __name__ == '__main__':
    main()
