#!/usr/bin/env python3
"""
enable_constraints.py
=====================
Executa os enable_constraints_*.sql na ordem correta, recriando
PKs, índices, unique, checks, FKs e triggers de todas as tabelas migradas.
Gera relatório detalhado com Rich ao final.

Uso:
    python enable_constraints.py
    python enable_constraints.py --dry-run
    python enable_constraints.py --dir /outro/diretorio
    python enable_constraints.py --table operacao_credito  # só uma tabela
"""

import sys
import re
import time
import argparse
import logging
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional

import yaml
import psycopg2

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
    from rich.text import Text
    from rich import box
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

BASE_DIR = Path(__file__).parent
WORK_DIR = BASE_DIR / 'work'
LOG_DIR  = BASE_DIR / 'logs'

# Ordem de execução das tabelas (respeita dependências de FK entre tabelas)
TABLE_ORDER = [
    'parcelasctb',
    'nmov',
    'historico_operacao',
    'log_eventos',
    'operacao_credito',
    'pessoa_pretendente',
    'ocorrencia',
    'ocorrencia_sisat',
    'controleversao',
    'documento_operacao',
]


# ═══════════════════════════════════════════════════════════════
#  ESTRUTURAS DE DADOS
# ═══════════════════════════════════════════════════════════════

@dataclass
class StmtResult:
    table: str
    stmt_type: str       # index, primary_key, unique, check, foreign_key, trigger, analyze, reindex, config
    obj_name: str        # nome extraído do SQL
    sql: str
    status: str          # OK | SKIP | FAIL | DRY
    duration_ms: float = 0.0
    error: str = ''


@dataclass
class TableResult:
    table: str
    sql_file: str
    ok: int = 0
    skip: int = 0
    fail: int = 0
    dry: int = 0
    duration_s: float = 0.0
    stmts: List[StmtResult] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════
#  PARSING DE SQL
# ═══════════════════════════════════════════════════════════════

# Palavras que indicam statements de controle — executados mas não contabilizados
_CONTROL_PREFIXES = ('begin', 'commit', 'rollback')

# Prefixos que queremos pular silenciosamente
_SKIP_PREFIXES = ('begin', 'commit', 'rollback')


def _classify(sql: str) -> tuple[str, str]:
    """
    Retorna (stmt_type, obj_name) a partir do texto SQL.
    Usado apenas para classificação no relatório.
    """
    s = sql.strip().upper()
    low = sql.strip().lower()

    # SET
    if s.startswith('SET '):
        m = re.search(r'set\s+(\w+)', low)
        return 'config', m.group(1) if m else 'set'

    # ANALYZE
    if s.startswith('ANALYZE'):
        m = re.search(r'analyze\s+"?(\w+)"?\."?(\w+)"?', low)
        return 'analyze', m.group(2) if m else 'analyze'

    # REINDEX
    if s.startswith('REINDEX'):
        m = re.search(r'reindex\s+table\s+"?(\w+)"?\."?(\w+)"?', low)
        return 'reindex', m.group(2) if m else 'reindex'

    # CREATE INDEX
    if s.startswith('CREATE INDEX') or s.startswith('CREATE UNIQUE INDEX'):
        m = re.search(r'create\s+(?:unique\s+)?index\s+"?(\w+)"?', low)
        return 'index', m.group(1) if m else 'index'

    # ADD CONSTRAINT
    if 'ADD CONSTRAINT' in s:
        m = re.search(r'add\s+constraint\s+"?(\w+)"?', low)
        name = m.group(1) if m else 'constraint'
        if 'PRIMARY KEY' in s:
            return 'primary_key', name
        if 'FOREIGN KEY' in s:
            return 'foreign_key', name
        if 'UNIQUE' in s:
            return 'unique', name
        if 'CHECK' in s:
            return 'check', name
        return 'constraint', name

    # ENABLE TRIGGER
    if 'ENABLE TRIGGER' in s:
        m = re.search(r'enable\s+trigger\s+"?(\w+)"?', low)
        return 'trigger', m.group(1) if m else 'trigger'

    return 'other', sql[:40].strip()


def parse_sql_file(path: Path) -> List[str]:
    """
    Lê um .sql e retorna lista de statements individuais,
    sem comments, sem BEGIN/COMMIT, sem strings vazias.
    """
    text = path.read_text(encoding='utf-8')
    statements = []

    for raw in text.split(';'):
        # Remove linhas de comentário
        lines = [ln for ln in raw.splitlines()
                 if not ln.strip().startswith('--')]
        stmt = '\n'.join(lines).strip()

        if not stmt:
            continue

        upper = stmt.upper()
        if upper in ('BEGIN', 'COMMIT', 'ROLLBACK'):
            continue

        statements.append(stmt)

    return statements


# ═══════════════════════════════════════════════════════════════
#  EXECUÇÃO
# ═══════════════════════════════════════════════════════════════

def _is_already_exists(e: Exception) -> bool:
    msg = str(e).lower()
    return ('already exists' in msg
            or 'duplicate' in msg
            or 'já existe' in msg)


def execute_file(sql_file: Path, table: str, pg_conn_params: dict,
                 dry_run: bool = False, log: logging.Logger = None) -> TableResult:
    """
    Executa um enable_constraints_*.sql statement a statement.
    Retorna TableResult com o resultado de cada statement.
    """
    result = TableResult(table=table, sql_file=sql_file.name)
    statements = parse_sql_file(sql_file)

    if dry_run:
        for stmt in statements:
            stmt_type, obj_name = _classify(stmt)
            result.stmts.append(StmtResult(
                table=table, stmt_type=stmt_type, obj_name=obj_name,
                sql=stmt, status='DRY'))
            result.dry += 1
        return result

    conn = psycopg2.connect(**pg_conn_params)
    conn.autocommit = True   # DDL não pode rodar em transação explícita aberta
    cur = conn.cursor()

    t_table = time.time()
    try:
        for stmt in statements:
            stmt_type, obj_name = _classify(stmt)
            t0 = time.time()
            try:
                cur.execute(stmt)
                duration_ms = (time.time() - t0) * 1000
                result.stmts.append(StmtResult(
                    table=table, stmt_type=stmt_type, obj_name=obj_name,
                    sql=stmt, status='OK', duration_ms=duration_ms))
                result.ok += 1
                if log:
                    log.info(f'  [OK]   {stmt_type:<15} {obj_name}  ({duration_ms:.0f}ms)')

            except Exception as e:
                duration_ms = (time.time() - t0) * 1000
                if _is_already_exists(e):
                    result.stmts.append(StmtResult(
                        table=table, stmt_type=stmt_type, obj_name=obj_name,
                        sql=stmt, status='SKIP', duration_ms=duration_ms,
                        error='já existe'))
                    result.skip += 1
                    if log:
                        log.warning(f'  [SKIP] {stmt_type:<15} {obj_name}  (já existe)')
                else:
                    err_msg = str(e).splitlines()[0][:120]
                    result.stmts.append(StmtResult(
                        table=table, stmt_type=stmt_type, obj_name=obj_name,
                        sql=stmt, status='FAIL', duration_ms=duration_ms,
                        error=err_msg))
                    result.fail += 1
                    if log:
                        log.error(f'  [FAIL] {stmt_type:<15} {obj_name}  → {err_msg}')
                # Continua mesmo em erro
                try:
                    conn.autocommit = True  # garante que próximo statement não herda estado de erro
                except Exception:
                    pass

    finally:
        result.duration_s = time.time() - t_table
        cur.close()
        conn.close()

    return result


# ═══════════════════════════════════════════════════════════════
#  RELATÓRIO
# ═══════════════════════════════════════════════════════════════

STATUS_STYLE = {
    'OK':   'bold green',
    'SKIP': 'yellow',
    'FAIL': 'bold red',
    'DRY':  'dim cyan',
}


def _fmt_dur(ms: float) -> str:
    if ms >= 60_000:
        return f'{ms/60_000:.1f}min'
    if ms >= 1_000:
        return f'{ms/1_000:.1f}s'
    return f'{ms:.0f}ms'


def print_report(results: List[TableResult], dry_run: bool, console=None):
    total_ok   = sum(r.ok   for r in results)
    total_skip = sum(r.skip for r in results)
    total_fail = sum(r.fail for r in results)
    total_dry  = sum(r.dry  for r in results)
    total_time = sum(r.duration_s for r in results)

    if HAS_RICH and console:
        _print_rich(results, dry_run, console,
                    total_ok, total_skip, total_fail, total_dry, total_time)
    else:
        _print_plain(results, dry_run,
                     total_ok, total_skip, total_fail, total_dry, total_time)


def _print_rich(results, dry_run, console,
                total_ok, total_skip, total_fail, total_dry, total_time):

    # ── Detalhes por tabela ──────────────────────────────────
    for r in results:
        tbl = Table(
            title=f'  {r.table}  |  {r.sql_file}',
            header_style='bold cyan',
            box=box.SIMPLE_HEAVY,
            show_lines=False,
            expand=True,
        )
        tbl.add_column('Tipo',    style='dim',   min_width=14)
        tbl.add_column('Nome',    min_width=30)
        tbl.add_column('Status',  justify='center', min_width=6)
        tbl.add_column('Tempo',   justify='right', min_width=8, style='dim')
        tbl.add_column('Detalhe', min_width=30, style='dim')

        for s in r.stmts:
            style = STATUS_STYLE.get(s.status, 'white')
            tbl.add_row(
                s.stmt_type,
                s.obj_name,
                Text(s.status, style=style),
                _fmt_dur(s.duration_ms),
                s.error[:60] if s.error else '',
            )

        # Linha de totais da tabela
        ok_txt   = Text(f'{r.ok} OK',   style='green')
        skip_txt = Text(f'{r.skip} skip', style='yellow')
        fail_txt = Text(f'{r.fail} FAIL', style='red' if r.fail else 'dim')
        console.print(tbl)
        console.print(
            f'  → {r.ok} OK  |  {r.skip} skip  |  {r.fail} FAIL  '
            f'|  {_fmt_dur(r.duration_s * 1000)} total\n')

    # ── Sumário geral ────────────────────────────────────────
    summary = Table(
        title='  RESUMO FINAL — ENABLE CONSTRAINTS',
        header_style='bold cyan',
        box=box.ROUNDED,
        show_lines=True,
        expand=False,
    )
    summary.add_column('Tabela',   style='bold', min_width=25)
    summary.add_column('OK',       justify='right', style='green',  min_width=6)
    summary.add_column('Skip',     justify='right', style='yellow', min_width=6)
    summary.add_column('Fail',     justify='right', style='red',    min_width=6)
    summary.add_column('Tempo',    justify='right', min_width=9)
    summary.add_column('Status',   justify='center', min_width=10)

    for r in results:
        if r.fail:
            status = Text('COM ERROS', style='bold red')
        elif r.skip and not r.ok:
            status = Text('TUDO SKIP', style='yellow')
        elif dry_run:
            status = Text('DRY-RUN',   style='dim cyan')
        else:
            status = Text('OK ✓',      style='bold green')

        summary.add_row(
            r.table,
            str(r.ok),
            str(r.skip),
            str(r.fail),
            _fmt_dur(r.duration_s * 1000),
            status,
        )

    # Linha de totais
    summary.add_row(
        Text('TOTAL', style='bold'),
        Text(str(total_ok),   style='bold green'),
        Text(str(total_skip), style='bold yellow'),
        Text(str(total_fail), style='bold red' if total_fail else 'bold'),
        Text(_fmt_dur(total_time * 1000), style='bold'),
        Text('COM ERROS' if total_fail else ('DRY-RUN' if dry_run else 'SUCESSO ✓'),
             style='bold red' if total_fail else ('dim cyan' if dry_run else 'bold green')),
    )

    console.print(summary)

    if total_fail:
        console.print(
            f'\n[bold red]  {total_fail} statements falharam.[/bold red] '
            f'Verifique os detalhes acima e o arquivo de log.')
    elif dry_run:
        console.print(
            f'\n[dim cyan]  DRY-RUN: {total_dry} statements seriam executados.[/dim cyan]')
    else:
        console.print(
            f'\n[bold green]  Concluído em {_fmt_dur(total_time * 1000)}. '
            f'{total_ok} OK | {total_skip} já existiam.[/bold green]')


def _print_plain(results, dry_run,
                 total_ok, total_skip, total_fail, total_dry, total_time):
    print()
    print('=' * 72)
    print('  ENABLE CONSTRAINTS — RESULTADO')
    print('=' * 72)
    for r in results:
        print(f'\n  Tabela: {r.table}  ({r.sql_file})')
        print(f'  {"Tipo":<15} {"Nome":<35} {"Status":<6} {"Tempo":>8}')
        print('  ' + '-' * 68)
        for s in r.stmts:
            print(f'  {s.stmt_type:<15} {s.obj_name[:34]:<35} '
                  f'{s.status:<6} {_fmt_dur(s.duration_ms):>8}')
            if s.error:
                print(f'    → {s.error[:65]}')
        print(f'  OK={r.ok}  SKIP={r.skip}  FAIL={r.fail}  {_fmt_dur(r.duration_s*1000)}')

    print()
    print('=' * 72)
    print(f'  TOTAL  OK={total_ok}  SKIP={total_skip}  FAIL={total_fail}  '
          f'{_fmt_dur(total_time * 1000)}')
    print('=' * 72)


def save_report(results: List[TableResult], out_path: Path):
    """Salva relatório em texto puro para consulta posterior."""
    lines = [
        f'ENABLE CONSTRAINTS — RELATÓRIO',
        f'Gerado em: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '=' * 80,
        '',
    ]
    for r in results:
        lines.append(f'TABELA: {r.table}  ({r.sql_file})')
        lines.append(f'  Resultado: {r.ok} OK | {r.skip} skip | {r.fail} FAIL | {_fmt_dur(r.duration_s*1000)}')
        for s in r.stmts:
            lines.append(f'  [{s.status:<4}] {s.stmt_type:<15} {s.obj_name}')
            if s.error:
                lines.append(f'         ERRO: {s.error}')
        lines.append('')

    total_ok   = sum(r.ok   for r in results)
    total_skip = sum(r.skip for r in results)
    total_fail = sum(r.fail for r in results)
    total_time = sum(r.duration_s for r in results)

    lines += [
        '=' * 80,
        f'TOTAL: {total_ok} OK | {total_skip} skip | {total_fail} FAIL | {_fmt_dur(total_time*1000)}',
    ]
    out_path.write_text('\n'.join(lines), encoding='utf-8')


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description='Habilita constraints/PKs/índices após migração',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python enable_constraints.py
  python enable_constraints.py --dry-run
  python enable_constraints.py --table operacao_credito
  python enable_constraints.py --table operacao_credito --table historico_operacao
  python enable_constraints.py --dir /migracao_firebird
        """)
    ap.add_argument('-c', '--config', default='config.yaml',
                    help='Arquivo de configuração YAML (padrão: config.yaml)')
    ap.add_argument('-d', '--dir', default=None,
                    help='Diretório dos .sql (padrão: mesmo dir do script)')
    ap.add_argument('-t', '--table', action='append', dest='tables', metavar='TABELA',
                    help='Executar apenas esta tabela (pode repetir para múltiplas)')
    ap.add_argument('--dry-run', action='store_true',
                    help='Mostra o que seria executado sem rodar no banco')
    ap.add_argument('--fail-fast', action='store_true',
                    help='Interrompe na primeira tabela com FAIL')
    ap.add_argument('--report', default=None, metavar='ARQUIVO',
                    help='Salva relatório em texto puro neste arquivo')
    args = ap.parse_args()

    sql_dir = Path(args.dir) if args.dir else WORK_DIR

    # ── Config ───────────────────────────────────────────────
    cfg_path = Path(args.config) if Path(args.config).is_absolute() else BASE_DIR / args.config
    if not cfg_path.exists():
        print(f'ERRO: {cfg_path} não encontrado.')
        sys.exit(1)

    with open(cfg_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    cfg_pg = config['postgresql']
    pg_params = {
        'host':     cfg_pg['host'],
        'port':     cfg_pg.get('port', 5432),
        'database': cfg_pg['database'],
        'user':     cfg_pg['user'],
        'password': cfg_pg['password'],
    }

    # ── Logging ──────────────────────────────────────────────
    log_path = LOG_DIR / f'enable_constraints_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)-7s] %(message)s',
        datefmt='%H:%M:%S',
        handlers=[
            logging.FileHandler(str(log_path), encoding='utf-8'),
            logging.StreamHandler(sys.stdout),
        ])
    log = logging.getLogger('enable')

    console = Console() if HAS_RICH else None

    # ── Descoberta de arquivos ───────────────────────────────
    # Usa TABLE_ORDER como ordem canônica; filtra pelo --table se especificado
    requested = set(args.tables) if args.tables else None

    ordered_tables = []
    for table in TABLE_ORDER:
        if requested and table not in requested:
            continue
        sql_file = sql_dir / f'enable_constraints_{table}.sql'
        if sql_file.exists():
            ordered_tables.append((table, sql_file))
        else:
            log.warning(f'Arquivo não encontrado: {sql_file.name} — pulando.')

    # Tabelas solicitadas que não estão em TABLE_ORDER (ex: --table nova_tabela)
    if requested:
        for table in requested:
            if table not in TABLE_ORDER:
                sql_file = sql_dir / f'enable_constraints_{table}.sql'
                if sql_file.exists():
                    ordered_tables.append((table, sql_file))
                else:
                    log.warning(f'Arquivo não encontrado: {sql_file.name} — pulando.')

    if not ordered_tables:
        log.error('Nenhum arquivo enable_constraints_*.sql encontrado.')
        sys.exit(1)

    log.info('=' * 70)
    log.info(f'  ENABLE CONSTRAINTS — {"DRY-RUN" if args.dry_run else "EXECUÇÃO"}')
    log.info(f'  Tabelas: {len(ordered_tables)}')
    log.info(f'  Banco  : {cfg_pg["database"]} @ {cfg_pg["host"]}:{cfg_pg.get("port", 5432)}')
    log.info(f'  Log    : {log_path.name}')
    log.info('=' * 70)

    # ── Execução ─────────────────────────────────────────────
    results: List[TableResult] = []

    for table, sql_file in ordered_tables:
        log.info('')
        log.info(f'━' * 70)
        log.info(f'  Tabela: {table}  ({sql_file.name})')
        log.info(f'━' * 70)

        stmts = parse_sql_file(sql_file)
        log.info(f'  {len(stmts)} statements encontrados')

        result = execute_file(
            sql_file=sql_file,
            table=table,
            pg_conn_params=pg_params,
            dry_run=args.dry_run,
            log=log,
        )
        results.append(result)

        status_msg = (f'OK={result.ok}  SKIP={result.skip}  FAIL={result.fail}  '
                      f'{_fmt_dur(result.duration_s * 1000)}')
        if result.fail:
            log.error(f'  Resultado: {status_msg}')
        else:
            log.info(f'  Resultado: {status_msg}')

        if args.fail_fast and result.fail:
            log.error('  --fail-fast ativado. Interrompendo.')
            break

    # ── Relatório ─────────────────────────────────────────────
    log.info('')
    log.info('=' * 70)
    log.info('  RELATÓRIO FINAL')
    log.info('=' * 70)

    print_report(results, args.dry_run, console)

    # Salva relatório em arquivo se solicitado
    report_path = Path(args.report) if args.report else (
        LOG_DIR / f'relatorio_enable_constraints_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
    save_report(results, report_path)
    log.info(f'\n  Relatório salvo em: {report_path.name}')

    # Exit code
    total_fail = sum(r.fail for r in results)
    sys.exit(1 if total_fail else 0)


if __name__ == '__main__':
    main()
