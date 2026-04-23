#!/usr/bin/env python3
"""
monitor.py
==========
Acompanha progresso de até 10 migrações em paralelo em tempo real.

Uso:
    python monitor.py                          # painel com todas as migrações detectadas
    python monitor.py --state-db X.db          # detalhe de uma tabela específica
    python monitor.py --summary                # resumo tabular de todas as tabelas
    python monitor.py --history 50             # histórico de batches (requer --state-db)
    python monitor.py --json                   # JSON (requer --state-db)
    python monitor.py --constraints            # ver constraints de todas as tabelas
    python monitor.py -i 5                     # refresh a cada 5s (padrão: 2s)
"""

import sys
import os
import json
import time
import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.live import Live
    from rich.columns import Columns
    from rich.text import Text
    from rich import box
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

BASE_DIR = Path(__file__).parent
WORK_DIR = BASE_DIR / 'work'
LOG_DIR  = BASE_DIR / 'logs'

def _discover_dbs() -> list[Path]:
    """Descobre todos os migration_state_*.db na raiz, em work/ e em subpastas MIGRACAO_XXXX."""
    dbs = list(WORK_DIR.glob('migration_state_*.db'))
    # Também busca em subpastas de migração do Maestro V2
    for mig_dir in BASE_DIR.glob('MIGRACAO_*'):
        if mig_dir.is_dir():
            # Busca tanto na raiz da pasta de migração quanto em subpastas work dela
            dbs.extend(mig_dir.glob('migration_state_*.db'))
            dbs.extend((mig_dir / 'work').glob('migration_state_*.db'))
    
    # Remove duplicatas se houver (mesmo caminho) e ordena
    unique_dbs = {p.resolve(): p for p in dbs}
    return sorted(unique_dbs.values(), key=lambda x: x.name)

def _build_multi_table(dbs: list[Path]) -> Table:
    """Monta a rich.Table com uma linha por migração."""
    tbl = Table(
        title=f'  Quadro Geral de Migracoes  |  {datetime.now().strftime("%H:%M:%S")}',
        header_style='bold cyan',
        box=box.ROUNDED,
        show_lines=True,
        expand=True,
    )
    tbl.add_column('Sessão/Pasta', style='dim', width=15)
    tbl.add_column('Tabela',    style='bold',  min_width=18)
    tbl.add_column('Status',    justify='center', min_width=10)
    tbl.add_column('Progresso', min_width=28)
    tbl.add_column('Linhas',    justify='right', min_width=22)
    tbl.add_column('Vel (l/s)', justify='right', min_width=10)
    tbl.add_column('ETA',       justify='right', min_width=10)

    totals_m = 0
    totals_t = 0

    for db_path in dbs:
        p = _read_progress(db_path)
        
        # Identifica a sessão (MIGRACAO_XXXX)
        parts = db_path.parts
        session = "RAIZ"
        for part in parts:
            if part.startswith("MIGRACAO_"):
                session = part
                break

        if not p:
            # DB existe mas sem dados ainda
            name = db_path.stem.removeprefix('migration_state_')
            tbl.add_row(session, name, Text('AGUARDANDO', style='dim'),
                        Text(_bar(0) + '   0.00%', style='dim'),
                        '-', '-', '-')
            continue

        src   = p.get('source_table', '?')
        dst   = p.get('dest_table', '?')
        label = f'{src}\n→ {dst}'

        status = p.get('status', 'idle')
        color  = _status_color(status)
        status_text = Text(status.upper(), style=f'bold {color}')

        m   = p.get('rows_migrated', 0)
        t   = p.get('total_rows', 0)
        pct = _calc_pct(p)
        totals_m += m
        totals_t += t

        bar_str = _bar(pct, 20)
        progress_text = Text(f'{bar_str}  {pct:5.2f}%', style=color)

        rows_text = f'{m:>12,} / {t:>12,}'

        spd  = p.get('speed_rows_per_sec', 0) or 0
        eta  = p.get('eta_seconds') or 0

        tbl.add_row(
            session,
            label,
            status_text,
            progress_text,
            rows_text,
            f'{spd:>10,.0f}',
            _fd(eta),
        )

    # Linha de totais
    if totals_t > 0:
        pct_total = (totals_m / totals_t * 100)
        tbl.add_row(
            'TOTAL GERAL',
            '',
            '',
            Text(f'{_bar(pct_total, 20)}  {pct_total:5.2f}%', style='bold cyan'),
            Text(f'{totals_m:>12,} / {totals_t:>12,}', style='bold'),
            '', '',
        )

    return tbl



def display_live_all(interval: float = 2.0, only: set = None):
    """Painel ao vivo com todas as migrações detectadas.
    Se `only` for fornecido, exibe apenas os DBs cujo nome estiver no conjunto.
    """
    def _get(o): return _filter_dbs(_discover_dbs(), o) if o else _discover_dbs()

    dbs = _get(only)
    if not dbs:
        print('Nenhum arquivo migration_state_*.db encontrado.')
        print('Inicie a migracao com: python migrator.py --table NOME_TABELA')
        return

    if HAS_RICH:
        console = Console()
        with Live(_build_multi_table(dbs), console=console,
                  refresh_per_second=1 / interval) as live:
            try:
                while True:
                    time.sleep(interval)
                    dbs = _get(only)
                    live.update(_build_multi_table(dbs))
            except KeyboardInterrupt:
                pass
    else:
        try:
            while True:
                os.system('cls' if os.name == 'nt' else 'clear')
                dbs = _get(only)
                print('=' * 80)
                print('  MONITOR MIGRACOES - TODAS AS TABELAS')
                print(f'  {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
                print('=' * 80)
                for db_path in dbs:
                    p = _read_progress(db_path)
                    if not p:
                        name = db_path.stem.removeprefix('migration_state_')
                        print(f'  {name:<20} AGUARDANDO')
                        continue
                    m   = p.get('rows_migrated', 0)
                    t   = p.get('total_rows', 0)
                    pct = _calc_pct(p)
                    print(f'  {p.get("dest_table","?"):<20} '
                          f'{p.get("status","?").upper():<12} '
                          f'[{_bar(pct, 15)}] {pct:5.2f}%  '
                          f'{m:>10,}/{t:>10,}  '
                          f'{p.get("speed_rows_per_sec",0):>8,.0f} l/s  '
                          f'ETA {_fd(p.get("eta_seconds",0))}')
                print('\nCtrl+C para sair')
                time.sleep(interval)
        except KeyboardInterrupt:
            pass


# ─── exibição de tabela única (modo detalhe) ────────────────────────────────

class MigrationMonitor:
    """Modo detalhe para uma tabela específica (--state-db)."""

    def __init__(self, state_db: str):
        self.state_db = Path(state_db)
        self.console = Console() if HAS_RICH else None

    def get_progress(self) -> dict:
        return _read_progress(self.state_db)

    def get_recent(self, n: int = 30) -> list:
        return _read_recent(self.state_db, n)

    def display_live(self, interval: float = 2.0):
        if HAS_RICH:
            self._display_rich(interval)
        else:
            self._display_simple(interval)

    def _display_rich(self, interval: float):
        def build():
            p = self.get_progress()
            if not p:
                return Panel('Nenhum dado.', title='Monitor')

            status = p.get('status', '?')
            color  = _status_color(status)

            m   = p.get('rows_migrated', 0)
            t   = p.get('total_rows', 0)
            pct = _calc_pct(p)
            spd = p.get('speed_rows_per_sec', 0) or 0
            eta = p.get('eta_seconds', 0) or 0
            elp = p.get('elapsed_seconds', 0) or 0

            bar = _bar(pct, 40)
            lines = [
                f"[bold]Tabela:[/bold] {p.get('source_table','?')} -> {p.get('dest_table','?')}",
                f"[bold]Status:[/bold] [{color}]{status.upper()}[/{color}]   "
                f"[bold]Fase:[/bold] {p.get('phase','?')}",
                '',
                f'  [{bar}] {pct:.2f}%',
                '',
                f'  Linhas:     {m:>12,} / {t:>12,}',
                f'  Batches:    {p.get("current_batch",0):>12,} / {p.get("total_batches",0):>12,}',
                f'  Velocidade: {spd:>12,.0f} lin/s',
                f'  Decorrido:  {_fd(elp):>20}',
                f'  ETA:        {_fd(eta) if eta else "N/A":>20}',
            ]

            if p.get('last_pk_value'):
                lines.append(f'\n  [dim]PK: {p["last_pk_value"]}[/dim]')
            if p.get('error_message'):
                lines.append(f'\n  [red]Erro: {p["error_message"]}[/red]')

            batches = self.get_recent(8)
            if batches:
                lines.append('\n[bold]Ultimos batches:[/bold]')
                for b in batches:
                    ts = (b[0] or '?')[:19]
                    lines.append(
                        f'  {ts} | Lote {b[1]:>6,} | {b[2]:>6,} lin | '
                        f'{(b[4] or 0):>10,.0f} l/s')

            return Panel(
                '\n'.join(lines),
                title=f'Migracao | {p.get("updated_at","")[:19]}',
                border_style=color,
            )

        with Live(build(), refresh_per_second=1 / interval,
                  console=self.console) as live:
            try:
                while True:
                    time.sleep(interval)
                    live.update(build())
            except KeyboardInterrupt:
                pass

    def _display_simple(self, interval: float):
        try:
            while True:
                os.system('cls' if os.name == 'nt' else 'clear')
                p = self.get_progress()
                if not p:
                    print('Nenhum dado encontrado.')
                    time.sleep(interval)
                    continue

                m   = p.get('rows_migrated', 0)
                t   = p.get('total_rows', 0)
                pct = _calc_pct(p)

                print('=' * 65)
                print('  MONITOR MIGRACAO FIREBIRD -> POSTGRESQL')
                print('=' * 65)
                print(f'  Tabela: {p.get("source_table","?")} -> {p.get("dest_table","?")}')
                print(f'  Status: {p.get("status","?").upper()}  Fase: {p.get("phase","?")}')
                print(f'  [{_bar(pct, 30)}] {pct:.2f}%')
                print(f'  Linhas: {m:>12,} / {t:>12,}')
                print(f'  Vel:    {p.get("speed_rows_per_sec",0):>12,.0f} l/s')
                print(f'  ETA:    {_fd(p.get("eta_seconds",0)):>12}')

                batches = self.get_recent(6)
                if batches:
                    print(f'\n  {"Hora":<10} {"Batch":>7} {"Linhas":>8} {"Vel":>10}')
                    for b in batches:
                        print(f'  {(b[0] or "?")[11:19]:<10} '
                              f'{b[1]:>7,} {b[2]:>8,} {(b[4] or 0):>10,.0f}')

                print('\n  Ctrl+C sair')
                time.sleep(interval)
        except KeyboardInterrupt:
            pass

    def show_summary(self):
        p = self.get_progress()
        if not p:
            print('Nenhum dado encontrado.')
            return

        m   = p.get('rows_migrated', 0)
        t   = p.get('total_rows', 0)
        pct = _calc_pct(p)

        if HAS_RICH:
            tbl = Table(title='Resumo', header_style='bold cyan')
            tbl.add_column('Campo', style='bold')
            tbl.add_column('Valor', justify='right')
            for label, val in [
                ('Tabela',         f'{p.get("source_table","?")} -> {p.get("dest_table","?")}'),
                ('Status',         p.get('status','?').upper()),
                ('Progresso',      f'[{_bar(pct, 30)}] {pct:.2f}%'),
                ('Linhas',         f'{m:,} / {t:,}'),
                ('Batch',          f'{p.get("current_batch",0):,} / {p.get("total_batches",0):,}'),
                ('Velocidade',     f'{p.get("speed_rows_per_sec",0):,.0f} lin/s'),
                ('ETA',            _fd(p.get('eta_seconds', 0))),
                ('Constraints OFF',str(p.get('constraints_disabled', False))),
                ('Inicio',         str(p.get('started_at',''))[:19]),
                ('Atualizado',     str(p.get('updated_at',''))[:19]),
            ]:
                tbl.add_row(label, str(val))
            if p.get('error_message'):
                tbl.add_row('ERRO', f'[red]{p["error_message"]}[/red]')
            self.console.print(tbl)
        else:
            for label, val in [
                ('Tabela',    f'{p.get("source_table","?")} -> {p.get("dest_table","?")}'),
                ('Status',    p.get('status','?').upper()),
                ('Progresso', f'[{_bar(pct, 30)}] {pct:.2f}%'),
                ('Linhas',    f'{m:,} / {t:,}'),
                ('Vel',       f'{p.get("speed_rows_per_sec",0):,.0f} l/s'),
                ('ETA',       _fd(p.get('eta_seconds', 0))),
            ]:
                print(f'  {label:<15} {val}')

    def show_json(self):
        p = self.get_progress()
        if p.get('last_db_key') and isinstance(p['last_db_key'], bytes):
            p['last_db_key'] = p['last_db_key'].hex()
        print(json.dumps(p, indent=2, default=str))

    def show_history(self, n: int = 50):
        rows = self.get_recent(n)
        if HAS_RICH:
            tbl = Table(title=f'Ultimos {n} batches', header_style='bold cyan')
            tbl.add_column('Timestamp', style='dim')
            tbl.add_column('Batch',  justify='right')
            tbl.add_column('Linhas', justify='right')
            tbl.add_column('Total',  justify='right')
            tbl.add_column('Vel (l/s)', justify='right')
            for b in rows:
                tbl.add_row(
                    (b[0] or '?')[:19], f'{b[1]:,}', f'{b[2]:,}',
                    f'{b[3]:,}', f'{(b[4] or 0):,.0f}')
            self.console.print(tbl)
        else:
            for b in rows:
                print(f'  {(b[0] or "?")[:19]}  Batch:{b[1]:>6,}  '
                      f'Rows:{b[2]:>6,}  Total:{b[3]:>10,}  '
                      f'Speed:{(b[4] or 0):>10,.0f} l/s')

    def reset(self):
        if not self.state_db.exists():
            print(f'ERRO: {self.state_db} nao encontrado.')
            return
        conn = sqlite3.connect(str(self.state_db), timeout=5)
        conn.executescript('DELETE FROM migration_state; DELETE FROM migration_log;')
        conn.commit()
        conn.close()
        print(f'Estado resetado: {self.state_db}')


# ─── small-tables monitor ────────────────────────────────────────────────────

def _build_small_tables_panel(master_db: Path, interval: float) -> object:
    """Monta painel Rich para o modo --small-tables."""
    from rich.progress import BarColumn, Progress

    state = _read_master_state(master_db)
    s     = state['summary']
    total = s['total']
    done  = s['completed']
    fail  = s['failed']
    pend  = s['pending'] + s['running']

    now_str = datetime.now().strftime('%H:%M:%S')

    if not HAS_RICH:
        return None

    from rich.console import Group

    # ── Cabeçalho: barra de progresso global ─────────────────
    pct = (done / total * 100) if total else 0
    bar = _bar(pct, 40)
    header_lines = [
        f'[bold cyan]Small Tables Migration[/bold cyan]  |  {now_str}  |  refresh {interval}s',
        '',
        f'  [{bar}]  [bold]{pct:.1f}%[/bold]  ({done}/{total} tabelas)',
        f'  [green]Concluídas: {done}[/green]  '
        f'[yellow]Pendentes: {pend}[/yellow]  '
        f'[red]Falhas: {fail}[/red]',
    ]

    from rich.panel import Panel as RPanel
    from rich.table import Table as RTable

    # ── Tabela de workers ativos ──────────────────────────────
    worker_tbl = RTable(
        header_style='bold cyan',
        box=box.SIMPLE,
        expand=True,
        show_header=True,
    )
    worker_tbl.add_column('Tabela (worker ativo)', style='bold', min_width=28)
    worker_tbl.add_column('Status',    justify='center', min_width=12)
    worker_tbl.add_column('Progresso', min_width=30)
    worker_tbl.add_column('Linhas',    justify='right', min_width=22)
    worker_tbl.add_column('Vel (l/s)', justify='right', min_width=10)
    worker_tbl.add_column('ETA',       justify='right', min_width=10)

    running = state['running_tables']
    if running:
        for rt in running:
            dest     = rt['dest']
            db_path  = master_db.parent / f'migration_state_{dest}.db'
            p        = _read_progress(db_path)
            if not p:
                worker_tbl.add_row(
                    f'{rt["source"]} → {dest}',
                    Text('INICIANDO', style='dim'),
                    _bar(0) + '   0.00%',
                    '-', '-', '-',
                )
                continue
            status = p.get('status', 'running')
            color  = _status_color(status)
            m = p.get('rows_migrated', 0)
            t = p.get('total_rows', 0)
            pct_w = _calc_pct(p)
            worker_tbl.add_row(
                f'{rt["source"]} → {dest}',
                Text(status.upper(), style=f'bold {color}'),
                Text(f'{_bar(pct_w, 20)}  {pct_w:5.1f}%', style=color),
                f'{m:>10,} / {t:>10,}',
                f'{p.get("speed_rows_per_sec", 0):>8,.0f}',
                _fd(p.get('eta_seconds', 0)),
            )
    else:
        if total == 0:
            worker_tbl.add_row(
                '[dim]Aguardando início...[/dim]', '', '', '', '', '')
        elif done == total:
            worker_tbl.add_row(
                '[green bold]Migração concluída![/green bold]', '', '', '', '', '')
        else:
            worker_tbl.add_row(
                '[dim]Nenhum worker ativo no momento[/dim]', '', '', '', '', '')

    # ── Falhas (se houver) ────────────────────────────────────
    fail_lines = []
    if state['recent_failed']:
        fail_lines.append('[red bold]Tabelas com falha:[/red bold]')
        for f in state['recent_failed']:
            err = (f.get('error') or '')[:80]
            fail_lines.append(f'  [red]✗ {f["source"]}[/red] — {err}')

    body = '\n'.join(header_lines)
    if fail_lines:
        body += '\n\n' + '\n'.join(fail_lines)

    return RPanel(
        Group(body, worker_tbl),  # type: ignore[arg-type]
        title='Monitor — Tabelas Pequenas',
        border_style='cyan',
    )


def _build_small_tables_plain(master_db: Path):
    """Exibe modo texto simples para --small-tables (sem rich)."""
    state = _read_master_state(master_db)
    s     = state['summary']
    total = s['total']
    done  = s['completed']
    fail  = s['failed']

    pct = (done / total * 100) if total else 0
    print('=' * 65)
    print('  MONITOR — TABELAS PEQUENAS')
    print(f'  {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 65)
    print(f'  [{_bar(pct, 30)}] {pct:.1f}%')
    print(f'  Concluídas: {done} / {total} | Falhas: {fail} | Pendentes: {s["pending"]}')

    if state['running_tables']:
        print(f'\n  Workers ativos ({len(state["running_tables"])}):')
        for rt in state['running_tables']:
            dest    = rt['dest']
            db_path = master_db.parent / f'migration_state_{dest}.db'
            p       = _read_progress(db_path)
            if p:
                m   = p.get('rows_migrated', 0)
                t   = p.get('total_rows', 0)
                pct_w = _calc_pct(p)
                print(f'    {dest:<30} [{_bar(pct_w, 15)}] {pct_w:5.1f}%  '
                      f'{m:>8,}/{t:>8,}')
            else:
                print(f'    {dest:<30} INICIANDO')

    if state['recent_failed']:
        print(f'\n  Falhas:')
        for f in state['recent_failed']:
            print(f'    ✗ {f["source"]}: {(f.get("error") or "")[:60]}')


def display_small_tables_live(master_db: Path, interval: float = 2.0):
    """Painel ao vivo para a migração das tabelas pequenas."""
    if not master_db.exists():
        print(f'Master state não encontrado: {master_db}')
        print('Inicie a migração com: python migrator_smalltables.py --small-tables')
        return

    if HAS_RICH:
        from rich.console import Console as RConsole
        from rich.live import Live
        console = RConsole()
        panel = _build_small_tables_panel(master_db, interval)
        with Live(panel, console=console, refresh_per_second=1/interval) as live:
            try:
                while True:
                    time.sleep(interval)
                    live.update(_build_small_tables_panel(master_db, interval))
            except KeyboardInterrupt:
                pass
    else:
        try:
            while True:
                import os as _os
                _os.system('cls' if _os.name == 'nt' else 'clear')
                _build_small_tables_plain(master_db)
                print('\nCtrl+C para sair')
                time.sleep(interval)
        except KeyboardInterrupt:
            pass


# ─── show_constraints ────────────────────────────────────────────────────────

def show_constraints(console=None):
    files = sorted(WORK_DIR.glob('constraint_state_*.json'))
    if not files:
        print('Nenhum arquivo constraint_state_*.json encontrado.')
        return
    for sf in files:
        with open(sf, encoding='utf-8') as f:
            data = json.load(f)
        if HAS_RICH and console:
            tbl = Table(title=sf.name, header_style='bold cyan')
            tbl.add_column('Tipo', style='bold')
            tbl.add_column('Nome')
            for obj in data:
                tbl.add_row(obj['obj_type'], obj['obj_name'])
            console.print(tbl)
            console.print(f'Total: {len(data)}\n')
        else:
            print(f'\n  {sf.name} ({len(data)} objetos)')
            for obj in data:
                print(f'    {obj["obj_type"]:<25} {obj["obj_name"]}')


# ─── show_summary_all ────────────────────────────────────────────────────────

def show_summary_all(console=None, only: set = None):
    """Resumo tabular de todas as migrações encontradas.
    Se `only` for fornecido, filtra apenas os DBs cujo nome estiver no conjunto.
    """
    dbs = _filter_dbs(_discover_dbs(), only) if only else _discover_dbs()
    if not dbs:
        print('Nenhum arquivo migration_state_*.db encontrado.')
        return

    if HAS_RICH and console:
        tbl = Table(title='Resumo de Todas as Migracoes', header_style='bold cyan',
                    box=box.ROUNDED, show_lines=True)
        tbl.add_column('Tabela',    style='bold')
        tbl.add_column('Status',    justify='center')
        tbl.add_column('Progresso', min_width=28)
        tbl.add_column('Linhas',    justify='right')
        tbl.add_column('Vel (l/s)', justify='right')
        tbl.add_column('ETA',       justify='right')
        tbl.add_column('Inicio',    justify='right', style='dim')

        for db_path in dbs:
            p = _read_progress(db_path)
            if not p:
                name = db_path.stem.removeprefix('migration_state_')
                tbl.add_row(name, Text('AGUARDANDO', style='dim'),
                            '-', '-', '-', '-', '-')
                continue
            m   = p.get('rows_migrated', 0)
            t   = p.get('total_rows', 0)
            pct = _calc_pct(p)
            status = p.get('status', 'idle')
            color  = _status_color(status)
            tbl.add_row(
                f'{p.get("source_table","?")} -> {p.get("dest_table","?")}',
                Text(status.upper(), style=f'bold {color}'),
                Text(f'{_bar(pct, 20)}  {pct:5.2f}%', style=color),
                f'{m:>12,} / {t:>12,}',
                f'{p.get("speed_rows_per_sec",0):>10,.0f}',
                _fd(p.get('eta_seconds', 0)),
                str(p.get('started_at',''))[:16],
            )
        console.print(tbl)
    else:
        print(f'  {"Tabela":<22} {"Status":<12} {"Progresso":>8}  {"Linhas":>22}')
        print('  ' + '-' * 72)
        for db_path in dbs:
            p = _read_progress(db_path)
            if not p:
                name = db_path.stem.removeprefix('migration_state_')
                print(f'  {name:<22} {"AGUARDANDO":<12}')
                continue
            m   = p.get('rows_migrated', 0)
            t   = p.get('total_rows', 0)
            pct = _calc_pct(p)
            print(f'  {p.get("dest_table","?"):<22} '
                  f'{p.get("status","?").upper():<12} '
                  f'{pct:6.2f}%  {m:>10,}/{t:>10,}')


# ─── CLI ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description='Monitor de Migracoes Firebird -> PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python monitor.py                                  # painel de todas as migracoes
  python monitor.py --big-tables                     # painel só das 10 tabelas grandes
  python monitor.py --small-tables                   # painel das 901 tabelas pequenas
  python monitor.py --state-db migration_state_operacao_credito.db  # detalhe unico
  python monitor.py --summary                        # resumo de todas
  python monitor.py --summary --big-tables           # resumo só das 10 grandes
  python monitor.py --history 50 --state-db X.db    # historico de batches
  python monitor.py --constraints                    # estado das constraints
  python monitor.py -i 5                             # refresh a cada 5s
        """)
    ap.add_argument('-s', '--state-db', default=None,
                    help='Banco de estado especifico (se omitido, detecta todos automaticamente)')
    ap.add_argument('--summary',    action='store_true',
                    help='Exibe resumo tabular (todas as tabelas ou a especificada por --state-db)')
    ap.add_argument('--json',       dest='json_out', action='store_true',
                    help='Saida JSON (requer --state-db)')
    ap.add_argument('-n', '--history', type=int, default=0,
                    help='Exibe historico de N batches (requer --state-db)')
    ap.add_argument('--constraints', action='store_true',
                    help='Exibe estado de todas as constraints')
    ap.add_argument('--reset',      action='store_true',
                    help='Reseta estado do banco (requer --state-db)')
    ap.add_argument('--big-tables', action='store_true',
                    help='Filtra exibição para as 10 tabelas grandes '
                         '(migradas pelo migrator.py). '
                         'Combinável com --summary.')
    ap.add_argument('--small-tables', action='store_true',
                    help='Painel para migração paralela das tabelas pequenas. '
                         'Lê migration_state_smalltables_master.db')
    ap.add_argument('--master-db', type=str, default=None,
                    help='Caminho do master state DB para --small-tables '
                         '(padrão: migration_state_smalltables_master.db)')
    ap.add_argument('-i', '--interval', type=float, default=2.0,
                    help='Intervalo de refresh em segundos (padrao: 2)')
    args = ap.parse_args()

    console = Console() if HAS_RICH else None
    only_big = BIG_TABLES if args.big_tables else None

    # ── Modo tabelas pequenas ────────────────────────────────
    if args.small_tables:
        master_db_path = Path(
            args.master_db if args.master_db
            else WORK_DIR / 'migration_state_smalltables_master.db'
        )
        display_small_tables_live(master_db_path, args.interval)
        return

    # Operações que exigem --state-db
    if args.json_out or args.history or args.reset:
        if not args.state_db:
            print('ERRO: --json, --history e --reset requerem --state-db')
            sys.exit(1)
        mon = MigrationMonitor(args.state_db)
        if args.reset:
            mon.reset()
        elif args.json_out:
            mon.show_json()
        elif args.history:
            mon.show_history(args.history)
        return

    # Constraints: sempre todas as tabelas
    if args.constraints:
        show_constraints(console)
        return

    # Resumo
    if args.summary:
        if args.state_db:
            MigrationMonitor(args.state_db).show_summary()
        else:
            show_summary_all(console, only=only_big)
        return

    # Modo ao vivo
    if args.state_db:
        # Detalhe de uma tabela específica
        MigrationMonitor(args.state_db).display_live(args.interval)
    else:
        # Painel multi-tabela (comportamento padrão, com filtro opcional)
        display_live_all(args.interval, only=only_big)


if __name__ == '__main__':
    main()
