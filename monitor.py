#!/usr/bin/env python3
"""
monitor.py
==========
Versão restaurada e atualizada do monitor clássico para o Maestro V2.
Acompanha progresso de Big Tables e Small Tables.

Uso:
    python monitor.py MIGRACAO_0005
    python monitor.py MIGRACAO_0005 --small-tables
    python monitor.py MIGRACAO_0005 --big-tables
"""

import sys
import os
import json
import time
import argparse
import sqlite3
import re
from datetime import datetime
from pathlib import Path

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.live import Live
    from rich.text import Text
    from rich import box
    from rich.console import Group
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

# ─── globais ajustáveis ──────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
SESSION_DIR = BASE_DIR
WORK_DIR = BASE_DIR / 'work'

# ─── helpers ────────────────────────────────────────────────────────────────

def _fd(s) -> str:
    if not s or s < 0: return 'N/A'
    s = int(s)
    h, r = divmod(s, 3600)
    m, s = divmod(r, 60)
    return f'{h}h{m:02d}m{s:02d}s' if h else (f'{m}m{s:02d}s' if m else f'{s}s')

def _bar(pct: float, width: int = 20) -> str:
    pct = max(0, min(100, pct))
    filled = int(width * pct / 100)
    return '█' * filled + '░' * (width - filled)

STATUS_COLOR = {
    'running':   'green',
    'completed': 'blue',
    'loaded':    'blue',
    'paused':    'yellow',
    'error':     'red',
    'idle':      'dim',
}

def _status_color(status: str) -> str:
    return STATUS_COLOR.get(status.lower(), 'white')

def _calc_pct(p: dict) -> float:
    if p.get('status', '').lower() in ('completed', 'loaded'):
        return 100.0
    m = p.get('rows_migrated', 0)
    t = p.get('total_rows', 0)
    return (m / t * 100) if t else 0

# ─── leitura de estado ───────────────────────────────────────────────────────

def _read_progress(db_path: Path, table_name: str = None, retries: int = 3) -> dict:
    """Lê progresso de um .db individual OU de uma entrada no migration.db."""
    if not db_path.exists(): return {}
    
    is_master = (db_path.name == 'migration.db')
    
    for attempt in range(retries):
        try:
            conn = sqlite3.connect(str(db_path), timeout=3)
            conn.execute('PRAGMA journal_mode=WAL')
            conn.row_factory = sqlite3.Row
            
            if is_master:
                if not table_name: return {}
                row = conn.execute("SELECT * FROM tables WHERE source_table = ?", (table_name.upper(),)).fetchone()
                if row:
                    return dict(row)
            else:
                row = conn.execute("SELECT progress_json FROM migration_state WHERE id=1").fetchone()
                if row:
                    return json.loads(row[0])
            conn.close()
        except Exception:
            if attempt == retries -1: return {}
            time.sleep(0.1)
    return {}

def _read_master_state(db_path: Path, retries: int = 3) -> dict:
    """Lê o estado de todas as tabelas do mestre (migration.db)."""
    result = {
        'summary': {'pending': 0, 'running': 0, 'completed': 0, 'failed': 0, 'total': 0},
        'running_tables': [],
        'recent_failed': [],
        'all_tables': []
    }
    if not db_path.exists(): return result
    for attempt in range(retries):
        try:
            conn = sqlite3.connect(str(db_path), timeout=5)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM tables").fetchall()
            for r in rows:
                d = dict(r)
                status = d['status'].lower()
                if status in result['summary']:
                    result['summary'][status] += 1
                elif status == 'loaded':
                    result['summary']['completed'] += 1

                result['summary']['total'] += 1
                result['all_tables'].append(d)

                if status == 'running':
                    result['running_tables'].append(d)
                if status in ('error', 'failed'):
                    result['recent_failed'].append(d)
            conn.close()
            return result
        except Exception:
            if attempt < retries - 1:
                time.sleep(0.2)
    return result

# ─── descoberta ─────────────────────────────────────────────────────────────

BIG_TABLES_LIST = [
    'DOCUMENTO_OPERACAO', 'LOG_EVENTOS', 'HISTORICO_OPERACAO', 
    'OCORRENCIA_SISAT', 'OCORRENCIA', 'NMOV', 'OPERACAO_CREDITO', 
    'PARCELASCTB', 'PESSOA_PRETENDENTE', 'CONTROLEVERSAO'
]

def _discover_sources(session_path: Path):
    """Retorna lista de (db_path, table_name_if_master)."""
    results = []
    
    # 1. Banco mestre
    master_db = session_path / 'migration.db'
    if master_db.exists():
        state = _read_master_state(master_db)
        for t in state['all_tables']:
            # Se for big table ou estiver ativa, incluímos como fonte primária
            if t['source_table'] in BIG_TABLES_LIST or t['status'] == 'running':
                results.append((master_db, t['source_table']))

    # 2. Arquivos de workers individuais (_t0, _t1...)
    # Buscamos na raiz da sessão e na subpasta work
    patterns = ['migration_state_*.db']
    found_files = []
    for p in patterns:
        found_files.extend(list(session_path.glob(p)))
        found_files.extend(list((session_path / 'work').glob(p)))
    
    for f in found_files:
        if f.name == 'migration.db': continue
        # Evita duplicar se já temos a agregada e o arquivo não é de partição
        name = f.stem.removeprefix('migration_state_').upper()
        is_partition = re.search(r'_T\d+$', name)
        results.append((f, None))
        
    return results

# ─── visualização ───────────────────────────────────────────────────────────

def _build_main_table(sources: list, title_prefix: str = "") -> Table:
    tbl = Table(
        title=f'  {title_prefix}  |  {datetime.now().strftime("%H:%M:%S")}',
        header_style='bold cyan',
        box=box.ROUNDED,
        show_lines=True,
        expand=True,
    )
    tbl.add_column('Tabela',    style='bold',  min_width=20)
    tbl.add_column('Status',    justify='center', min_width=10)
    tbl.add_column('Progresso', min_width=28)
    tbl.add_column('Linhas',    justify='right', min_width=22)
    tbl.add_column('Vel (l/s)', justify='right', min_width=10)
    tbl.add_column('ETA',       justify='right', min_width=10)
    tbl.add_column('Atualizado',justify='right', style='dim')

    seen = set()
    
    for db_path, t_name in sources:
        p = _read_progress(db_path, t_name)
        if not p: continue
        
        src = p.get('source_table', t_name or '?').upper()
        is_master = (db_path.name == 'migration.db')
        
        # Identificador único para evitar duplicatas simples (não partições)
        # Partições tem nomes de arquivo diferentes, então passam.
        entry_id = (src, db_path.name)
        if entry_id in seen: continue
        seen.add(entry_id)

        # Label customizado
        if is_master:
            label = f'[bold yellow]Σ[/bold yellow] {src}'
        else:
            # Se for partição, extrai o tX
            m = re.search(r'_t(\d+)\.db$', db_path.name.lower())
            suffix = f' [dim]↳ T{m.group(1)}[/dim]' if m else ""
            label = f'{src}{suffix}'

        status = p.get('status', 'idle').lower()
        color  = _status_color(status)
        
        m = p.get('rows_migrated', 0)
        t = p.get('total_rows', 0)
        pct = _calc_pct(p)
        spd = p.get('speed_rows_per_sec', 0) or 0
        eta = p.get('eta_seconds', 0) or 0
        upd = str(p.get('updated_at', ''))[11:19]

        tbl.add_row(
            label,
            Text(status.upper(), style=f'bold {color}'),
            Text(f'{_bar(pct, 20)}  {pct:5.2f}%', style=color),
            f'{m:>12,} / {t:>12,}',
            f'{spd:>10,.0f}',
            _fd(eta),
            upd
        )
    return tbl

def display_small_tables(master_db: Path, interval: float):
    if not HAS_RICH:
        print("Rich não instalado. Use modo básico.")
        return

    console = Console()
    def build():
        state = _read_master_state(master_db)
        
        s = {'pending': 0, 'running': 0, 'completed': 0, 'failed': 0, 'total': 0}
        for t in state['all_tables']:
            if t['source_table'] in BIG_TABLES_LIST:
                continue
            
            st = t['status'].lower()
            if st in s:
                s[st] += 1
            elif st == 'loaded':
                s['completed'] += 1
            s['total'] += 1
            
        pct = (s['completed'] / s['total'] * 100) if s['total'] else 0
        
        header = Panel(
            f"[bold cyan]Progresso Global Tabelas Pequenas[/bold cyan]\n\n"
            f"[{_bar(pct, 40)}] {pct:.1f}%  ({s['completed']}/{s['total']})\n"
            f"Pendentes: {s['pending']} | Rodando: {s['running']} | Falhas: {s['failed']}",
            title="SCCI Maestro V2", border_style="cyan"
        )
        
        # Filtra para mostrar apenas o que está rodando ou falhou (Workers Ativos)
        active_sources = []
        for t in state['all_tables']:
            # No modo small-tables, ignoramos as big-tables neste painel
            if t['source_table'] in BIG_TABLES_LIST:
                continue
                
            if t['status'].lower() in ('running', 'error', 'failed'):
                active_sources.append((master_db, t['source_table']))
        
        # Ordena por nome para evitar que as linhas fiquem pulando de posição
        active_sources.sort(key=lambda x: x[1])

        tbl = _build_main_table(active_sources, "Workers Ativos (Migrando Agora)")
        return Group(header, tbl)

    with Live(build(), console=console, refresh_per_second=1/interval) as live:
        try:
            while True:
                time.sleep(interval)
                live.update(build())
        except KeyboardInterrupt:
            pass

# ─── CLI ────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) == 1:
        print("\n[!] Uso do Monitor SCCI Maestro V2:")
        print("\n  1. Para ver tudo da sessão 0005:")
        print("      python monitor.py MIGRACAO_0005")
        print("\n  2. Para ver especificamente o progresso das 900+ tabelas pequenas:")
        print("      python monitor.py MIGRACAO_0005 --small-tables")
        print("\n  3. Para focar apenas nas 10 Big Tables:")
        print("      python monitor.py MIGRACAO_0005 --big-tables\n")
        sys.exit(0)

    ap = argparse.ArgumentParser(description='Monitor SCCI Maestro V2')
    ap.add_argument('session', help='Pasta da sessão (ex: MIGRACAO_0005)')
    ap.add_argument('--big-tables', action='store_true', help='Foca nas 10 big tables')
    ap.add_argument('--small-tables', action='store_true', help='Modo progresso global small tables')
    ap.add_argument('-i', '--interval', type=float, default=2.0)
    args = ap.parse_args()

    session_path = Path(args.session)
    if not session_path.exists():
        print(f"Erro: Pasta {args.session} não encontrada.")
        sys.exit(1)

    master_db = session_path / 'migration.db'

    if args.small_tables:
        display_small_tables(master_db, args.interval)
        return

    # Modo Geral / Big Tables
    if HAS_RICH:
        console = Console()
        def build_general():
            sources = _discover_sources(session_path)
            if args.big_tables:
                sources = [s for s in sources if (s[1] in BIG_TABLES_LIST) or (s[0].name != 'migration.db' and any(b in s[0].name.upper() for b in BIG_TABLES_LIST))]
            
            # Ordenação: Master primeiro, depois nome
            sources.sort(key=lambda x: (0 if x[0].name == 'migration.db' else 1, x[1] or x[0].name))
            
            return _build_main_table(sources, f"Painel {args.session}")

        with Live(build_general(), console=console, refresh_per_second=1/args.interval) as live:
            try:
                while True:
                    time.sleep(args.interval)
                    live.update(build_general())
            except KeyboardInterrupt:
                pass
    else:
        print("Rich não instalado.")

if __name__ == '__main__':
    main()
