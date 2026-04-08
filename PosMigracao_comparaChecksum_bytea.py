#!/usr/bin/env python3
"""
PosMigracao_comparaChecksum_bytea.py
======================================
Compara o checksum MD5 de colunas BLOB binário (Firebird) vs BYTEA (PostgreSQL)
para as 10 tabelas da migração Fire2pg.

Estratégia:
  - Descobre automaticamente colunas BLOB SUB_TYPE 0 (binário) no Firebird
  - Descobre automaticamente colunas BYTEA no PostgreSQL
  - Para tabelas COM chave primária: comparação linha a linha (PK como chave de join)
  - Para tabelas SEM chave primária: comparação por contagens de não-nulos
  - Relatório final formatado com Rich

Uso:
    python PosMigracao_comparaChecksum_bytea.py
    python PosMigracao_comparaChecksum_bytea.py --config config.yaml
    python PosMigracao_comparaChecksum_bytea.py --table operacao_credito
"""

import os
import sys
import hashlib
import argparse
import threading
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
import psycopg2

# ── Carregamento do fbclient.dll (Windows) ────────────────────────────────────
if os.name == "nt" and hasattr(os, "add_dll_directory"):
    try:
        os.add_dll_directory(os.path.abspath(os.path.dirname(__file__) or "."))
    except Exception:
        pass

import fdb

if os.name == "nt":
    _fb_paths = [
        os.path.abspath(os.path.join(os.path.dirname(__file__) or ".", "fbclient.dll")),
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
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, SpinnerColumn
from rich import box
from rich.text import Text
from rich.rule import Rule
from rich.align import Align

# ─── Tabelas a verificar (Firebird MAIÚSCULAS → PostgreSQL minúsculas) ──────────
TABELAS = [
    ("CONTROLEVERSAO",     "controleversao"),
    ("LOG_EVENTOS",        "log_eventos"),
    ("HISTORICO_OPERACAO", "historico_operacao"),
    ("OCORRENCIA",         "ocorrencia"),
    ("PARCELASCTB",        "parcelasctb"),
    ("OPERACAO_CREDITO",   "operacao_credito"),
    ("PESSOA_PRETENDENTE", "pessoa_pretendente"),
    ("NMOV",               "nmov"),
    ("OCORRENCIA_SISAT",   "ocorrencia_sisat"),
    ("DOCUMENTO_OPERACAO", "documento_operacao"),
]

console = Console()

# ──────────────────────────────────────────────────────────────────────────────
# Conexão e configuração
# ──────────────────────────────────────────────────────────────────────────────

def load_config(path: str = "config.yaml") -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


# Mapa de nomes alternativos → nome oficial Firebird
_CHARSET_ALIAS = {
    "iso-8859-1": "ISO8859_1",
    "iso8859-1":  "ISO8859_1",
    "latin1":     "ISO8859_1",
    "latin-1":    "ISO8859_1",
    "win1252":    "WIN1252",
    "windows-1252": "WIN1252",
    "utf-8":      "UTF8",
    "utf8":       "UTF8",
}


def _normalize_fb_charset(raw: str) -> str:
    return _CHARSET_ALIAS.get(raw.lower(), raw.upper())


def connect_fb(cfg: dict):
    fb = cfg["firebird"]
    charset = _normalize_fb_charset(fb.get("charset", "ISO8859_1"))
    return fdb.connect(
        host=fb.get("host", "localhost"),
        port=fb.get("port", 3050),
        database=fb["database"],
        user=fb["user"],
        password=fb["password"],
        charset=charset,
    )


def connect_pg(cfg: dict):
    pg = cfg["postgresql"]
    return psycopg2.connect(
        host=pg.get("host", "localhost"),
        port=pg.get("port", 5432),
        database=pg["database"],
        user=pg["user"],
        password=pg["password"],
    )


# ──────────────────────────────────────────────────────────────────────────────
# Descoberta de metadados
# ──────────────────────────────────────────────────────────────────────────────

def get_blob_binary_columns_fb(conn_fb, table_name: str) -> list[str]:
    """Retorna nomes (lowercase) de colunas BLOB SUB_TYPE 0 (binário) no Firebird."""
    cur = conn_fb.cursor()
    cur.execute(
        """
        SELECT TRIM(rf.RDB$FIELD_NAME)
        FROM RDB$RELATION_FIELDS rf
        JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = rf.RDB$FIELD_SOURCE
        WHERE TRIM(rf.RDB$RELATION_NAME) = ?
          AND f.RDB$FIELD_TYPE = 261
          AND (f.RDB$FIELD_SUB_TYPE = 0 OR f.RDB$FIELD_SUB_TYPE IS NULL)
        ORDER BY rf.RDB$FIELD_POSITION
        """,
        (table_name,),
    )
    return [row[0].strip().lower() for row in cur.fetchall()]


def get_bytea_columns_pg(conn_pg, table_name: str, schema: str = "public") -> list[str]:
    """Retorna nomes de colunas BYTEA no PostgreSQL."""
    cur = conn_pg.cursor()
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND data_type = 'bytea'
        ORDER BY ordinal_position
        """,
        (schema, table_name),
    )
    return [row[0] for row in cur.fetchall()]


def get_pk_columns_pg(conn_pg, table_name: str, schema: str = "public") -> list[str]:
    """Retorna colunas da PK da tabela no PostgreSQL (na ordem da chave)."""
    cur = conn_pg.cursor()
    cur.execute(
        """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a
          ON a.attrelid = i.indrelid
         AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass
          AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
        """,
        (f"{schema}.{table_name}",),
    )
    return [row[0] for row in cur.fetchall()]


def get_row_count(cursor, query: str) -> int:
    cursor.execute(query)
    return cursor.fetchone()[0]


# ──────────────────────────────────────────────────────────────────────────────
# Cálculo de hash
# ──────────────────────────────────────────────────────────────────────────────

def md5_of(data) -> str | None:
    """Calcula MD5 de bytes, memoryview, ou stream BLOB. Retorna None se vazio."""
    if data is None:
        return None
    if hasattr(data, "read"):          # stream fdb
        data = data.read()
    if isinstance(data, memoryview):
        data = bytes(data)
    return hashlib.md5(data).hexdigest()


# ──────────────────────────────────────────────────────────────────────────────
# Comparação de tabela COM chave primária
# ──────────────────────────────────────────────────────────────────────────────

def comparar_com_pk(
    conn_fb, conn_pg,
    fb_table: str, pg_table: str,
    pk_cols: list[str], blob_cols: list[str],
    schema: str = "public",
    progress=None, task=None,
) -> tuple[int, dict, list[dict]]:
    """
    Compara BLOB vs BYTEA linha a linha usando a PK como chave de join.

    Estratégia de memória eficiente:
      1. Carrega todos os hashes do PostgreSQL em um dict {pk_tuple: {col: md5}}
      2. Itera o Firebird e compara contra o dict

    Retorna: (total_linhas_comparadas, stats_por_coluna, lista_de_divergências)
    """
    cur_pg = conn_pg.cursor()
    cur_fb = conn_fb.cursor()

    pk_pg   = ", ".join(f'"{c}"' for c in pk_cols)
    blob_pg = ", ".join(f'"{c}"' for c in blob_cols)
    pk_fb   = ", ".join(c.upper() for c in pk_cols)
    blob_fb = ", ".join(c.upper() for c in blob_cols)

    # ── Fase 1: carregar hashes do PostgreSQL ─────────────────────────────────
    cur_pg.execute(f"SELECT {pk_pg}, {blob_pg} FROM {schema}.{pg_table}")
    pg_hashes: dict[tuple, dict[str, str | None]] = {}
    pk_len = len(pk_cols)
    for row in cur_pg:
        pk_val = tuple(row[:pk_len])
        pg_hashes[pk_val] = {
            col: md5_of(row[pk_len + i])
            for i, col in enumerate(blob_cols)
        }

    # ── Fase 2: iterar Firebird e comparar ────────────────────────────────────
    cur_fb.execute(f"SELECT {pk_fb}, {blob_fb} FROM {fb_table}")

    stats   = {col: {"ok": 0, "diff": 0, "only_fb": 0, "only_pg": 0, "both_null": 0}
               for col in blob_cols}
    errors  = []
    compared = 0

    for row in cur_fb:
        pk_val    = tuple(row[:pk_len])
        pg_row    = pg_hashes.pop(pk_val, None)
        compared += 1

        if progress and task is not None:
            progress.advance(task)

        for i, col in enumerate(blob_cols):
            hash_fb = md5_of(row[pk_len + i])
            hash_pg = pg_row[col] if pg_row is not None else None

            if pg_row is None:
                stats[col]["only_fb"] += 1
            elif hash_fb is None and hash_pg is None:
                stats[col]["both_null"] += 1
            elif hash_fb == hash_pg:
                stats[col]["ok"] += 1
            else:
                stats[col]["diff"] += 1
                if len(errors) < 20:
                    errors.append({"col": col, "pk": pk_val,
                                   "hash_fb": hash_fb, "hash_pg": hash_pg})

    # Linhas que estão apenas no PG (sobraram no dict)
    for pk_val, pg_row in pg_hashes.items():
        for col in blob_cols:
            if pg_row[col] is not None:
                stats[col]["only_pg"] += 1

    return compared, stats, errors


# ──────────────────────────────────────────────────────────────────────────────
# Comparação de tabela SEM chave primária (apenas contagens)
# ──────────────────────────────────────────────────────────────────────────────

def comparar_sem_pk(
    conn_fb, conn_pg,
    fb_table: str, pg_table: str,
    blob_cols: list[str],
    schema: str = "public",
) -> dict:
    """Para tabelas sem PK, compara contagens de BLOBs não-nulos."""
    cur_fb = conn_fb.cursor()
    cur_pg = conn_pg.cursor()
    stats = {}
    for col in blob_cols:
        cur_fb.execute(
            f"SELECT COUNT(*) FROM {fb_table} WHERE {col.upper()} IS NOT NULL"
        )
        count_fb = cur_fb.fetchone()[0]

        cur_pg.execute(
            f'SELECT COUNT(*) FROM {schema}.{pg_table} WHERE "{col}" IS NOT NULL'
        )
        count_pg = cur_pg.fetchone()[0]

        stats[col] = {
            "count_fb": count_fb,
            "count_pg": count_pg,
            "match": count_fb == count_pg,
        }
    return stats


# ──────────────────────────────────────────────────────────────────────────────
# Worker para execução paralela (thread-safe — conexões próprias)
# ──────────────────────────────────────────────────────────────────────────────

def _run_one_table(cfg: dict, fb_table: str, pg_table: str, schema: str) -> dict:
    """Executa toda a verificação de checksum para uma tabela.
    Thread-safe: cria e fecha suas próprias conexões FB e PG.
    """
    t0 = datetime.now()
    result: dict = {
        "tabela": pg_table, "fb_table": fb_table, "ok": False,
        "n_colunas": 0, "linhas": None, "divergencias": 0,
        "pk_cols": [], "blob_cols": [], "stats": None, "errors": [],
        "elapsed": 0.0, "error": None,
    }
    try:
        conn_fb = connect_fb(cfg)
        conn_pg = connect_pg(cfg)
        try:
            blob_cols_fb  = get_blob_binary_columns_fb(conn_fb, fb_table)
            bytea_cols_pg = get_bytea_columns_pg(conn_pg, pg_table, schema)
            shared_cols   = [c for c in blob_cols_fb if c in bytea_cols_pg]
            result["blob_cols"] = shared_cols
            result["n_colunas"] = len(shared_cols)

            if not shared_cols:
                result["ok"]   = True
                result["nota"] = "sem colunas BLOB binário"
                return result

            pk_cols = get_pk_columns_pg(conn_pg, pg_table, schema)
            result["pk_cols"] = pk_cols

            if pk_cols:
                total, stats, errors = comparar_com_pk(
                    conn_fb, conn_pg, fb_table, pg_table,
                    pk_cols, shared_cols, schema,
                )
                n_divs = sum(s["diff"] + s["only_fb"] + s["only_pg"]
                             for s in stats.values())
                result.update(ok=n_divs == 0, linhas=total,
                              divergencias=n_divs, stats=stats, errors=errors)
            else:
                stats  = comparar_sem_pk(conn_fb, conn_pg, fb_table, pg_table,
                                         shared_cols, schema)
                n_divs = sum(0 if s["match"] else abs(s["count_fb"] - s["count_pg"])
                             for s in stats.values())
                result.update(ok=n_divs == 0, divergencias=n_divs, stats=stats)
        finally:
            conn_fb.close()
            conn_pg.close()
    except Exception as exc:
        result["error"]        = str(exc)
        result["ok"]           = False
        result["divergencias"] = -1
    result["elapsed"] = (datetime.now() - t0).total_seconds()
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Saída Rich
# ──────────────────────────────────────────────────────────────────────────────

def print_header():
    console.print()
    console.print(Panel.fit(
        "[bold cyan]Verificação Pós-Migração[/bold cyan]\n"
        "[dim]Checksum MD5 — BLOB Firebird vs BYTEA PostgreSQL[/dim]",
        border_style="cyan",
        padding=(1, 4),
    ))
    console.print(f"[dim]Início: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}[/dim]")
    console.print()


def print_table_result_with_pk(pg_table: str, pk_cols: list, blob_cols: list,
                                total: int, stats: dict, errors: list):
    """Exibe resultado de tabela com PK em formato de tabela Rich."""
    console.print(Rule(f"[bold]{pg_table}[/bold]  [dim](PK: {', '.join(pk_cols)})[/dim]"))

    t = Table(box=box.ROUNDED, show_header=True, header_style="bold magenta",
              padding=(0, 1), expand=False)
    t.add_column("Coluna BYTEA",   style="cyan",  no_wrap=True)
    t.add_column("OK",             style="green", justify="right")
    t.add_column("Divergentes",    style="red",   justify="right")
    t.add_column("Só FB",          style="yellow",justify="right")
    t.add_column("Só PG",          style="yellow",justify="right")
    t.add_column("Ambos NULL",     style="dim",   justify="right")
    t.add_column("Status",         justify="center")

    all_ok = True
    for col in blob_cols:
        s = stats[col]
        ok     = s["ok"]
        diff   = s["diff"]
        only_f = s["only_fb"]
        only_p = s["only_pg"]
        null_b = s["both_null"]
        col_ok = diff == 0 and only_f == 0 and only_p == 0

        status = "[green]✓ OK[/green]" if col_ok else "[red]✗ ERRO[/red]"
        if not col_ok:
            all_ok = False

        t.add_row(
            col,
            str(ok),
            str(diff)   if diff   else "[dim]0[/dim]",
            str(only_f) if only_f else "[dim]0[/dim]",
            str(only_p) if only_p else "[dim]0[/dim]",
            str(null_b) if null_b else "[dim]0[/dim]",
            status,
        )

    console.print(t)
    console.print(f"  [dim]Linhas comparadas: {total:,}[/dim]")

    if errors:
        console.print(f"\n  [red]Amostra de divergências ({len(errors)} de até 20):[/red]")
        for e in errors[:5]:
            console.print(
                f"    [dim]col=[/dim][cyan]{e['col']}[/cyan]  "
                f"[dim]pk=[/dim]{e['pk']}\n"
                f"    [dim]FB:[/dim] {e['hash_fb']}\n"
                f"    [dim]PG:[/dim] {e['hash_pg']}"
            )
    console.print()
    return all_ok


def print_table_result_no_pk(pg_table: str, blob_cols: list, stats: dict):
    """Exibe resultado de tabela sem PK (apenas contagens)."""
    console.print(Rule(f"[bold]{pg_table}[/bold]  [dim](sem PK — comparação por contagem)[/dim]"))

    t = Table(box=box.ROUNDED, show_header=True, header_style="bold magenta",
              padding=(0, 1))
    t.add_column("Coluna BYTEA", style="cyan", no_wrap=True)
    t.add_column("Qtd FB",       justify="right")
    t.add_column("Qtd PG",       justify="right")
    t.add_column("Status",       justify="center")

    all_ok = True
    for col in blob_cols:
        s = stats[col]
        match  = s["match"]
        status = "[green]✓ OK[/green]" if match else "[red]✗ DIVERGENTE[/red]"
        if not match:
            all_ok = False
        t.add_row(col, f"{s['count_fb']:,}", f"{s['count_pg']:,}", status)

    console.print(t)
    console.print()
    return all_ok


def print_final_summary(results: list[dict], elapsed: float):
    """Exibe painel de resumo final."""
    console.print(Rule("[bold]Resumo Final[/bold]"))
    console.print()

    t = Table(box=box.DOUBLE_EDGE, show_header=True,
              header_style="bold white on dark_blue", padding=(0, 2), expand=True)
    t.add_column("Tabela",           style="cyan",   no_wrap=True)
    t.add_column("Colunas BYTEA",    justify="right")
    t.add_column("Linhas verif.",     justify="right")
    t.add_column("Divergências",     justify="right")
    t.add_column("Resultado",        justify="center")

    total_ok   = 0
    total_fail = 0

    for r in results:
        ok_icon = "[green]✓ OK[/green]" if r["ok"] else "[red]✗ FALHOU[/red]"
        divs    = str(r.get("divergencias", "—"))
        rows    = f"{r.get('linhas', 0):,}" if r.get("linhas") else "—"

        if r["ok"]:
            total_ok += 1
        else:
            total_fail += 1

        t.add_row(
            r["tabela"],
            str(r["n_colunas"]),
            rows,
            f"[red]{divs}[/red]" if r.get("divergencias") else "[dim]0[/dim]",
            ok_icon,
        )

    console.print(t)
    console.print()

    # Painel de conclusão
    if total_fail == 0:
        conclusion = (
            f"[bold green]Todas as {total_ok} tabelas passaram na verificação.[/bold green]\n"
            f"[dim]Integridade dos dados BYTEA confirmada.[/dim]"
        )
        border = "green"
    else:
        conclusion = (
            f"[bold red]{total_fail} tabela(s) com divergências![/bold red]  "
            f"[green]{total_ok} OK[/green]\n"
            f"[dim]Revise os erros acima antes de liberar o ambiente.[/dim]"
        )
        border = "red"

    console.print(Panel(
        Align.center(conclusion),
        border_style=border,
        padding=(1, 4),
    ))
    console.print(f"\n[dim]Tempo total: {elapsed:.1f}s  |  "
                  f"Fim: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}[/dim]\n")


# ──────────────────────────────────────────────────────────────────────────────
# Programa principal
# ──────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Verifica checksums BLOB vs BYTEA após migração")
    p.add_argument("--config", default="config.yaml", help="Arquivo de configuração")
    p.add_argument("--table",  default=None,
                   help="Processar apenas esta tabela (nome Firebird ou PostgreSQL, ex: OPERACAO_CREDITO ou operacao_credito)")
    p.add_argument("--schema",  default="public", help="Schema PostgreSQL (padrão: public)")
    p.add_argument("--workers", type=int, default=10,
                   help="Tabelas em paralelo (padrão: 10 = todas simultâneas; use 1 para sequencial)")
    return p.parse_args()


def main():
    args   = parse_args()
    cfg    = load_config(args.config)
    schema = args.schema

    print_header()

    t_inicio = datetime.now()

    # Filtra tabelas se --table especificado
    tabelas = TABELAS
    if args.table:
        needle = args.table.strip()
        tabelas = [
            (fb, pg) for fb, pg in TABELAS
            if pg.lower() == needle.lower() or fb.upper() == needle.upper()
        ]
        if not tabelas:
            nomes = ", ".join(f"{fb}/{pg}" for fb, pg in TABELAS)
            console.print(f"[red]Tabela não encontrada na lista: {needle}[/red]")
            console.print(f"[dim]Tabelas disponíveis (Firebird/PostgreSQL): {nomes}[/dim]")
            sys.exit(1)

    n_workers = min(args.workers, len(tabelas))

    # ── Modo sequencial (tabela única ou --workers 1) ─────────────────────────
    if n_workers <= 1:
        try:
            conn_fb = connect_fb(cfg)
            conn_pg = connect_pg(cfg)
        except Exception as e:
            console.print(f"[bold red]Erro ao conectar:[/bold red] {e}")
            sys.exit(1)

        results = []

        for fb_table, pg_table in tabelas:
            console.print(f"[dim]Analisando[/dim] [bold]{pg_table}[/bold]...", end=" ")

            blob_cols_fb  = get_blob_binary_columns_fb(conn_fb, fb_table)
            bytea_cols_pg = get_bytea_columns_pg(conn_pg, pg_table, schema)
            shared_cols   = [c for c in blob_cols_fb if c in bytea_cols_pg]

            if not shared_cols:
                console.print(
                    f"[yellow]nenhuma coluna BLOB binário↔BYTEA encontrada "
                    f"(FB={blob_cols_fb}, PG={bytea_cols_pg})[/yellow]"
                )
                results.append({
                    "tabela": pg_table, "ok": True,
                    "n_colunas": 0, "linhas": 0, "divergencias": 0,
                    "nota": "sem colunas BLOB binário",
                })
                continue

            console.print(f"[dim]colunas: {shared_cols}[/dim]")

            pk_cols = get_pk_columns_pg(conn_pg, pg_table, schema)
            n_divs  = 0

            if pk_cols:
                cur_fb = conn_fb.cursor()
                cur_fb.execute(f"SELECT COUNT(*) FROM {fb_table}")
                total_fb = cur_fb.fetchone()[0]

                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=30),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TextColumn("[cyan]{task.completed:,}[/cyan]/[dim]{task.total:,}[/dim]"),
                    TimeElapsedColumn(),
                    console=console,
                    transient=True,
                ) as progress:
                    task = progress.add_task(
                        f"  Comparando {pg_table}", total=total_fb
                    )
                    total, stats, errors = comparar_com_pk(
                        conn_fb, conn_pg, fb_table, pg_table,
                        pk_cols, shared_cols, schema,
                        progress=progress, task=task,
                    )

                table_ok = print_table_result_with_pk(
                    pg_table, pk_cols, shared_cols, total, stats, errors
                )
                n_divs = sum(s["diff"] + s["only_fb"] + s["only_pg"]
                             for s in stats.values())
                results.append({
                    "tabela": pg_table, "ok": table_ok,
                    "n_colunas": len(shared_cols),
                    "linhas": total,
                    "divergencias": n_divs,
                })
            else:
                stats    = comparar_sem_pk(conn_fb, conn_pg, fb_table, pg_table,
                                           shared_cols, schema)
                table_ok = print_table_result_no_pk(pg_table, shared_cols, stats)
                n_divs   = sum(0 if s["match"] else abs(s["count_fb"] - s["count_pg"])
                               for s in stats.values())
                results.append({
                    "tabela": pg_table, "ok": table_ok,
                    "n_colunas": len(shared_cols),
                    "linhas": None,
                    "divergencias": n_divs,
                })

        conn_fb.close()
        conn_pg.close()

    # ── Modo paralelo: N workers, cada um com conexões próprias ───────────────
    else:
        console.print(
            f"[bold cyan]Modo paralelo:[/bold cyan] "
            f"{n_workers} tabelas simultâneas  "
            f"[dim]({len(tabelas)} tabelas no total)[/dim]\n"
        )

        ordered_results: list = [None] * len(tabelas)
        lock      = threading.Lock()
        completed = 0

        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = {
                pool.submit(_run_one_table, cfg, fb, pg, schema): (idx, pg)
                for idx, (fb, pg) in enumerate(tabelas)
            }

            for future in as_completed(futures):
                idx, pg_table = futures[future]
                try:
                    res = future.result()
                except Exception as exc:
                    res = {
                        "tabela": pg_table, "ok": False,
                        "n_colunas": 0, "linhas": None,
                        "divergencias": -1, "error": str(exc),
                        "pk_cols": [], "blob_cols": [],
                        "stats": None, "errors": [], "elapsed": 0.0,
                    }
                ordered_results[idx] = res
                with lock:
                    completed += 1
                    status  = "[green]✓[/green]" if res["ok"] else "[red]✗[/red]"
                    elapsed = res.get("elapsed", 0.0)
                    console.print(
                        f"  {status} [cyan]{pg_table}[/cyan]  "
                        f"[dim]{elapsed:.1f}s[/dim]  "
                        f"[dim]({completed}/{len(tabelas)})[/dim]"
                    )

        console.print()

        # Exibe detalhes de cada tabela na ordem original, depois coleta results
        results = []
        for res in ordered_results:
            pg_table  = res["tabela"]
            blob_cols = res.get("blob_cols") or []
            pk_cols   = res.get("pk_cols")   or []
            stats     = res.get("stats")
            errors    = res.get("errors")    or []

            if res.get("error"):
                console.print(
                    f"[red]✗ {pg_table}:[/red] [dim]{res['error']}[/dim]\n"
                )
                results.append({
                    "tabela": pg_table, "ok": False,
                    "n_colunas": 0, "linhas": None, "divergencias": -1,
                })
                continue

            if not blob_cols:
                console.print(
                    f"[dim]{pg_table}: nenhuma coluna BLOB binário↔BYTEA — ignorada[/dim]"
                )
                results.append({
                    "tabela": pg_table, "ok": True,
                    "n_colunas": 0, "linhas": 0, "divergencias": 0,
                    "nota": "sem colunas BLOB binário",
                })
                continue

            if pk_cols:
                table_ok = print_table_result_with_pk(
                    pg_table, pk_cols, blob_cols,
                    res["linhas"], stats, errors,
                )
            else:
                table_ok = print_table_result_no_pk(pg_table, blob_cols, stats)

            results.append({
                "tabela": pg_table, "ok": table_ok,
                "n_colunas": res["n_colunas"],
                "linhas": res.get("linhas"),
                "divergencias": res["divergencias"],
            })

    elapsed = (datetime.now() - t_inicio).total_seconds()
    print_final_summary(results, elapsed)

    # Exit code: 0 = tudo OK, 1 = divergências encontradas
    sys.exit(0 if all(r["ok"] for r in results) else 1)


if __name__ == "__main__":
    main()
