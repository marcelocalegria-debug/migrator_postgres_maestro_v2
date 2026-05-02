#!/usr/bin/env python3
"""
investigacao_consulta_sig.py
============================
Diagnóstico dos falsos positivos "Só FB" reportados pelo checksum em
consulta_sig (5 linhas) e consulta_sig_pret (3 linhas).

Hipótese: colunas PK são VARCHAR no PG com trailing spaces preservados da
migração (CHAR→VARCHAR). O modo de amostragem do checksum usa valores
stripados do FB no IN clause do PG, mas VARCHAR é sensível a espaços.

Uso:
    python investigacao_consulta_sig.py
    python investigacao_consulta_sig.py --config config.yaml
"""

import hashlib
import os
import sys
import argparse
from pathlib import Path

import yaml
import psycopg2

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

if os.name == "nt" and hasattr(os, "add_dll_directory"):
    try:
        os.add_dll_directory(os.path.abspath(os.path.dirname(__file__) or "."))
    except Exception:
        pass

import fdb

if os.name == "nt":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for _p in [
        os.path.join(script_dir, "fbclient.dll"),
        os.path.abspath("fbclient.dll"),
        r"C:\Program Files\Firebird\Firebird_3_0\fbclient.dll",
    ]:
        if os.path.exists(_p):
            try:
                fdb.load_api(_p)
                break
            except Exception:
                pass


# ─── Tabelas a investigar ────────────────────────────────────────────────────

TABELAS = [
    ("CONSULTA_SIG",      "consulta_sig"),
    ("CONSULTA_SIG_PRET", "consulta_sig_pret"),
]

# ─── Utilitários ─────────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def connect_fb(cfg: dict):
    fb = cfg["firebird"]
    charset_raw = fb.get("charset", "ISO8859_1")
    alias = {"win1252": "WIN1252", "iso8859-1": "ISO8859_1", "utf8": "UTF8", "utf-8": "UTF8"}
    charset = alias.get(charset_raw.lower(), charset_raw.upper())
    return fdb.connect(
        host=fb.get("host", "localhost"), port=fb.get("port", 3050),
        database=fb["database"], user=fb["user"], password=fb["password"],
        charset=charset,
    )


def connect_pg(cfg: dict):
    pg = cfg["postgresql"]
    return psycopg2.connect(
        host=pg.get("host", "localhost"), port=pg.get("port", 5432),
        database=pg["database"], user=pg["user"], password=pg["password"],
    )


def md5_of(data) -> str | None:
    if data is None:
        return None
    if hasattr(data, "read"):
        data = data.read()
    if isinstance(data, memoryview):
        data = bytes(data)
    if isinstance(data, str):
        data = data.encode("latin-1", errors="replace")
    return hashlib.md5(data).hexdigest()


def _strip(v):
    return v.rstrip() if isinstance(v, str) else v


def get_pk_cols_pg(conn_pg, table_name: str, schema: str = "public") -> list[str]:
    cur = conn_pg.cursor()
    cur.execute(
        """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
        """,
        (f"{schema}.{table_name}",),
    )
    return [r[0] for r in cur.fetchall()]


def get_col_types_pg(conn_pg, table_name: str, cols: list[str], schema: str = "public") -> dict:
    cur = conn_pg.cursor()
    cur.execute(
        """
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = ANY(%s)
        """,
        (schema, table_name, cols),
    )
    return {r[0]: {"type": r[1], "max_len": r[2]} for r in cur.fetchall()}


def get_te_cols_pg(conn_pg, table_name: str, schema: str = "public") -> list[str]:
    cur = conn_pg.cursor()
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND data_type = 'bytea'
        ORDER BY ordinal_position
        """,
        (schema, table_name),
    )
    return [r[0] for r in cur.fetchall()]


# ─── Diagnóstico por tabela ───────────────────────────────────────────────────

def investigar(conn_fb, conn_pg, fb_table: str, pg_table: str, schema: str = "public"):
    sep = "=" * 70
    print(f"\n{sep}")
    print(f"  TABELA: {pg_table}")
    print(sep)

    pk_cols = get_pk_cols_pg(conn_pg, pg_table, schema)
    te_cols = get_te_cols_pg(conn_pg, pg_table, schema)
    col_types = get_col_types_pg(conn_pg, pg_table, pk_cols, schema)

    print(f"  PK columns : {pk_cols}")
    print(f"  TE columns : {te_cols}")
    print()
    print("  Tipos das colunas PK no PostgreSQL:")
    for col, info in col_types.items():
        print(f"    {col}: {info['type']}"
              + (f"({info['max_len']})" if info["max_len"] else ""))

    # ── Contagens ─────────────────────────────────────────────────────────────
    cur_fb = conn_fb.cursor()
    cur_pg = conn_pg.cursor()

    cur_fb.execute(f"SELECT COUNT(*) FROM {fb_table}")
    cnt_fb = cur_fb.fetchone()[0]
    cur_pg.execute(f'SELECT COUNT(*) FROM {schema}."{pg_table}"')
    cnt_pg = cur_pg.fetchone()[0]

    print(f"\n  Contagem total: FB={cnt_fb}  PG={cnt_pg}  "
          + ("✓ OK" if cnt_fb == cnt_pg else "✗ DIFERENTE"))

    # ── Busca todos os PKs brutos do PG ──────────────────────────────────────
    pk_pg_q   = ", ".join(f'"{c}"' for c in pk_cols)
    pk_fb_q   = ", ".join(c.upper() for c in pk_cols)

    cur_pg.execute(f'SELECT {pk_pg_q} FROM {schema}."{pg_table}"')
    pg_raw_rows = cur_pg.fetchall()
    pg_stripped = {tuple(_strip(v) for v in r) for r in pg_raw_rows}

    # ── Busca todos os PKs brutos do FB ──────────────────────────────────────
    cur_fb.execute(f"SELECT {pk_fb_q} FROM {fb_table}")
    fb_raw_rows = cur_fb.fetchall()
    fb_stripped = {tuple(_strip(v) for v in r) for r in fb_raw_rows}

    # ── Comparação de PKs ─────────────────────────────────────────────────────
    only_in_fb  = fb_stripped - pg_stripped
    only_in_pg  = pg_stripped - fb_stripped
    common      = fb_stripped & pg_stripped

    print(f"\n  PKs (stripped):  FB={len(fb_stripped)}  PG={len(pg_stripped)}  "
          f"comum={len(common)}  só-FB={len(only_in_fb)}  só-PG={len(only_in_pg)}")

    if only_in_fb:
        print(f"\n  ⚠  PKs presentes no FB mas ausentes no PG (stripped):")
        for pk in sorted(only_in_fb):
            print(f"     {pk}")

        # Verifica se estão no PG sem strip (hipótese do trailing space)
        pg_unstripped_set = {tuple(r) for r in pg_raw_rows}
        found_with_spaces = []
        for fb_pk in only_in_fb:
            # Reconstrói o pk bruto do FB para comparar
            for fb_row in fb_raw_rows:
                fb_pk_raw = tuple(fb_row)
                if tuple(_strip(v) for v in fb_pk_raw) == fb_pk:
                    # Verifica se esse pk (sem strip) existe no PG raw
                    if fb_pk_raw in pg_unstripped_set:
                        found_with_spaces.append((fb_pk, fb_pk_raw))
                    else:
                        # Busca no PG com cada variante
                        for pg_row in pg_raw_rows:
                            if tuple(_strip(v) for v in pg_row) == fb_pk:
                                found_with_spaces.append((fb_pk, tuple(pg_row)))
                                break
                    break

        if found_with_spaces:
            print(f"\n  ✓ Esses PKs existem no PG com trailing spaces (confirma hipótese):")
            for stripped_pk, raw_pg_pk in found_with_spaces:
                print(f"     stripped  : {stripped_pk}")
                print(f"     raw no PG : {raw_pg_pk}")
                # Mostra quais valores têm espaços
                for i, (s, r) in enumerate(zip(stripped_pk, raw_pg_pk)):
                    if s != r:
                        col = pk_cols[i]
                        print(f"     → coluna '{col}': PG={repr(r)} vs FB-stripped={repr(s)}")
                print()
        else:
            print("  ✗ PKs não encontrados no PG mesmo sem strip — linhas genuinamente ausentes!")

    if only_in_pg:
        print(f"\n  ⚠  PKs presentes no PG mas ausentes no FB (stripped): {len(only_in_pg)}")
        for pk in sorted(only_in_pg):
            print(f"     {pk}")

    # ── Comparação do conteúdo te_* para linhas comuns ───────────────────────
    if not te_cols:
        print("\n  (sem colunas BYTEA para comparar conteúdo)")
        return

    print(f"\n  Comparando conteúdo de {te_cols} para {len(common)} linhas comuns...")

    # Monta dict PG: pk_stripped → {col: md5}
    te_pg_q = ", ".join(f'"{c}"' for c in te_cols)
    cur_pg.execute(f'SELECT {pk_pg_q}, {te_pg_q} FROM {schema}."{pg_table}"')
    pg_data: dict = {}
    for row in cur_pg.fetchall():
        pk = tuple(_strip(v) for v in row[:len(pk_cols)])
        pg_data[pk] = {col: md5_of(row[len(pk_cols) + i]) for i, col in enumerate(te_cols)}

    # Itera FB
    te_fb_q = ", ".join(c.upper() for c in te_cols)
    cur_fb.execute(f"SELECT {pk_fb_q}, {te_fb_q} FROM {fb_table}")

    stats = {col: {"ok": 0, "diff": 0, "fb_null": 0, "pg_null": 0, "both_null": 0}
             for col in te_cols}
    diffs = []

    for row in cur_fb.fetchall():
        pk = tuple(_strip(v) for v in row[:len(pk_cols)])
        if pk not in common:
            continue
        pg_row = pg_data.get(pk)
        for i, col in enumerate(te_cols):
            h_fb = md5_of(row[len(pk_cols) + i])
            h_pg = pg_row[col] if pg_row else None
            if h_fb is None and h_pg is None:
                stats[col]["both_null"] += 1
            elif h_fb is None:
                stats[col]["fb_null"] += 1
            elif h_pg is None:
                stats[col]["pg_null"] += 1
            elif h_fb == h_pg:
                stats[col]["ok"] += 1
            else:
                stats[col]["diff"] += 1
                diffs.append({"pk": pk, "col": col, "fb": h_fb, "pg": h_pg})

    print(f"\n  Resultado por coluna:")
    for col in te_cols:
        s = stats[col]
        ok_icon = "✓" if s["diff"] == 0 and s["pg_null"] == 0 else "✗"
        print(f"    {ok_icon} {col}: ok={s['ok']} diff={s['diff']} "
              f"fb_null={s['fb_null']} pg_null={s['pg_null']} both_null={s['both_null']}")

    if diffs:
        print(f"\n  ✗ {len(diffs)} linhas com conteúdo diferente (até 10 exibidas):")
        for d in diffs[:10]:
            print(f"     pk={d['pk']}  col={d['col']}")
            print(f"     FB: {d['fb']}")
            print(f"     PG: {d['pg']}")
    else:
        print("\n  ✓ Nenhuma divergência de conteúdo nas linhas comuns.")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Diagnóstico only_fb em consulta_sig")
    p.add_argument("--config", default="config.yaml")
    p.add_argument("--schema", default="public")
    args = p.parse_args()

    cfg = load_config(args.config)

    print("Conectando a Firebird e PostgreSQL...")
    conn_fb = connect_fb(cfg)
    conn_pg = connect_pg(cfg)
    print("Conectado.\n")

    try:
        for fb_table, pg_table in TABELAS:
            investigar(conn_fb, conn_pg, fb_table, pg_table, args.schema)
    finally:
        conn_fb.close()
        conn_pg.close()

    print("\n" + "=" * 70)
    print("  Diagnóstico concluído.")
    print("=" * 70)


if __name__ == "__main__":
    main()
