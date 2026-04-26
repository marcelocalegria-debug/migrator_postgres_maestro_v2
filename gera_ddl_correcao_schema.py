"""
gera_ddl_correcao_schema.py
============================
Gerador determinístico de DDL de correção de schema (FB → PG).
Chamado pelo Maestro (S03) quando diferenças são encontradas no compare_pre.

Saída: MIGRACAO_XXXX/sql/schema_correction_YYYYMMDD_HHMMSS.ddl

Exit codes:
    0 = DDL gerado com correções automáticas
    1 = Sem diferenças (schemas idênticos)
    2 = Apenas diferenças não-corrigíveis automaticamente (manuais)
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

import yaml

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import fdb
if os.name == "nt":
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    for _p in [
        os.path.join(_script_dir, "fbclient.dll"),
        os.path.abspath("fbclient.dll"),
        r"C:\Program Files\Firebird\Firebird_3_0\fbclient.dll",
        r"C:\Program Files\Firebird\Firebird_4_0\fbclient.dll",
        r"C:\Program Files\Firebird\Firebird_5_0\fbclient.dll",
        r"C:\Program Files (x86)\Firebird\Firebird_3_0\fbclient.dll",
    ]:
        if os.path.exists(_p):
            try:
                fdb.load_api(_p)
                break
            except Exception:
                pass

import psycopg2

_CHARSET_MAP = {
    'iso-8859-1': 'ISO8859_1', 'iso8859-1': 'ISO8859_1',
    'latin1': 'ISO8859_1', 'win1252': 'WIN1252',
    'windows-1252': 'WIN1252', 'cp1252': 'WIN1252',
    'utf-8': 'UTF8', 'utf8': 'UTF8',
}

_PG_CONFTYPE: Dict[str, str] = {
    'a': 'NO ACTION', 'r': 'RESTRICT', 'c': 'CASCADE',
    'n': 'SET NULL',  'd': 'SET DEFAULT',
}


# ─── Conexões ─────────────────────────────────────────────────────────────────

def _fb_connect(cfg: dict):
    c = cfg['firebird']
    charset = _CHARSET_MAP.get(c.get('charset', 'win1252').lower(), 'WIN1252')
    return fdb.connect(
        host=c['host'], port=c.get('port', 3050),
        database=c['database'],
        user=c['user'], password=c['password'],
        charset=charset,
    )


def _pg_connect(cfg: dict):
    c = cfg.get('postgresql') or cfg.get('postgres') or {}
    try:
        conn = psycopg2.connect(
            host=c['host'], port=c.get('port', 5432),
            database=c['database'],
            user=c['user'], password=c['password'],
        )
        conn.set_client_encoding('UTF8')
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as e:
        if "does not exist" in str(e):
            print(f"[WARNING] Banco '{c.get('database')}' não existe no PostgreSQL.")
            return None
        raise


# ─── Listagem de tabelas ──────────────────────────────────────────────────────

def _fb_tables(conn) -> list:
    cur = conn.cursor()
    cur.execute("""
        SELECT TRIM(r.RDB$RELATION_NAME)
        FROM RDB$RELATIONS r
        WHERE r.RDB$SYSTEM_FLAG = 0 AND r.RDB$VIEW_BLR IS NULL
        ORDER BY r.RDB$RELATION_NAME
    """)
    return [row[0] for row in cur.fetchall()]


def _pg_tables(conn, schema: str) -> list:
    if conn is None:
        return []
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """, (schema,))
    return [row[0] for row in cur.fetchall()]


# ─── FK helpers (com nomes de constraints) ────────────────────────────────────

def _fb_get_fk_full(conn, table: str) -> dict:
    """Retorna {conname: {local_cols, dest_table, dest_cols, del_rule, upd_rule}}"""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            TRIM(rc.RDB$CONSTRAINT_NAME),
            TRIM(sg.RDB$FIELD_NAME),
            TRIM(rc2.RDB$RELATION_NAME),
            TRIM(sg2.RDB$FIELD_NAME),
            TRIM(ref.RDB$DELETE_RULE),
            TRIM(ref.RDB$UPDATE_RULE)
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
    result = {}
    for row in cur.fetchall():
        conname, local_col, dest_table, dest_col, del_rule, upd_rule = row
        k = conname.lower()
        if k not in result:
            result[k] = {
                'local_cols': [],
                'dest_table': dest_table.lower(),
                'dest_cols': [],
                'del_rule': (del_rule or 'NO ACTION').strip().upper(),
                'upd_rule': (upd_rule or 'NO ACTION').strip().upper(),
            }
        result[k]['local_cols'].append(local_col.lower())
        result[k]['dest_cols'].append(dest_col.lower())
    return result


def _pg_get_fk_full(conn, schema: str, table: str) -> dict:
    """Retorna {conname: {local_cols, dest_table, dest_cols, del_rule, upd_rule}}"""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            con.conname,
            att1.attname,
            cls2.relname,
            att2.attname,
            con.confdeltype,
            con.confupdtype
        FROM pg_constraint con
        JOIN pg_class cls1 ON con.conrelid = cls1.oid
        JOIN pg_namespace nsp ON cls1.relnamespace = nsp.oid
        JOIN pg_class cls2 ON con.confrelid = cls2.oid
        CROSS JOIN LATERAL unnest(con.conkey, con.confkey) AS u(local_att, ref_att)
        JOIN pg_attribute att1 ON att1.attrelid = con.conrelid AND att1.attnum = u.local_att
        JOIN pg_attribute att2 ON att2.attrelid = con.confrelid AND att2.attnum = u.ref_att
        WHERE nsp.nspname = %s AND cls1.relname = %s AND con.contype = 'f'
        ORDER BY con.conname, u.local_att
    """, (schema, table))
    result = {}
    for row in cur.fetchall():
        conname, local_col, dest_table, dest_col, confdeltype, confupdtype = row
        if conname not in result:
            result[conname] = {
                'local_cols': [],
                'dest_table': dest_table.lower(),
                'dest_cols': [],
                'del_rule': _PG_CONFTYPE.get(confdeltype, 'NO ACTION'),
                'upd_rule': _PG_CONFTYPE.get(confupdtype, 'NO ACTION'),
            }
        result[conname]['local_cols'].append(local_col.lower())
        result[conname]['dest_cols'].append(dest_col.lower())
    return result


def _fk_sig(info: dict) -> tuple:
    """Assinatura canônica de FK para comparação entre FB e PG."""
    return (tuple(sorted(info['local_cols'])), info['dest_table'], tuple(sorted(info['dest_cols'])))


# ─── Index helpers (com nomes) ────────────────────────────────────────────────

def _fb_get_index_full(conn, table: str) -> dict:
    """Retorna {idx_name: {cols: list, is_unique: bool}} para índices sem constraints."""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            TRIM(i.RDB$INDEX_NAME),
            i.RDB$UNIQUE_FLAG,
            TRIM(sg.RDB$FIELD_NAME),
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
    result = {}
    for row in cur.fetchall():
        idx_name, is_unique, col_name, _pos = row
        k = idx_name.lower()
        if k not in result:
            result[k] = {'cols': [], 'is_unique': bool(is_unique)}
        result[k]['cols'].append(col_name.lower())
    return result


def _pg_get_index_full(conn, schema: str, table: str) -> dict:
    """Retorna {idx_name: {cols: list, is_unique: bool}} para índices sem constraints."""
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
        WHERE i.schemaname = %s AND i.tablename = %s
          AND NOT EXISTS (
              SELECT 1 FROM information_schema.table_constraints tc
              WHERE tc.table_schema = i.schemaname
                AND tc.table_name = i.tablename
                AND tc.constraint_name = i.indexname
          )
        ORDER BY i.indexname, array_position(ix.indkey::int[], a.attnum::int)
    """, (schema, table))
    result = {}
    for row in cur.fetchall():
        idx_name, is_unique, col_name = row
        if idx_name not in result:
            result[idx_name] = {'cols': [], 'is_unique': is_unique}
        result[idx_name]['cols'].append(col_name.lower())
    return result


def _idx_sig(info: dict) -> tuple:
    """Assinatura canônica de índice para comparação."""
    return (','.join(sorted(info['cols'])), info['is_unique'])


# ─── Geração de DDL ──────────────────────────────────────────────────────────

def _gen_fk_ddl(table: str, conname: str, info: dict) -> str:
    """Gera DROP + ADD CONSTRAINT para uma FK com as regras do Firebird."""
    local_cols = ', '.join(f'"{c}"' for c in info['local_cols'])
    dest_cols  = ', '.join(f'"{c}"' for c in info['dest_cols'])
    return (
        f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{conname}";\n'
        f'ALTER TABLE "{table}" ADD CONSTRAINT "{conname}"\n'
        f'  FOREIGN KEY ({local_cols}) REFERENCES "{info["dest_table"]}" ({dest_cols})\n'
        f'  ON DELETE {info["del_rule"]} ON UPDATE {info["upd_rule"]};'
    )


# ─── Lógica principal ─────────────────────────────────────────────────────────

def generate_ddl(cfg: dict, work_dir: Path, schema: str) -> int:
    """
    Compara FB vs PG e gera DDL de correção.

    Retorna exit code:
        0 = DDL gerado com correções automáticas
        1 = Sem diferenças (schemas idênticos)
        2 = Apenas diferenças manuais (sem DDL executável gerado)
    """
    print("Conectando ao Firebird...")
    fb_conn = _fb_connect(cfg)

    print("Conectando ao PostgreSQL...")
    pg_conn = _pg_connect(cfg)
    if pg_conn is None:
        return 2

    print("Listando tabelas...")
    fb_map = {t.lower(): t for t in _fb_tables(fb_conn)}
    pg_map = {t.lower(): t for t in _pg_tables(pg_conn, schema)}

    common_keys    = sorted(set(fb_map) & set(pg_map))
    only_fb_tables = sorted(set(fb_map) - set(pg_map))
    only_pg_tables = sorted(set(pg_map) - set(fb_map))

    print(f"Tabelas: {len(common_keys)} em comum | {len(only_fb_tables)} só FB | {len(only_pg_tables)} só PG")
    print("Analisando FKs e índices...")

    auto_stmts  = []
    manual_lines = []
    counts = {'fk_rules': 0, 'fk_add': 0, 'fk_drop': 0, 'idx_drop': 0, 'idx_add': 0}

    for i, key in enumerate(common_keys, 1):
        if i % 100 == 0:
            print(f"  [{i}/{len(common_keys)}] {key}")

        fb_name = fb_map[key]
        pg_name = pg_map[key]

        # ── FKs ──────────────────────────────────────────────────────
        try:
            fb_fks = _fb_get_fk_full(fb_conn, fb_name.upper())
            pg_fks = _pg_get_fk_full(pg_conn, schema, pg_name)

            fb_sig_map = {_fk_sig(v): (k, v) for k, v in fb_fks.items()}
            pg_sig_map = {_fk_sig(v): (k, v) for k, v in pg_fks.items()}

            # FK-RULES divergentes
            for sig in set(fb_sig_map) & set(pg_sig_map):
                _, fb_info = fb_sig_map[sig]
                pg_conname, pg_info = pg_sig_map[sig]
                if fb_info['del_rule'] != pg_info['del_rule'] or fb_info['upd_rule'] != pg_info['upd_rule']:
                    corrected = {**pg_info, 'del_rule': fb_info['del_rule'], 'upd_rule': fb_info['upd_rule']}
                    auto_stmts.append(
                        f"-- [FK-RULES] {pg_name}.{pg_conname}: "
                        f"ON DELETE {pg_info['del_rule']}→{fb_info['del_rule']}, "
                        f"ON UPDATE {pg_info['upd_rule']}→{fb_info['upd_rule']}\n"
                        + _gen_fk_ddl(pg_name, pg_conname, corrected)
                    )
                    counts['fk_rules'] += 1

            # FK só no FB → ADD
            for sig in set(fb_sig_map) - set(pg_sig_map):
                fb_conname, fb_info = fb_sig_map[sig]
                auto_stmts.append(
                    f"-- [FK só no FB] {pg_name}: adicionando {fb_conname.lower()}\n"
                    + _gen_fk_ddl(pg_name, fb_conname.lower(), fb_info)
                )
                counts['fk_add'] += 1

            # FK só no PG → DROP
            for sig in set(pg_sig_map) - set(fb_sig_map):
                pg_conname, _ = pg_sig_map[sig]
                auto_stmts.append(
                    f'-- [FK só no PG] {pg_name}: removendo {pg_conname} (não existe no FB)\n'
                    f'ALTER TABLE "{pg_name}" DROP CONSTRAINT IF EXISTS "{pg_conname}";'
                )
                counts['fk_drop'] += 1

        except Exception as e:
            manual_lines.append(f"-- ERRO ao comparar FKs de {pg_name}: {e}")

        # ── Índices ───────────────────────────────────────────────────
        try:
            fb_idx = _fb_get_index_full(fb_conn, fb_name.upper())
            pg_idx = _pg_get_index_full(pg_conn, schema, pg_name)

            fb_sig_map_idx = {_idx_sig(v): (k, v) for k, v in fb_idx.items()}
            pg_sig_map_idx = {_idx_sig(v): (k, v) for k, v in pg_idx.items()}

            # IDX só no PG → DROP
            for sig in set(pg_sig_map_idx) - set(fb_sig_map_idx):
                pg_idx_name, _ = pg_sig_map_idx[sig]
                auto_stmts.append(
                    f'-- [IDX só no PG] {pg_name}: removendo {pg_idx_name} (não existe no FB)\n'
                    f'DROP INDEX IF EXISTS "{pg_idx_name}";'
                )
                counts['idx_drop'] += 1

            # IDX só no FB → CREATE
            for sig in set(fb_sig_map_idx) - set(pg_sig_map_idx):
                fb_idx_name, fb_idx_info = fb_sig_map_idx[sig]
                cols_q = ', '.join(f'"{c}"' for c in fb_idx_info['cols'])
                unique_kw = "UNIQUE " if fb_idx_info['is_unique'] else ""
                pg_new_name = fb_idx_name.lower()
                auto_stmts.append(
                    f'-- [IDX só no FB] {pg_name}: criando {pg_new_name}\n'
                    f'CREATE {unique_kw}INDEX IF NOT EXISTS "{pg_new_name}" ON "{pg_name}" ({cols_q});'
                )
                counts['idx_add'] += 1

        except Exception as e:
            manual_lines.append(f"-- ERRO ao comparar índices de {pg_name}: {e}")

    fb_conn.close()
    pg_conn.close()

    # Tabelas só em um lado → manual
    for t in only_fb_tables:
        manual_lines.append(f"-- MANUAL: tabela '{t}' só existe no Firebird (ver schema.sql para DDL CREATE TABLE)")
    for t in only_pg_tables:
        manual_lines.append(f"-- MANUAL: tabela '{t}' só existe no PostgreSQL (verificar se deve ser removida)")

    total_auto   = sum(counts.values())
    total_manual = len(manual_lines)

    if total_auto == 0 and total_manual == 0:
        print("[OK] Nenhuma diferença encontrada — schemas idênticos.")
        return 1

    # ── Escreve arquivo DDL ───────────────────────────────────────────
    sql_dir = work_dir / "sql"
    sql_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ddl_path = sql_dir / f"schema_correction_{ts}.ddl"

    with open(ddl_path, 'w', encoding='utf-8') as f:
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"-- ============================================================\n")
        f.write(f"-- Schema Correction DDL — gerado em {now_str}\n")
        f.write(f"-- Migração: {work_dir.name}\n")
        f.write(f"-- FK-RULES: {counts['fk_rules']} | FK add: {counts['fk_add']} | FK drop: {counts['fk_drop']}\n")
        f.write(f"-- IDX add: {counts['idx_add']} | IDX drop: {counts['idx_drop']}\n")
        f.write(f"-- Itens manuais (comentados): {total_manual}\n")
        f.write(f"-- ============================================================\n\n")

        if auto_stmts:
            f.write("-- ==== CORREÇÕES AUTOMÁTICAS ====\n\n")
            for stmt in auto_stmts:
                f.write(stmt + "\n\n")

        if manual_lines:
            f.write("-- ==== ITENS PARA REVISÃO MANUAL ====\n\n")
            for line in manual_lines:
                f.write(line + "\n")

    print(f"\n[DDL] FK-RULES corrigidas: {counts['fk_rules']} | FK add: {counts['fk_add']} | FK drop: {counts['fk_drop']}")
    print(f"[DDL] IDX add: {counts['idx_add']} | IDX drop: {counts['idx_drop']}")
    print(f"[DDL] Itens manuais (comentados): {total_manual}")
    print(f"[DDL] Total de correções automáticas: {total_auto}")
    # Linha especial para o Maestro capturar o caminho do arquivo
    print(f"\nDDL_PATH:{ddl_path.absolute()}")

    return 0 if total_auto > 0 else 2


def main():
    parser = argparse.ArgumentParser(
        description='Gera DDL de correção determinístico para diferenças de schema FB→PG'
    )
    parser.add_argument('--work-dir', required=True, help='Diretório de trabalho (MIGRACAO_XXXX)')
    parser.add_argument('--config',   default=None,  help='Caminho do config.yaml (default: work-dir/config.yaml)')
    parser.add_argument('--schema',   default=None,  help='Schema PostgreSQL (default: lido do config ou "public")')
    args = parser.parse_args()

    work_dir = Path(args.work_dir)
    if not work_dir.exists():
        print(f"[ERROR] Diretório não encontrado: {work_dir}")
        sys.exit(2)

    config_path = Path(args.config) if args.config else work_dir / 'config.yaml'
    if not config_path.exists():
        print(f"[ERROR] Config não encontrado: {config_path}")
        sys.exit(2)

    with open(config_path, encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    pg_section = cfg.get('postgresql') or cfg.get('postgres') or {}
    schema = args.schema or pg_section.get('schema', 'public')

    sys.exit(generate_ddl(cfg, work_dir, schema))


if __name__ == '__main__':
    main()
