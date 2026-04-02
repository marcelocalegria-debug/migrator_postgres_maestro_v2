#!/usr/bin/env python3
"""
Repara constraint_state_*.json e enable_constraints_*.sql que foram gerados
com FKs compostas duplicadas (bug de produto cartesiano no JOIN kcu×ccu).

Usa o Firebird como fonte autoritativa das definições de FK.
"""

import json
import glob
import os
import re
import sys

import fdb

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ─── fbclient ───────────────────────────────────────────────────────────────

_fb_paths = [
    os.path.join(BASE_DIR, "fbclient.dll"),
    r"C:\Program Files\Firebird\Firebird_3_0\fbclient.dll",
    r"C:\Program Files\Firebird\Firebird_4_0\fbclient.dll",
    r"C:\Program Files\Firebird\Firebird_5_0\fbclient.dll",
    r"C:\Program Files\Firebird\Firebird_2_5\bin\fbclient.dll",
]
for _p in _fb_paths:
    if os.path.exists(_p):
        try:
            fdb.load_api(_p)
            break
        except Exception:
            pass

FB_PARAMS = dict(
    host="localhost",
    port=3050,
    database="/firebird/data/c6emb.fdb",
    user="SYSDBA",
    password="masterkey",
    charset="WIN1252",
)

REENABLE_ORDER = [
    'index', 'primary_key', 'unique', 'check',
    'foreign_key_own', 'foreign_key_child', 'trigger',
]

LABEL_MAP = {
    'index':             'Indexes explícitos',
    'primary_key':       'Primary Key',
    'unique':            'Unique Constraints',
    'check':             'Check Constraints',
    'foreign_key_own':   'Foreign Keys (próprias)',
    'foreign_key_child': 'Foreign Keys (tabelas filhas)',
    'trigger':           'Triggers (reabilitar)',
}

# ─── Firebird ────────────────────────────────────────────────────────────────

def get_firebird_fk_map() -> dict:
    """
    Retorna dict: fk_name_lower → {child_table, parent_table,
                                    child_cols[], parent_cols[],
                                    update_rule, delete_rule}
    Os nomes de tabela/coluna são convertidos para lowercase (padrão PG).
    """
    conn = fdb.connect(**FB_PARAMS)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                TRIM(rc.RDB$CONSTRAINT_NAME),
                TRIM(rc.RDB$RELATION_NAME),
                TRIM(ref2.RDB$RELATION_NAME),
                TRIM(seg_child.RDB$FIELD_NAME),
                TRIM(seg_parent.RDB$FIELD_NAME),
                TRIM(ref.RDB$UPDATE_RULE),
                TRIM(ref.RDB$DELETE_RULE),
                seg_child.RDB$FIELD_POSITION
            FROM RDB$RELATION_CONSTRAINTS rc,
                 RDB$REF_CONSTRAINTS ref,
                 RDB$RELATION_CONSTRAINTS ref2,
                 RDB$INDEX_SEGMENTS seg_child,
                 RDB$INDEX_SEGMENTS seg_parent
            WHERE rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
              AND ref.RDB$CONSTRAINT_NAME = rc.RDB$CONSTRAINT_NAME
              AND ref2.RDB$CONSTRAINT_NAME = ref.RDB$CONST_NAME_UQ
              AND seg_child.RDB$INDEX_NAME = rc.RDB$INDEX_NAME
              AND seg_parent.RDB$INDEX_NAME = ref2.RDB$INDEX_NAME
              AND seg_parent.RDB$FIELD_POSITION = seg_child.RDB$FIELD_POSITION
            ORDER BY rc.RDB$CONSTRAINT_NAME, seg_child.RDB$FIELD_POSITION
        """)
        rows = cur.fetchall()
    finally:
        conn.close()

    fk_map: dict = {}
    for fk_name, child_table, parent_table, child_col, parent_col, update_rule, delete_rule, _pos in rows:
        key = fk_name.strip().lower()
        if key not in fk_map:
            fk_map[key] = {
                'child_table':  child_table.strip().lower(),
                'parent_table': parent_table.strip().lower(),
                'child_cols':   [],
                'parent_cols':  [],
                'update_rule':  (update_rule or 'NO ACTION').strip(),
                'delete_rule':  (delete_rule or 'NO ACTION').strip(),
            }
        fk_map[key]['child_cols'].append(child_col.strip().lower())
        fk_map[key]['parent_cols'].append(parent_col.strip().lower())

    return fk_map


# ─── detecção e reparo ───────────────────────────────────────────────────────

def has_duplicate_cols(sql: str) -> bool:
    """Detecta se a linha FK tem colunas duplicadas."""
    m = re.search(r'FOREIGN KEY \(([^)]+)\)', sql)
    if not m:
        return False
    cols = [c.strip().strip('"') for c in m.group(1).split(',')]
    return len(cols) != len(set(cols))


def build_fk_sql(child_schema: str, child_table: str, conname: str,
                  child_cols: list, parent_schema: str, parent_table: str,
                  parent_cols: list, update_rule: str, delete_rule: str) -> str:
    child_sql  = ', '.join(f'"{c}"' for c in child_cols)
    parent_sql = ', '.join(f'"{c}"' for c in parent_cols)
    return (f'ALTER TABLE "{child_schema}"."{child_table}" '
            f'ADD CONSTRAINT "{conname}" '
            f'FOREIGN KEY ({child_sql}) '
            f'REFERENCES "{parent_schema}"."{parent_table}"({parent_sql}) '
            f'ON UPDATE {update_rule} ON DELETE {delete_rule};')


def extract_rules_from_sql(sql: str) -> tuple[str, str]:
    """Extrai ON UPDATE e ON DELETE do SQL original (fonte: PostgreSQL, autoritativa)."""
    m = re.search(r'ON UPDATE (\w+(?: \w+)?) ON DELETE (\w+(?: \w+)?)', sql)
    if m:
        return m.group(1), m.group(2)
    return 'NO ACTION', 'NO ACTION'


def repair_json(json_path: str, fk_map: dict) -> tuple[list, bool]:
    """
    Corrige as entradas FK com colunas duplicadas no state JSON.
    - Colunas child/parent: vem do Firebird (posicional, correto).
    - update_rule/delete_rule: preservado do SQL original do PostgreSQL (autoritativo).
    Retorna (data_corrigida, houve_mudanca).
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    changed = False
    for entry in data:
        if entry['obj_type'] not in ('foreign_key_child', 'foreign_key_own'):
            continue
        sql = entry['create_sql']
        if not has_duplicate_cols(sql):
            continue

        m_name  = re.search(r'ADD CONSTRAINT "([^"]+)"', sql)
        m_child = re.search(r'ALTER TABLE "([^"]+)"\."([^"]+)"', sql)
        m_ref   = re.search(r'REFERENCES "([^"]+)"\."([^"]+)"', sql)
        if not (m_name and m_child and m_ref):
            print(f'  WARN: nao foi possivel parsear: {sql[:100]}')
            continue

        conname        = m_name.group(1).lower()
        child_schema   = m_child.group(1)
        child_tbl_sql  = m_child.group(2)
        parent_schema  = m_ref.group(1)
        parent_tbl_sql = m_ref.group(2)

        # Regras: preservar do SQL original do PostgreSQL
        update_rule, delete_rule = extract_rules_from_sql(sql)

        if conname not in fk_map:
            print(f'  WARN: FK "{conname}" nao encontrada no Firebird -- pulando')
            continue

        info = fk_map[conname]
        new_sql = build_fk_sql(
            child_schema, child_tbl_sql, conname,
            info['child_cols'],
            parent_schema, parent_tbl_sql,
            info['parent_cols'],
            update_rule,
            delete_rule,
        )
        print(f'  CORRIGIDO: {conname}')
        print(f'    ANTES: {sql}')
        print(f'    AGORA: {new_sql}')
        entry['create_sql'] = new_sql
        changed = True

    if changed:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f'  Salvo {os.path.basename(json_path)}')

    return data, changed


def regenerate_enable_sql(sql_path: str, data: list, schema: str, table: str):
    """Regenera o enable script a partir do state JSON já corrigido."""
    lines = [
        '-- ===========================================================',
        f'-- REABILITAR CONSTRAINTS/ÍNDICES: {schema}.{table}',
        '-- Gerado automaticamente pelo migrator',
        '-- ===========================================================',
        '',
        'BEGIN;',
        '',
    ]
    count = 0
    for obj_type in REENABLE_ORDER:
        group = [o for o in data if o['obj_type'] == obj_type]
        if not group:
            continue
        lines.append(f'-- {LABEL_MAP[obj_type]}')
        for obj in group:
            lines.append(obj['create_sql'])
            count += 1
        lines.append('')

    lines += [
        '-- Restaurar configurações',
        'SET synchronous_commit = on;',
        'SET jit = on;',
        '',
        '-- Atualizar estatísticas e reindexar',
        f'ANALYZE "{schema}"."{table}";',
        f'REINDEX TABLE "{schema}"."{table}";',
        '',
        'COMMIT;',
        '',
        f'-- Total: {count} objetos recriados',
    ]

    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    print(f'  Regenerado {os.path.basename(sql_path)}')


# ─── main ────────────────────────────────────────────────────────────────────

def main():
    print('Conectando ao Firebird para obter definições de FK...')
    try:
        fk_map = get_firebird_fk_map()
    except Exception as e:
        print(f'ERRO ao conectar ao Firebird: {e}')
        sys.exit(1)
    print(f'  {len(fk_map)} FKs encontradas no Firebird\n')

    json_files = sorted(glob.glob(os.path.join(BASE_DIR, 'constraint_state_*.json')))
    if not json_files:
        print('Nenhum arquivo constraint_state_*.json encontrado.')
        sys.exit(0)

    total_fixed = 0
    for json_path in json_files:
        basename = os.path.basename(json_path)
        table = basename.removeprefix('constraint_state_').removesuffix('.json')
        sql_path = os.path.join(BASE_DIR, f'enable_constraints_{table}.sql')

        print(f'Processando {basename}...')
        data, changed = repair_json(json_path, fk_map)

        if changed:
            total_fixed += 1
            if os.path.exists(sql_path):
                regenerate_enable_sql(sql_path, data, 'public', table)
            else:
                print(f'  WARN: {os.path.basename(sql_path)} não encontrado — apenas o JSON foi corrigido')
        else:
            print('  Sem duplicatas — nenhuma alteração necessária')
        print()

    print(f'Concluído. {total_fixed} arquivo(s) corrigido(s).')


if __name__ == '__main__':
    main()
