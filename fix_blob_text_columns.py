#!/usr/bin/env python3
"""
fix_blob_text_columns.py
========================
Gera script SQL para corrigir colunas BLOB SUB_TYPE 0 do Firebird que contêm
texto mas foram mapeadas como bytea no PostgreSQL.

Conecta ao Firebird para listar todas as colunas BLOB sub_type 0 de tabelas
de usuário, exclui as genuinamente binárias (IM_*, DADO, TOKEN, etc.) e gera
ALTER TABLE ... TYPE text USING convert_from(col, 'LATIN1') para as demais.

Uso:
    python fix_blob_text_columns.py                    # gera fix_blob_to_text.sql
    python fix_blob_text_columns.py --output outro.sql # nome customizado
    python fix_blob_text_columns.py --dry-run          # só mostra no console
"""

import argparse
import sys
import os

import yaml

# ── Carregamento do fbclient.dll (Windows) ────────────────────────────────────
if os.name == "nt" and hasattr(os, "add_dll_directory"):
    try:
        os.add_dll_directory(os.path.abspath(os.path.dirname(__file__) or "."))
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
    _loaded = False
    _errors = []
    for _p in _fb_paths:
        if os.path.exists(_p):
            try:
                fdb.load_api(_p)
                _loaded = True
                break
            except Exception as e:
                _errors.append(f"Erro ao carregar de {_p}: {e}")
    
    if not _loaded:
        # Tenta carregar sem caminho (pode estar no PATH)
        try:
            fdb.load_api("fbclient.dll")
            _loaded = True
        except Exception as e:
            _errors.append(f"Erro ao carregar 'fbclient.dll' do PATH: {e}")

    # Se ainda não carregou e estamos no Windows, fdb.connect vai falhar depois.
    # Não vamos printar tudo agora para não sujar o log se funcionar via fdb.connect (improvável)
    # Mas se houver erro depois, o usuário verá a exceção do fdb.

# ── Colunas genuinamente binárias — NÃO converter para text ──────────
BINARY_COLUMNS = {
    # (tabela, coluna) — colunas de imagem, dados binários brutos, tokens
    ('CONTROLEVERSAO', 'DADO'),
    ('CONTROLEVERSAO', 'TE_IMAGEM_REDUZIDA'),
    ('EMAIL_A_ENVIAR', 'TE_IMAGEM_REDUZIDA'),
    ('GRUPO_TIPO_OPERACAO', 'IM_GRUPO_TIPO_OPERACAO'),
    ('IMAGEM_DOCUMENTO_RGI', 'IM_PAGINA_RGI'),
    ('SCCI_SESSION', 'TOKEN'),
    ('SCCI_SESSION', 'REFRESH_TOKEN'),
    ('SEGURA', 'IM_SEGURADORA'),
    ('SEGURA', 'IM_SEGURADORA_MINI'),
    ('SIMULACAO_ORIGINACAO', 'IM_ENQUADRAMENTO'),
}

# Prefixos que indicam coluna binária (imagem)
BINARY_PREFIXES = ('IM_', 'IMAGEM')


def is_binary_column(table: str, column: str) -> bool:
    """Retorna True se a coluna é genuinamente binária."""
    if (table.strip(), column.strip()) in BINARY_COLUMNS:
        return True
    col = column.strip()
    for prefix in BINARY_PREFIXES:
        if col.startswith(prefix):
            return True
    return False


def get_blob_columns_from_firebird(config: dict) -> list:
    """Consulta Firebird e retorna lista de (tabela, coluna) BLOB sub_type 0."""
    import fdb

    fb_cfg = config['firebird']
    conn = fdb.connect(
        host=fb_cfg['host'],
        port=fb_cfg.get('port', 3050),
        database=fb_cfg['database'],
        user=fb_cfg['user'],
        password=fb_cfg['password'],
        charset=fb_cfg.get('charset', 'WIN1252'),
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT rf.RDB$RELATION_NAME, rf.RDB$FIELD_NAME
        FROM RDB$RELATION_FIELDS rf
        JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
        JOIN RDB$RELATIONS r ON r.RDB$RELATION_NAME = rf.RDB$RELATION_NAME
        WHERE f.RDB$FIELD_TYPE = 261
          AND f.RDB$FIELD_SUB_TYPE = 0
          AND r.RDB$SYSTEM_FLAG = 0
          AND r.RDB$VIEW_BLR IS NULL
        ORDER BY rf.RDB$RELATION_NAME, rf.RDB$FIELD_NAME
    """)
    rows = [(r[0].strip(), r[1].strip()) for r in cur.fetchall()]
    conn.close()
    return rows


def generate_sql(columns: list, schema: str = 'public') -> str:
    """Gera script SQL com ALTER TABLE para converter bytea → text."""
    lines = []
    lines.append('-- ==========================================================')
    lines.append('-- fix_blob_to_text.sql')
    lines.append('-- Corrige colunas BLOB SUB_TYPE 0 que contêm texto')
    lines.append('-- Gerado por fix_blob_text_columns.py')
    lines.append('-- ==========================================================')
    lines.append('')
    lines.append('-- IMPORTANTE: Executar com superuser ou owner das tabelas.')
    lines.append('-- Cada ALTER roda em sua própria transação (autocommit).')
    lines.append('-- Se uma falhar, as demais continuam.')
    lines.append('')

    text_cols = [(t, c) for t, c in columns if not is_binary_column(t, c)]
    binary_cols = [(t, c) for t, c in columns if is_binary_column(t, c)]

    lines.append(f'-- Total BLOB sub_type 0: {len(columns)}')
    lines.append(f'-- Converter para text: {len(text_cols)}')
    lines.append(f'-- Manter como bytea: {len(binary_cols)}')
    lines.append('')

    if binary_cols:
        lines.append('-- Colunas mantidas como bytea (genuinamente binárias):')
        for t, c in binary_cols:
            lines.append(f'--   {t.lower()}.{c.lower()}')
        lines.append('')

    # Agrupar por tabela
    from collections import defaultdict
    by_table = defaultdict(list)
    for t, c in text_cols:
        by_table[t].append(c)

    for table in sorted(by_table.keys()):
        cols = sorted(by_table[table])
        tbl_lower = table.lower()
        lines.append(f'-- {tbl_lower} ({len(cols)} colunas)')
        for col in cols:
            col_lower = col.lower()
            lines.append(
                f'ALTER TABLE "{schema}"."{tbl_lower}" '
                f'ALTER COLUMN "{col_lower}" TYPE text '
                f'USING convert_from("{col_lower}", \'LATIN1\');'
            )
        lines.append('')

    lines.append('-- Fim do script')
    lines.append(f'-- {len(text_cols)} colunas convertidas de bytea para text')
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(
        description='Gera SQL para corrigir BLOB sub_type 0 → text no PostgreSQL')
    parser.add_argument('-c', '--config', default='config_smalltables.yaml',
                        help='Arquivo de configuração YAML')
    parser.add_argument('-o', '--output', default='fix_blob_to_text.sql',
                        help='Arquivo SQL de saída')
    parser.add_argument('--dry-run', action='store_true',
                        help='Só mostra no console, não grava arquivo')
    parser.add_argument('--schema', default='public',
                        help='Schema do PostgreSQL')
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f'ERRO: {args.config} não encontrado.', file=sys.stderr)
        sys.exit(1)

    with open(args.config, encoding='utf-8') as f:
        config = yaml.safe_load(f)

    print(f'Conectando ao Firebird ({config["firebird"]["host"]})...')
    columns = get_blob_columns_from_firebird(config)
    print(f'Encontradas {len(columns)} colunas BLOB sub_type 0')

    text_count = sum(1 for t, c in columns if not is_binary_column(t, c))
    binary_count = len(columns) - text_count
    print(f'  -> {text_count} para converter (texto)')
    print(f'  -> {binary_count} para manter (binário)')

    sql = generate_sql(columns, args.schema)

    if args.dry_run:
        print('\n' + sql)
    else:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(sql)
        print(f'\nScript gerado: {args.output}')
        print(f'Executar com: psql -h host -p 5435 -U postgres -d c6_producao -f {args.output}')


if __name__ == '__main__':
    main()
