"""
pg_constraints.py
=================
Gerencia constraints, PKs, índices e triggers de uma tabela PostgreSQL
para otimizar cargas em massa.

Correções nesta versão:
  - FK query JOIN corrigido (constraint_schema em vez de unique_constraint_schema)
  - Removida referência a função inexistente _cfg_val
  - Ordem de re-enable correta incluindo UNIQUE
  - Autocommit gerenciado corretamente em disable_all/enable_all
  - ANALYZE após recriação de índices/PK
"""

import json
import logging
from dataclasses import dataclass, field
from typing import List, Optional

import psycopg2

logger = logging.getLogger(__name__)


@dataclass
class DroppedObject:
    """Um objeto que foi removido e precisa ser recriado."""
    obj_type: str          # primary_key, unique, foreign_key_own, foreign_key_child,
                           # check, index, trigger
    obj_name: str
    create_sql: str
    drop_sql: str


# Ordem de RE-CRIAÇÃO (respeita dependências)
REENABLE_ORDER = [
    'index',               # índices explícitos primeiro (aceleram FK validation)
    'primary_key',         # PK (cria índice implícito)
    'unique',              # unique constraints (criam índice implícito)
    'check',               # check constraints
    'foreign_key_own',     # FKs que a tabela destino referencia
    'foreign_key_child',   # FKs de outras tabelas que referenciam a destino
    'trigger',             # triggers por último
]


class ConstraintManager:
    """
    Descobre, remove e recria TODOS os objetos dependentes de uma tabela.

    Objetos gerenciados:
      - Foreign Keys de tabelas FILHAS que referenciam a tabela destino
      - Foreign Keys DA tabela destino (referenciando outras tabelas)
      - Check constraints
      - Unique constraints
      - Primary key
      - Índices explícitos (não-constraint)
      - Triggers do usuário
    """

    def __init__(self, pg_conn_params: dict, schema: str, table: str):
        self.pg_conn_params = pg_conn_params
        self.schema = schema
        self.table = table
        self.dropped_objects: List[DroppedObject] = []

    # ─── conexão ────────────────────────────────────────────

    def _connect(self):
        return psycopg2.connect(**self.pg_conn_params)

    def _run_query(self, cur, sql, params=None):
        """Executa query com log em caso de erro."""
        try:
            cur.execute(sql, params)
        except Exception as e:
            logger.error(f"SQL falhou: {sql[:120]}... -> {e}")
            raise

    # ─── consultas de metadados ─────────────────────────────

    def _q_foreign_keys_referencing_us(self, cur) -> list:
        """
        FKs em OUTRAS tabelas que apontam PARA a tabela destino.
        Usa pg_catalog com unnest posicional para garantir mapeamento
        correto entre colunas filho↔pai em FKs compostas (multi-coluna).
        A abordagem via information_schema causava produto cartesiano N²
        para FKs com N colunas.
        """
        self._run_query(cur, """
            SELECT
                c.conname                               AS constraint_name,
                n_child.nspname                         AS child_schema,
                t_child.relname                         AS child_table,
                a_child.attname                         AS child_column,
                a_ref.attname                           AS parent_column,
                CASE c.confupdtype
                    WHEN 'a' THEN 'NO ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                END                                     AS update_rule,
                CASE c.confdeltype
                    WHEN 'a' THEN 'NO ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                END                                     AS delete_rule,
                cols.ord                                AS ordinal_position
            FROM pg_constraint c
            JOIN pg_class      t_child  ON t_child.oid  = c.conrelid
            JOIN pg_namespace  n_child  ON n_child.oid  = t_child.relnamespace
            JOIN pg_class      t_ref    ON t_ref.oid    = c.confrelid
            JOIN pg_namespace  n_ref    ON n_ref.oid    = t_ref.relnamespace
            CROSS JOIN LATERAL unnest(c.conkey, c.confkey)
                               WITH ORDINALITY AS cols(child_attnum, ref_attnum, ord)
            JOIN pg_attribute  a_child  ON a_child.attrelid = c.conrelid
                                       AND a_child.attnum   = cols.child_attnum
            JOIN pg_attribute  a_ref    ON a_ref.attrelid   = c.confrelid
                                       AND a_ref.attnum     = cols.ref_attnum
            WHERE c.contype     = 'f'
              AND n_ref.nspname = %s
              AND t_ref.relname = %s
              AND (n_child.nspname != %s OR t_child.relname != %s)
            ORDER BY c.conname, cols.ord
        """, (self.schema, self.table, self.schema, self.table))
        return cur.fetchall()

    def _q_own_foreign_keys(self, cur) -> list:
        """
        FKs que a tabela destino possui (referenciando outras tabelas).
        Usa pg_catalog com unnest posicional para garantir mapeamento
        correto entre colunas filho↔pai em FKs compostas (multi-coluna).
        """
        self._run_query(cur, """
            SELECT
                c.conname                               AS constraint_name,
                a_child.attname                         AS child_column,
                n_ref.nspname                           AS ref_schema,
                t_ref.relname                           AS ref_table,
                a_ref.attname                           AS ref_column,
                CASE c.confupdtype
                    WHEN 'a' THEN 'NO ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                END                                     AS update_rule,
                CASE c.confdeltype
                    WHEN 'a' THEN 'NO ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                END                                     AS delete_rule,
                cols.ord                                AS ordinal_position
            FROM pg_constraint c
            JOIN pg_class      t_child  ON t_child.oid  = c.conrelid
            JOIN pg_namespace  n_child  ON n_child.oid  = t_child.relnamespace
            JOIN pg_class      t_ref    ON t_ref.oid    = c.confrelid
            JOIN pg_namespace  n_ref    ON n_ref.oid    = t_ref.relnamespace
            CROSS JOIN LATERAL unnest(c.conkey, c.confkey)
                               WITH ORDINALITY AS cols(child_attnum, ref_attnum, ord)
            JOIN pg_attribute  a_child  ON a_child.attrelid = c.conrelid
                                       AND a_child.attnum   = cols.child_attnum
            JOIN pg_attribute  a_ref    ON a_ref.attrelid   = c.confrelid
                                       AND a_ref.attnum     = cols.ref_attnum
            WHERE c.contype      = 'f'
              AND n_child.nspname = %s
              AND t_child.relname = %s
            ORDER BY c.conname, cols.ord
        """, (self.schema, self.table))
        return cur.fetchall()

    def _q_constraints_by_type(self, cur, contype: str) -> list:
        """Busca constraints por tipo: 'c'=check, 'u'=unique, 'p'=primary key."""
        self._run_query(cur, """
            SELECT conname, pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conrelid = %s::regclass
              AND contype  = %s
            ORDER BY conname
        """, (f'"{self.schema}"."{self.table}"', contype))
        return cur.fetchall()

    def _q_explicit_indexes(self, cur) -> list:
        """Índices criados explicitamente (não por constraint)."""
        self._run_query(cur, """
            SELECT i.relname, pg_get_indexdef(i.oid)
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = %s
              AND t.relname = %s
              AND NOT ix.indisprimary
              AND NOT EXISTS (
                  SELECT 1 FROM pg_constraint c
                  WHERE c.conindid = ix.indexrelid
              )
            ORDER BY i.relname
        """, (self.schema, self.table))
        return cur.fetchall()

    def _q_user_triggers(self, cur) -> list:
        """Triggers do usuário (não internos do sistema)."""
        self._run_query(cur, """
            SELECT tgname
            FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND NOT t.tgisinternal
              AND t.tgenabled IN ('a','r','t')
            ORDER BY tgname
        """, (self.schema, self.table))
        return cur.fetchall()

    # ─── coleta completa ────────────────────────────────────

    def collect_all(self) -> int:
        """Coleta TODOS os objetos que serão afetados. Retorna a contagem."""
        self.dropped_objects.clear()
        logger.info(f"Coletando constraints de '{self.schema}'.'{self.table}'...")
        conn = self._connect()
        try:
            cur = conn.cursor()

            # 1) FKs de tabelas filhas que nos referenciam
            # Agrupadas por constraint para suportar FKs compostas (multi-coluna)
            rows = self._q_foreign_keys_referencing_us(cur)
            logger.info(f"  FKs filhas (outras tabelas -> esta): {len(rows)} colunas")
            fk_child: dict = {}   # (conname, cschema, ctable) -> info
            for row in rows:
                conname, cschema, ctable, ccol, pcol, urule, drule, _pos = row
                key = (conname, cschema, ctable)
                if key not in fk_child:
                    fk_child[key] = {'child_cols': [], 'parent_cols': [],
                                     'urule': urule, 'drule': drule}
                fk_child[key]['child_cols'].append(ccol)
                fk_child[key]['parent_cols'].append(pcol)

            for (conname, cschema, ctable), info in fk_child.items():
                child_cols  = ', '.join(f'"{c}"' for c in info['child_cols'])
                parent_cols = ', '.join(f'"{c}"' for c in info['parent_cols'])
                drop = (f'ALTER TABLE "{cschema}"."{ctable}" '
                        f'DROP CONSTRAINT IF EXISTS "{conname}";')
                create = (f'ALTER TABLE "{cschema}"."{ctable}" '
                          f'ADD CONSTRAINT "{conname}" '
                          f'FOREIGN KEY ({child_cols}) '
                          f'REFERENCES "{self.schema}"."{self.table}"({parent_cols}) '
                          f'ON UPDATE {info["urule"]} ON DELETE {info["drule"]};')
                self.dropped_objects.append(DroppedObject(
                    'foreign_key_child', f'{cschema}.{ctable}.{conname}',
                    create, drop))
                logger.debug(f"    FK child: {conname} ({child_cols}) -> ({parent_cols})")

            logger.info(f"  FKs filhas agrupadas: {len(fk_child)} constraints")

            # 2) FKs próprias da tabela destino
            # Agrupadas por constraint para suportar FKs compostas (multi-coluna)
            rows = self._q_own_foreign_keys(cur)
            logger.info(f"  FKs próprias (esta tabela -> outras): {len(rows)} colunas")
            fk_own: dict = {}   # (conname, rschema, rtable) -> info
            for row in rows:
                conname, ccol, rschema, rtable, rcol, urule, drule, _pos = row
                key = (conname, rschema, rtable)
                if key not in fk_own:
                    fk_own[key] = {'child_cols': [], 'parent_cols': [],
                                   'urule': urule, 'drule': drule}
                fk_own[key]['child_cols'].append(ccol)
                fk_own[key]['parent_cols'].append(rcol)

            for (conname, rschema, rtable), info in fk_own.items():
                child_cols  = ', '.join(f'"{c}"' for c in info['child_cols'])
                parent_cols = ', '.join(f'"{c}"' for c in info['parent_cols'])
                drop = (f'ALTER TABLE "{self.schema}"."{self.table}" '
                        f'DROP CONSTRAINT IF EXISTS "{conname}";')
                create = (f'ALTER TABLE "{self.schema}"."{self.table}" '
                          f'ADD CONSTRAINT "{conname}" '
                          f'FOREIGN KEY ({child_cols}) '
                          f'REFERENCES "{rschema}"."{rtable}"({parent_cols}) '
                          f'ON UPDATE {info["urule"]} ON DELETE {info["drule"]};')
                self.dropped_objects.append(DroppedObject(
                    'foreign_key_own', conname, create, drop))
                logger.debug(f"    FK own: {conname} ({child_cols}) -> {rtable}({parent_cols})")

            logger.info(f"  FKs próprias agrupadas: {len(fk_own)} constraints")

            # 3) Check constraints
            rows = self._q_constraints_by_type(cur, 'c')
            logger.info(f"  Check constraints: {len(rows)}")
            for conname, condef in rows:
                drop = (f'ALTER TABLE "{self.schema}"."{self.table}" '
                        f'DROP CONSTRAINT IF EXISTS "{conname}";')
                create = (f'ALTER TABLE "{self.schema}"."{self.table}" '
                          f'ADD CONSTRAINT "{conname}" {condef};')
                self.dropped_objects.append(DroppedObject('check', conname, create, drop))

            # 4) Unique constraints
            rows = self._q_constraints_by_type(cur, 'u')
            logger.info(f"  Unique constraints: {len(rows)}")
            for conname, condef in rows:
                drop = (f'ALTER TABLE "{self.schema}"."{self.table}" '
                        f'DROP CONSTRAINT IF EXISTS "{conname}";')
                create = (f'ALTER TABLE "{self.schema}"."{self.table}" '
                          f'ADD CONSTRAINT "{conname}" {condef};')
                self.dropped_objects.append(DroppedObject('unique', conname, create, drop))

            # 5) Primary key
            rows = self._q_constraints_by_type(cur, 'p')
            logger.info(f"  Primary key(s): {len(rows)}")
            for conname, condef in rows:
                drop = (f'ALTER TABLE "{self.schema}"."{self.table}" '
                        f'DROP CONSTRAINT IF EXISTS "{conname}";')
                create = (f'ALTER TABLE "{self.schema}"."{self.table}" '
                          f'ADD CONSTRAINT "{conname}" {condef};')
                self.dropped_objects.append(DroppedObject('primary_key', conname, create, drop))
                logger.info(f"    PK: {conname}")

            # 6) Índices explícitos
            rows = self._q_explicit_indexes(cur)
            logger.info(f"  Índices explícitos: {len(rows)}")
            for idxname, idxdef in rows:
                drop = f'DROP INDEX IF EXISTS "{self.schema}"."{idxname}";'
                self.dropped_objects.append(DroppedObject('index', idxname, f'{idxdef};', drop))
                logger.debug(f"    index: {idxname}")

            # 7) Triggers (disable, não drop)
            rows = self._q_user_triggers(cur)
            logger.info(f"  Triggers: {len(rows)}")
            for (tgname,) in rows:
                disable = (f'ALTER TABLE "{self.schema}"."{self.table}" '
                           f'DISABLE TRIGGER "{tgname}";')
                enable = (f'ALTER TABLE "{self.schema}"."{self.table}" '
                          f'ENABLE TRIGGER "{tgname}";')
                self.dropped_objects.append(DroppedObject('trigger', tgname, enable, disable))

        finally:
            conn.close()

        total = len(self.dropped_objects)
        if total == 0:
            logger.warning(
                f"ATENÇÃO: Nenhum objeto encontrado em '{self.schema}'.'{self.table}'. "
                f"Verifique se a tabela PG existe e tem PK/FK/índices definidos.")
        else:
            logger.info(f"Total coletado: {total} objetos.")
        return total

    # ─── geração de scripts SQL ─────────────────────────────

    def generate_disable_script(self) -> str:
        """Gera script SQL para remover tudo antes da carga."""
        if not self.dropped_objects:
            self.collect_all()

        lines = [
            '-- ===========================================================',
            f'-- DESABILITAR CONSTRAINTS/ÍNDICES: {self.schema}.{self.table}',
            '-- Gerado automaticamente pelo migrator',
            '-- ===========================================================',
            '',
            'BEGIN;',
            '',
        ]
        for obj in self.dropped_objects:
            lines.append(obj.drop_sql)
        lines.extend([
            '',
            '-- Otimizações de sessão para carga',
            "SET synchronous_commit = off;",
            "SET jit = off;",
            '',
            'COMMIT;',
            '',
            f'-- Total: {len(self.dropped_objects)} objetos',
        ])
        return '\n'.join(lines)

    def generate_enable_script(self) -> str:
        """Gera script SQL para recriar tudo após a carga."""
        if not self.dropped_objects:
            self.collect_all()

        lines = [
            '-- ===========================================================',
            f'-- REABILITAR CONSTRAINTS/ÍNDICES: {self.schema}.{self.table}',
            '-- Gerado automaticamente pelo migrator',
            '-- ===========================================================',
            '',
            'BEGIN;',
            '',
        ]

        count = 0
        for obj_type in REENABLE_ORDER:
            group = [o for o in self.dropped_objects if o.obj_type == obj_type]
            if not group:
                continue
            label = {
                'index': 'Indexes explícitos',
                'primary_key': 'Primary Key',
                'unique': 'Unique Constraints',
                'check': 'Check Constraints',
                'foreign_key_own': 'Foreign Keys (próprias)',
                'foreign_key_child': 'Foreign Keys (tabelas filhas)',
                'trigger': 'Triggers (reabilitar)',
            }[obj_type]
            lines.append(f'-- {label}')
            for obj in group:
                lines.append(obj.create_sql)
                count += 1
            lines.append('')

        lines.extend([
            '-- Restaurar configurações',
            "SET synchronous_commit = on;",
            "SET jit = on;",
            '',
            '-- Atualizar estatísticas e reindexar',
            f'ANALYZE "{self.schema}"."{self.table}";',
            f'REINDEX TABLE "{self.schema}"."{self.table}";',
            '',
            'COMMIT;',
            '',
            f'-- Total: {count} objetos recriados',
        ])
        return '\n'.join(lines)

    # ─── execução direta ────────────────────────────────────

    def disable_all(self) -> int:
        """Remove/desabilita tudo. Retorna quantidade de objetos afetados."""
        if not self.dropped_objects:
            self.collect_all()

        conn = self._connect()
        try:
            conn.autocommit = True          # cada DDL auto-commit
            cur = conn.cursor()
            ok = 0
            for obj in self.dropped_objects:
                try:
                    cur.execute(obj.drop_sql)
                    logger.info(f"  ✕ {obj.obj_type:25s} -> {obj.obj_name}")
                    ok += 1
                except Exception as e:
                    logger.warning(f"  ⚠ {obj.obj_type:25s} -> {obj.obj_name}: {e}")
            # Otimizações
            cur.execute("SET synchronous_commit = off")
            cur.execute("SET jit = off")
            logger.info(f"Removidos/desabilitados: {ok}/{len(self.dropped_objects)}")
            return ok
        finally:
            conn.close()

    def enable_all(self) -> int:
        """Recria/reabilita tudo na ordem correta."""
        if not self.dropped_objects:
            logger.warning("Nenhum objeto para recriar.")
            return 0

        conn = self._connect()
        try:
            conn.autocommit = True
            cur = conn.cursor()
            ok = 0

            for obj_type in REENABLE_ORDER:
                for obj in self.dropped_objects:
                    if obj.obj_type != obj_type:
                        continue
                    try:
                        cur.execute(obj.create_sql)
                        logger.info(f"  ✓ {obj.obj_type:25s} -> {obj.obj_name}")
                        ok += 1
                    except Exception as e:
                        logger.error(f"  ✗ {obj.obj_type:25s} -> {obj.obj_name}: {e}")

            # Restaurar e analisar
            cur.execute("SET synchronous_commit = on")
            cur.execute(f'ANALYZE "{self.schema}"."{self.table}"')
            cur.execute(f'REINDEX TABLE "{self.schema}"."{self.table}"')

            logger.info(f"Recriados/reabilitados: {ok}/{len(self.dropped_objects)}")
            return ok
        finally:
            conn.close()

    # ─── serialização de estado ─────────────────────────────

    def save_state(self, filepath: str):
        data = [{'obj_type': o.obj_type, 'obj_name': o.obj_name,
                 'create_sql': o.create_sql, 'drop_sql': o.drop_sql}
                for o in self.dropped_objects]
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def load_state(self, filepath: str):
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        self.dropped_objects = [DroppedObject(**d) for d in data]
