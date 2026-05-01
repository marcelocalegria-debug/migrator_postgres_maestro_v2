---
name: sqlite-controle
description: Especialista nas tabelas de controle migration.db do Maestro V2 — consulta, diagnóstico e ajuste seguro via MCP
---

# Especialista em Controle de Migração (migration.db)

## Visão Geral

Cada diretório `MIGRACAO_NNNN/` contém um banco SQLite chamado `migration.db`.
Este banco é o estado centralizado do Maestro V2. As ferramentas MCP
`query_migration_db` e `update_migration_db` permitem consultar e ajustar
este banco sem risco de corromper a migração.

---

## Ferramentas MCP Disponíveis

### `query_migration_db(sql, project="")`
- Executa qualquer `SELECT` no `migration.db`.
- `project`: nome do diretório (ex: `MIGRACAO_0002`). Se omitido, usa o projeto ativo.
- Retorna até 200 linhas formatadas em tabela com cabeçalho.

### `update_migration_db(sql, project="")`
- Executa `UPDATE` controlado. **`WHERE` é obrigatório.**
- Bloqueios embutidos: rejeita `DELETE`, `INSERT`, `DROP`, `CREATE`, `ALTER`, `TRUNCATE`.
- Tabelas permitidas: `migration_meta`, `steps`, `tables`, `batches`, `constraints`, `errors`.
- Retorna quantas linhas foram afetadas.

---

## Protocolo de Segurança (ZERO RISCO)

> Siga estes passos em ordem. Nunca pule etapas.

### Passo 1 — Identificar migration_id
Sempre confirme qual migration_id está ativo antes de qualquer UPDATE:
```sql
SELECT id, seq, status, created_at FROM migration_meta ORDER BY id DESC LIMIT 5
```

### Passo 2 — Consultar o estado atual
Mostre ao usuário os dados que serão alterados:
```sql
SELECT id, source_table, status, rows_migrated, error_message
FROM tables
WHERE migration_id = <ID> AND source_table = 'NOME_TABELA'
```

### Passo 3 — Confirmar com o usuário
Antes de executar qualquer `update_migration_db`, mostre o SQL completo e aguarde
confirmação explícita do usuário ("sim", "pode executar", "confirmo").

### Passo 4 — Executar e reportar
Execute e informe quantas linhas foram afetadas. Se `0 linhas afetadas`,
revise o `WHERE` — nunca reexecute sem diagnóstico.

---

## Esquema Completo das Tabelas

### `migration_meta` — Metadados de cada execução de migração

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER PK | Identificador único da execução |
| `seq` | TEXT | Sequência de 4 dígitos (`0001`, `0002`…) |
| `status` | TEXT | `created` \| `running` \| `completed` \| `failed` |
| `config_yaml` | TEXT | Conteúdo do config.yaml usado |
| `schema_sql_path` | TEXT | Caminho do DDL aplicado |
| `created_at` | TIMESTAMP | Início da criação |
| `updated_at` | TIMESTAMP | Última atualização de status |
| `completed_at` | TIMESTAMP | Preenchido só em `completed`/`failed` |

**Campos imutáveis (NUNCA alterar):** `id`, `seq`, `config_yaml`, `schema_sql_path`, `created_at`

---

### `steps` — Estado de cada step do pipeline

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER PK | ID do step |
| `migration_id` | INTEGER | FK → `migration_meta.id` |
| `step_number` | INTEGER | Número do step (0–13) |
| `step_name` | TEXT | Nome do step (ver tabela abaixo) |
| `status` | TEXT | `pending` \| `running` \| `completed` \| `failed` \| `skipped` |
| `started_at` | TIMESTAMP | Início da execução |
| `completed_at` | TIMESTAMP | Fim da execução |
| `error_message` | TEXT | Mensagem de erro (se houver) |
| `details_json` | TEXT | JSON com detalhes extras do step |

**Mapa de steps:**

| step_number | step_name | O que faz |
|-------------|-----------|-----------|
| 0 | PRECHECK | Valida conexões FB+PG, espaço em disco |
| 1 | CREATE_DATABASE | DBA cria banco PG via DDL fornecido |
| 2 | IMPORT_SCHEMA | Aplica DDL no PostgreSQL |
| 3 | COMPARE_PRE | Comparação estrutural pré-carga |
| 5 | DISABLE_CONSTRAINTS | Remove FK/PK/índices/triggers |
| 6 | MIGRATE_BIG | Migra 10 tabelas grandes |
| 7 | MIGRATE_SMALL | Migra ~901 tabelas pequenas |
| 8 | ENABLE_CONSTRAINTS | Re-habilita constraints |
| 9 | SEQUENCES | Ajusta sequences para max(PK) |
| 10 | COMPARE_POST | Comparação estrutural pós-carga |
| 11 | VALIDATE | Count comparison FB×PG |
| 12 | ANALYZE | ANALYZE VERBOSE no PostgreSQL |
| 13 | REPORT | Gera relatório HTML |

**Campos imutáveis:** `id`, `migration_id`, `step_number`, `step_name`

---

### `tables` — Progresso por tabela

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER PK | ID da tabela |
| `migration_id` | INTEGER | FK → `migration_meta.id` |
| `source_table` | TEXT | Nome UPPERCASE (Firebird) |
| `dest_table` | TEXT | Nome lowercase (PostgreSQL) |
| `category` | TEXT | `small` \| `big` |
| `total_rows` | INTEGER | Total de linhas a migrar |
| `rows_migrated` | INTEGER | Linhas já copiadas |
| `rows_failed` | INTEGER | Linhas com erro |
| `current_batch` | INTEGER | Último batch processado |
| `total_batches` | INTEGER | Total de batches |
| `batch_size` | INTEGER | Linhas por batch |
| `last_pk_value` | TEXT | Último PK processado (checkpoint) |
| `pk_columns` | TEXT | Colunas que formam o PK |
| `use_db_key` | BOOLEAN | Usa RDB$DB_KEY em vez de PK |
| `status` | TEXT | `pending` \| `running` \| `completed` \| `failed` \| `skipped` |
| `speed_rows_per_sec` | REAL | Velocidade de inserção |
| `eta_seconds` | REAL | ETA estimado |
| `started_at` | TIMESTAMP | Início da migração |
| `updated_at` | TIMESTAMP | Última atualização |
| `completed_at` | TIMESTAMP | Conclusão |
| `error_message` | TEXT | Mensagem de erro |
| `worker_id` | TEXT | ID do processo worker |

**Campos imutáveis:** `id`, `migration_id`, `source_table`, `dest_table`, `category`, `pk_columns`, `use_db_key`, `batch_size`

---

### `batches` — Histórico de batches (somente leitura)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER PK | ID do batch |
| `table_id` | INTEGER | FK → `tables.id` |
| `batch_number` | INTEGER | Número sequencial do batch |
| `rows_in_batch` | INTEGER | Linhas processadas neste batch |
| `speed_rps` | REAL | Velocidade (rows/s) |
| `eta_seconds` | REAL | ETA restante no momento |
| `timestamp` | TIMESTAMP | Quando foi registrado |

> **Nunca fazer UPDATE em `batches`** — é log imutável de auditoria.

---

### `constraints` — Estado das constraints PG

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER PK | ID |
| `migration_id` | INTEGER | FK → `migration_meta.id` |
| `dest_table` | TEXT | Tabela PG (lowercase) |
| `constraint_type` | TEXT | `index` \| `pk` \| `unique` \| `check` \| `fk_own` \| `fk_child` \| `trigger` |
| `constraint_name` | TEXT | Nome da constraint no PG |
| `sql_disable` | TEXT | SQL de DROP |
| `sql_enable` | TEXT | SQL de CREATE/ADD |
| `status` | TEXT | `active` \| `dropped` \| `restored` \| `error` |
| `error_message` | TEXT | Erro ao re-habilitar |

> Para corrigir falhas de re-enable, usar `enable_constraints.py`, nunca UPDATE direto em `sql_enable`.

**Único UPDATE seguro em `constraints`:** limpar `error_message` após resolução manual.

---

### `errors` — Log de erros e resoluções

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER PK | ID do erro |
| `migration_id` | INTEGER | FK → `migration_meta.id` |
| `step_number` | INTEGER | Step onde ocorreu |
| `table_name` | TEXT | Tabela relacionada |
| `error_type` | TEXT | Categoria do erro |
| `error_message` | TEXT | Mensagem completa |
| `context_json` | TEXT | Contexto serializado |
| `ai_suggestion` | TEXT | Sugestão do agente IA |
| `resolution` | TEXT | Resolução registrada (`NULL` = não resolvido) |
| `created_at` | TIMESTAMP | Quando ocorreu |

**Campos imutáveis:** `id`, `migration_id`, `step_number`, `table_name`, `error_type`, `error_message`, `context_json`, `created_at`

---

## Operações Seguras — Receitas Prontas

### Consultar visão geral da migração
```sql
SELECT m.seq, m.status, s.step_number, s.step_name, s.status AS step_status
FROM migration_meta m
JOIN steps s ON s.migration_id = m.id
WHERE m.seq = '0002'
ORDER BY s.step_number
```

### Listar tabelas com falha
```sql
SELECT source_table, rows_migrated, total_rows, error_message
FROM tables
WHERE migration_id = <ID> AND status = 'failed'
ORDER BY source_table
```

### Listar tabelas ainda pendentes ou em execução
```sql
SELECT source_table, category, status, rows_migrated, total_rows
FROM tables
WHERE migration_id = <ID> AND status IN ('pending', 'running')
ORDER BY category DESC, source_table
```

### Resumo de progresso por status
```sql
SELECT status, COUNT(*) AS qtd, SUM(rows_migrated) AS linhas_migradas
FROM tables
WHERE migration_id = <ID>
GROUP BY status
ORDER BY qtd DESC
```

### Ver erros não resolvidos
```sql
SELECT id, step_number, table_name, error_type, error_message
FROM errors
WHERE migration_id = <ID> AND resolution IS NULL
ORDER BY id
```

### Ver histórico de velocidade de uma tabela
```sql
SELECT b.batch_number, b.rows_in_batch, ROUND(b.speed_rps,1) AS rps, b.timestamp
FROM batches b
JOIN tables t ON t.id = b.table_id
WHERE t.migration_id = <ID> AND t.source_table = 'NOME_TABELA'
ORDER BY b.batch_number DESC LIMIT 20
```

---

## Ajustes Seguros — SQLs de Correção

### Resetar tabela com falha para reiniciar migração
```sql
UPDATE tables
SET status = 'pending',
    rows_migrated = 0,
    rows_failed = 0,
    current_batch = 0,
    last_pk_value = NULL,
    started_at = NULL,
    completed_at = NULL,
    error_message = NULL
WHERE migration_id = <ID> AND source_table = 'NOME_TABELA'
```
> Use quando o migrador parou com erro e você quer reprocessar do zero.
> Após isso, rode `/rerun 7` (small) ou `/rerun 6` (big) no Maestro.

### Ignorar tabela que não pode ser migrada (marcar manualmente como concluída)
```sql
UPDATE tables
SET status = 'completed',
    error_message = 'MANUAL_IGNORE: Ignorada pelo DBA'
WHERE migration_id = <ID> AND source_table = 'NOME_TABELA'
```
> Use com cautela: a tabela será excluída do progresso do pipeline.

### Resetar step com falha para reexecutar
```sql
UPDATE steps
SET status = 'pending',
    started_at = NULL,
    completed_at = NULL,
    error_message = NULL
WHERE migration_id = <ID> AND step_number = <N>
```
> Equivale ao efeito de `/rerun N` no Maestro. Prefira o comando Maestro quando disponível.

### Marcar step manualmente como concluído (sem reexecutar)
```sql
UPDATE steps
SET status = 'completed',
    completed_at = datetime('now'),
    error_message = NULL
WHERE migration_id = <ID> AND step_number = <N>
```
> Use apenas quando o step falhou por razão externa já corrigida (ex: timeout de rede)
> e o trabalho do step foi verificado manualmente como OK.

### Registrar resolução de erro
```sql
UPDATE errors
SET resolution = 'Descrição da solução aplicada'
WHERE id = <ERROR_ID>
```

### Limpar error_message de constraint após resolução manual
```sql
UPDATE constraints
SET status = 'restored', error_message = NULL
WHERE migration_id = <ID> AND constraint_name = 'nome_da_constraint'
```

---

## Operações PROIBIDAS

| Operação | Por quê |
|----------|---------|
| `UPDATE tables SET rows_migrated = X` | Quebra consistência com batches reais |
| `UPDATE tables SET total_rows = X` | Distorce o cálculo de progresso e ETA |
| `UPDATE tables` sem `WHERE` | Reseta toda a migração |
| `UPDATE steps` sem `WHERE` | Marca todos os steps — corrompe o pipeline |
| Qualquer `DELETE` em qualquer tabela | Perde histórico de auditoria |
| `UPDATE constraints SET sql_enable = ...` | Pode gerar SQL inválido para re-enable |
| `UPDATE batches` | Log imutável de auditoria |
| `UPDATE migration_meta SET seq = ...` | Quebra o roteamento do Maestro |

---

## Comportamento Esperado do Agente

1. **Sempre consulte antes de agir** — use `query_migration_db` para ver o estado atual.
2. **Sempre confirme o `migration_id`** — nunca assuma qual migração está ativa.
3. **Mostre o SQL ao usuário antes de executar** `update_migration_db`.
4. **Atualize uma tabela/step por vez** — peça confirmação antes de updates em massa.
5. **Informe o resultado** — quantas linhas foram afetadas e qual o próximo passo recomendado.
6. **Se em dúvida, NÃO execute** — oriente o usuário a usar o Maestro (`/rerun`, `/status`).
