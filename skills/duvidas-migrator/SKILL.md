---
name: duvidas-migrator
description: Guia de sintaxe e funcionamento do Maestro V2 e Migradores
---

# Guia de Referência Maestro V2 & Migradores

## 1. Maestro V2 (Orquestrador)

O **Maestro V2** é o orquestrador interativo (`maestro.py`). CLI com prompt_toolkit + Rich.
Auto-resume da última migração ao iniciar.

### Inicialização

```bash
python maestro.py               # inicia ou retoma última migração
python maestro.py --resume 0005 # resume explícito da migração 0005
```

### Comandos dentro do CLI interativo

| Comando | O que faz |
| :--- | :--- |
| `/init` | Cria nova migração `MIGRACAO_<SEQ>/`, copia `config.yaml` |
| `/resume 0005` | Carrega migração 0005 (alias: `/load 0005`) |
| `/status` | Exibe steps S00–S13 com status, duração e tabelas |
| `/check` | Valida conexões FB+PG (equivale a S00 isolado) |
| `/compare` | Roda comparação estrutural FB×PG e abre relatório HTML |
| `/run` | Executa pipeline a partir do primeiro step pendente |
| `/run 6` | Executa a partir do step 6 (`migrate_big`) |
| `/rerun 7` | Força re-execução do step 7 mesmo se já `completed` |
| `/monitor` | Abre monitor Rich TUI em tempo real |
| `/agent` | Chat com agente IA (diagnóstico e diff de schema) |
| `/help` | Lista comandos disponíveis |
| `/quit` | Sai (Ctrl+C também funciona) |

## 2. Migradores Standalone

### migrator_v2.py — Tabela individual

```bash
python migrator_v2.py --work-dir MIGRACAO_0001 --table OPERACAO_CREDITO
python migrator_v2.py --work-dir MIGRACAO_0001 --table OPERACAO_CREDITO --reset
python migrator_v2.py --work-dir MIGRACAO_0001 --table OPERACAO_CREDITO --dry-run
python migrator_v2.py --work-dir MIGRACAO_0001 --table OPERACAO_CREDITO --use-insert
```

Parâmetros: `--work-dir` (obrigatório), `--table`, `--config`, `--reset`, `--dry-run`,
`--use-insert`, `--master-db`, `--migration-id`

### migrator_parallel_doc_oper_v2.py — DOCUMENTO_OPERACAO

```bash
python migrator_parallel_doc_oper_v2.py --work-dir MIGRACAO_0001 --threads 4
python migrator_parallel_doc_oper_v2.py --work-dir MIGRACAO_0001 --threads 4 --reset
python migrator_parallel_doc_oper_v2.py --work-dir MIGRACAO_0001 --threads 4 --dry-run
python migrator_parallel_doc_oper_v2.py --work-dir MIGRACAO_0001 --generate-scripts-only
```

Parâmetros: `--work-dir` (obrigatório), `--threads` (padrão: 4), `--batch-size`,
`--reset`, `--dry-run`, `--use-insert`, `--generate-scripts-only`,
`--master-db`, `--migration-id`

### migrator_log_eventos_v2.py — LOG_EVENTOS (sem PK, RDB$DB_KEY)

```bash
python migrator_log_eventos_v2.py --work-dir MIGRACAO_0001 --threads 8
python migrator_log_eventos_v2.py --work-dir MIGRACAO_0001 --threads 8 --reset
python migrator_log_eventos_v2.py --work-dir MIGRACAO_0001 --generate-scripts-only
```

Parâmetros: `--work-dir` (obrigatório), `--threads` (padrão: 8), `--batch-size`,
`--reset`, `--dry-run`, `--use-insert`, `--generate-scripts-only`,
`--master-db`, `--migration-id`

### migrator_smalltables_v2.py — ~901 tabelas pequenas

```bash
python migrator_smalltables_v2.py --work-dir MIGRACAO_0001 --small-tables
python migrator_smalltables_v2.py --work-dir MIGRACAO_0001 --small-tables --workers 6
python migrator_smalltables_v2.py --work-dir MIGRACAO_0001 --table CEP
```

Parâmetros: `--work-dir` (obrigatório), `--small-tables`, `--table`,
`--workers`, `--master-db`, `--migration-id`

## 3. Monitor

```bash
python monitor.py MIGRACAO_0005
python monitor.py MIGRACAO_0005 --big-tables
python monitor.py MIGRACAO_0005 --small-tables
python monitor.py MIGRACAO_0005 -i 5.0        # atualiza a cada 5 segundos
```

## 4. Validação Pós-Migração

```bash
# Comparação estrutural (PKs, FKs, índices, constraints)
python compara_estrutura_fb2pg.py --work-dir MIGRACAO_0001
python compara_estrutura_fb2pg.py --work-dir MIGRACAO_0001 --verbose
python compara_estrutura_fb2pg.py --work-dir MIGRACAO_0001 --skip-count

# Comparação FULL (inclui colunas e tipos FB→PG)
python compara_estrutura_FULL_fb2pg.py --work-dir MIGRACAO_0001

# Contagem de linhas
python compara_cont_fb2pg.py --work-dir MIGRACAO_0001

# Checksum BYTEA/BLOB
python PosMigracao_comparaChecksum_bytea.py --table OPERACAO_CREDITO
python PosMigracao_comparaChecksum_bytea.py --workers 1 --sample 1000

# Relatório HTML
python gera_relatorio_compara_estrutura_fb2pg_html.py --work-dir MIGRACAO_0001
python gera_relatorio_compara_estrutura_fb2pg_html.py --work-dir MIGRACAO_0001 --output relatorio.html
```

## 5. Re-enable de Constraints (Emergência)

```bash
python enable_constraints.py                           # todas as tabelas
python enable_constraints.py --dry-run                 # simula sem executar
python enable_constraints.py -t operacao_credito       # apenas uma tabela (repetível)
python enable_constraints.py --fail-fast               # para ao primeiro erro
python enable_constraints.py --report relatorio.txt    # grava relatório em arquivo
```

## 6. Ferramentas MCP (via db_migration_server.py)

Usadas pelo agente IA (`/agent`). Sempre usar o nome EXATO da ferramenta.

| Ferramenta | Função |
| :--- | :--- |
| `get_firebird_table_count_safe(table_name)` | Conta linhas no Firebird (ReadOnly) |
| `execute_readonly_sql_postgres(sql)` | SELECT no PostgreSQL (auditoria) |
| `check_migration_logs(lines)` | Lê últimas linhas dos logs de migração |
| `run_count_comparison()` | Executa `compara_cont_fb2pg.py` |
| `generate_migration_report()` | Gera relatório HTML de estrutura |
| `open_html_report()` | Abre o último relatório HTML gerado |

## 7. Pipeline Completo (Referência)

| Step | Nome | O que faz |
| :--- | :--- | :--- |
| S00 | PRECHECK | Conectividade FB+PG, Python, espaço em disco |
| S01 | CREATE_DATABASE | Valida parâmetros, DBA cria banco via SQL fornecido |
| S02 | IMPORT_SCHEMA | Aplica DDL `.sql` no PostgreSQL via `psql` |
| S03 | COMPARE_PRE | Compara estrutura FB×PG, aciona agente IA se diferenças |
| S05 | DISABLE_CONSTRAINTS | Remove FK/PK/índices/triggers |
| S06 | MIGRATE_BIG | Migra 10 tabelas grandes em paralelo |
| S07 | MIGRATE_SMALL | Migra ~901 tabelas pequenas (ProcessPoolExecutor) |
| S08 | ENABLE_CONSTRAINTS | Re-habilita FK/PK/índices na ordem correta |
| S09 | SEQUENCES | Ajusta sequences para `max(PK)` do Firebird |
| S10 | COMPARE_POST | Comparação estrutural pós-carga |
| S11 | VALIDATE | Count comparison FB×PG |
| S12 | ANALYZE | `ANALYZE VERBOSE` no PostgreSQL |
| S13 | REPORT | Gera relatório HTML final |

## 8. Troubleshooting Rápido

| Sintoma | Causa provável | Ação |
| :--- | :--- | :--- |
| `database is locked` SQLite | Múltiplos workers no mesmo arquivo | Reduzir `--workers` |
| Thread parada sem progresso | Join sem timeout | `pkill` + `/rerun <step>` |
| FK violation ao re-enable | Dados com inconsistência | Ver log `enable_constraints_*.log` |
| OOM durante `migrate_small` | BLOB grande × batch alto | Reduzir `batch_size` para 1.000–2.000 |
| Step `completed` mas dados faltam | S06 marcou S07 prematuramente | `/rerun 7` |
| Constraints desabilitadas após falha | S05 OK, S06 falhou, S08 não rodou | `python enable_constraints.py` |
| Agente IA não responde | Timeout OpenRouter | `/quit` + reiniciar agente manualmente |
