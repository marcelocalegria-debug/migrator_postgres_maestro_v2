---
name: duvidas-migrator
description: Guia de sintaxe e funcionamento do Maestro V2 e Migradores
---

# Guia de Referência Maestro V2 & Migradores

## 1. Maestro V2 (Orquestrador)
O **Maestro V2** refere-se ao script orquestrador central `maestro.py`. Ele gerencia o ciclo de vida completo da migração, coordenando os passos (Steps) e garantindo que as dependências sejam respeitadas.

### Comandos Principais (via maestro.py)
- `python maestro.py /init`: Cria uma nova estrutura de migração (`MIGRACAO_XXXX`) baseada no `config.yaml`.
- `python maestro.py /resume [seq]`: Retoma uma migração existente. Se o número não for passado, lista as disponíveis.
- `python maestro.py /status`: Exibe o progresso de cada etapa (Step 0 a 12).
- `python maestro.py /run [step_number]`: Executa a migração a partir de um passo específico. 
    - Ex: `python maestro.py /run 5` inicia o `MIGRATE_BIG`.
- `python maestro.py /monitor`: Abre o dashboard em tempo real (Rich TUI).
- `python maestro.py /check`: Valida conexões e metadados.

## 2. Ferramentas MCP (Acesso Seguro via db_migration_server.py)
Para auxiliar no diagnóstico e validação sem comprometer a integridade dos dados, utilize as ferramentas disponibilizadas pelo servidor MCP (`mcps/db_migration_server.py`). 

**Importante:** Sempre use o nome EXATO da ferramenta.

### Ferramentas de Auditoria e Validação:
- `get_firebird_table_count_safe(table_name: str)`: **OBRIGATÓRIO** para contar registros no Firebird. Retorna a contagem de linhas de uma tabela específica de forma segura (ReadOnly). *Nota: O agente não deve tentar chamar `get_firebird_table_count` sem o sufixo `_safe`.*
- `execute_readonly_sql_postgres(sql: str)`: Executa comandos `SELECT` no PostgreSQL usando uma conexão de auditoria restrita.
- `check_migration_logs(lines: int)`: Lê as últimas linhas do log mais recente na pasta `/logs` para identificar erros (ERROR, TRACEBACK).
- `run_count_comparison()`: Executa o script `compara_cont_fb2pg.py` para comparar o total de registros entre Firebird e Postgres.
- `generate_migration_report()`: Gera o relatório HTML de comparação de estrutura (`gera_relatorio_compara_estrutura_fb2pg_html.py`).
- `open_html_report()`: Localiza e abre o último relatório HTML gerado.

## 3. Migradores Individuais
Os scripts `migrator_*.py` podem ser chamados manualmente para tarefas específicas.

### migrator_v2.py (Tabelas Individuais)
- `python migrator_v2.py --table NOME_TABELA`
- `--reset`: Apaga o checkpoint e recomeça a tabela.
- `--dry-run`: Simula a carga sem gravar no PostgreSQL.
- `--batch-size 10000`: Ajusta o tamanho do lote de inserção.

### Migradores Especializados
- `migrator_parallel_doc_oper_v2.py`: Para a tabela `DOCUMENTO_OPERACAO`. Suporta `--threads`.
- `migrator_smalltables_v2.py`: Processa as ~900 tabelas pequenas em paralelo usando `ProcessPoolExecutor`.

## 3. Ordem de Execução Recomendada (Pipeline)
0. `PRECHECK`: Conectividade.
1. `CREATE_DATABASE`: Cria banco no PG.
2. `IMPORT_SCHEMA`: Aplica o DDL inicial.
4. `DISABLE_CONSTRAINTS`: Remove FKs/Índices para acelerar carga.
5. `MIGRATE_BIG`: Carga das tabelas gigantes.
6. `MIGRATE_SMALL`: Carga das tabelas pequenas (concorrente).
7. `ENABLE_CONSTRAINTS`: Recria FKs/Índices.
10. `VALIDATE`: Compara contagens e integridade.

## 4. Troubleshooting e Dicas
- **Checkpoints**: O estado é salvo em arquivos `.db` na pasta `work/`.
- **Logs**: Cada execução gera logs detalhados em `logs/` ou na pasta da migração.
- **Processos Órfãos**: Se interromper o Maestro, verifique se ainda existem processos `python.exe` rodando no Gerenciador de Tarefas.
