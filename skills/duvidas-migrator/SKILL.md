---
name: duvidas-migrator
description: Guia de sintaxe e funcionamento do Maestro V2 e Migradores
---

# Guia de Referência Maestro V2 & Migradores

## 1. Maestro V2 (Orquestrador)
O Maestro é a CLI central (`maestro.py`) que gerencia o ciclo de vida da migração.

### Comandos Principais
- `/init`: Cria uma nova estrutura de migração (`MIGRACAO_XXXX`) baseada no `config.yaml`.
- `/resume [seq]`: Retoma uma migração existente. Se o número não for passado, lista as disponíveis.
- `/status`: Exibe o progresso de cada etapa (Step 0 a 12).
- `/run [step_number]`: Executa a migração a partir de um passo específico. 
    - Ex: `/run 5` inicia o `MIGRATE_BIG`.
- `/monitor`: Abre o dashboard em tempo real (Rich TUI).
- `/check`: Valida conexões e metadados.

## 2. Migradores Individuais
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
