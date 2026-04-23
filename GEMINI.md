# Project: Migração Firebird 3 → PostgreSQL 18+ (V2)

Este projeto é uma suíte de ferramentas para migração de alta performance de bancos de dados Firebird 3.0 para PostgreSQL 18+ (ou versões compatíveis), otimizada para grandes volumes de dados através do protocolo `COPY` e orquestração via SQLite.

## Visão Geral do Projeto

A arquitetura do projeto é dividida em orquestradores especializados por tipo de tabela (pequenas, grandes, e específicas como `DOCUMENTO_OPERACAO`), utilizando um banco de dados SQLite central para gerenciar o progresso, checkpoints e estado de constraints.

### Tecnologias Principais
- **Linguagem:** Python 3.13+
- **Bibliotecas:** 
    - `fdb`: Conectividade Firebird.
    - `psycopg2-binary`: Conectividade PostgreSQL.
    - `PyYAML`: Gerenciamento de configuração.
    - `rich`: Interface de console enriquecida.
    - `sqlite3`: Controle de estado e persistência de progresso.
- **Protocolo de Carga:** PostgreSQL `COPY` (3-5x mais rápido que `INSERT` convencional).

## Principais Componentes

### Migradores
- `migrator_v2.py`: Migrador universal para tabelas individuais. Suporta reinicialização automática e conversão de BLOBs.
- `migrator_smalltables_v2.py`: Migrador concorrente (via `ProcessPoolExecutor`) para processar múltiplas tabelas pequenas em paralelo.
- `migrator_parallel_doc_oper_v2.py`: Migrador especializado para a tabela `DOCUMENTO_OPERACAO`, particionando por range de chave primária para máxima vazão em múltiplas threads.

### Utilitários
- `pg_constraints.py`: Gerencia a remoção (Phase 0) e recriação (Phase 2) de constraints, PKs, Fks e índices para acelerar a carga de dados.
- `monitor.py`: Terminal interativo para acompanhar o progresso global, velocidade (rows/sec) e estimativa de término (ETA).
- `PosMigracao_comparaChecksum_bytea.py`: Ferramenta de validação pós-migração para garantir a integridade dos dados entre origem e destino.
- `lib/db.py`: Camada de acesso ao banco SQLite de orquestração ("Maestro V2").

## Guia de Execução

### 1. Preparação do Ambiente
```powershell
# Windows
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configuração
Crie um arquivo `config.yaml` baseado nas necessidades do banco (o projeto espera conexões para Firebird e PostgreSQL). Verifique o `fbclient.dll` para compatibilidade local.

### 3. Workflow Recomendado
1. **Preparar Destino:** Execute o script SQL de criação de estrutura no PostgreSQL.
2. **Carga Individual:**
   ```bash
   python migrator_v2.py --table NOME_DA_TABELA
   ```
3. **Carga de Big Tables (Paralelo):**
   ```bash
   ./chama_migrator.sh DOCUMENTO_OPERACAO
   ```
4. **Monitoramento:**
   ```bash
   python monitor.py
   ```
5. **Validação:**
   ```bash
   python PosMigracao_comparaChecksum_bytea.py --table NOME_DA_TABELA
   ```

## Convenções e Práticas de Desenvolvimento
- **Logs:** Centralizados no diretório `/logs`. Cada execução de tabela gera um log específico.
- **Estado:** Armazenado em `/work` via arquivos `.db` (SQLite) e `.json` (constraints).
- **Restart:** Todos os scripts são "restartable". O SQLite guarda o último `last_pk_value` ou `RDB$DB_KEY`.
- **Charset:** O projeto trata especificamente a conversão de `WIN1252` para `UTF-8` em campos BLOB TEXT.

## Troubleshooting
- **DLL Error (Firebird):** Certifique-se de que o `fbclient.dll` de 32 ou 64 bits (dependendo do Python) está no root ou no path do Windows.
- **Constraints Pending:** Se a migração for interrompida, as constraints podem ficar desabilitadas. Utilize o script `enable_constraints_{tabela}.sql` gerado em `/work` para recuperá-las manualmente via `psql`.
