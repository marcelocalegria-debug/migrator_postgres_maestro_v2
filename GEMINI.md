# Maestro V2: Firebird to PostgreSQL Migration Tool

Orquestrador e pipeline de migração de alta performance para migrar bancos de dados Firebird 3 para PostgreSQL 18.

## Project Overview

Este projeto é uma ferramenta de migração robusta, projetada para lidar com bancos de dados de grande porte. Ele utiliza uma arquitetura baseada em passos (steps) gerenciada por um orquestrador central (`maestro.py`). O estado da migração é mantido em um banco de dados SQLite local em cada diretório de migração, permitindo resiliência e continuidade (resume).

### Tech Stack
- **Linguagem:** Python 3.13+
- **Gerenciador de Pacotes:** `uv` (preferencial) ou `pip`.
- **Bancos de Dados:** Firebird 3 (Origem), PostgreSQL 18 (Destino), SQLite (Controle de Estado).
- **Drivers/Libs:** `fdb`, `psycopg2-binary`, `SQLAlchemy`, `rich` (TUI), `prompt_toolkit`.
- **AI/LLM:** Integração via `litellm` e MCP (Model Context Protocol) para diagnóstico de schema e monitoramento.

## Architecture & Workflow

### 1. Orchestrator (Maestro V2)
O ponto de entrada principal é o `maestro.py`. Ele fornece uma CLI interativa para gerenciar o ciclo de vida da migração.

- **Comandos Principais:**
  - `/init`: Cria um novo diretório de migração (`MIGRACAO_XXXX/`).
  - `/run`: Inicia ou continua a execução do pipeline a partir do último ponto de sucesso.
  - `/status`: Exibe o progresso detalhado de cada passo.
  - `/monitor`: Abre a interface visual de monitoramento em tempo real.
  - `/agent`: Ativa o assistente de IA para suporte técnico e diagnóstico.

### 2. Migration Pipeline (Steps)
O pipeline é composto por 14 passos (S00 a S13), localizados em `lib/steps/`:
- **S00-S03:** Pré-validação, criação do banco, importação de schema e comparação pré-carga.
- **S05-S07:** Desativação de constraints, migração de tabelas grandes (paralelizada) e tabelas pequenas.
- **S08-S09:** Re-ativação de constraints e ajuste de sequences.
- **S10-S13:** Comparação pós-carga, validação de contagem, análise de performance (`ANALYZE`) e geração de relatório final.

### 3. Data Migration Strategy
- **Big Tables:** Tabelas como `DOCUMENTO_OPERACAO` e `LOG_EVENTOS` possuem migrators dedicados que dividem a carga em threads/processos usando ranges de PK ou `RDB$DB_KEY`.
- **Small Tables:** As demais ~900 tabelas são migradas em lote usando um pool de processos (`ProcessPoolExecutor`).
- **Resiliência:** Checkpoints automáticos permitem que migrações interrompidas continuem exatamente de onde pararam.

## Building and Running

### Setup Environment
```bash
# Instalar uv (se não tiver)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Configurar ambiente
uv sync
source .venv/bin/activate  # ou .venv/Scripts/activate no Windows
```

### Running Migration
```bash
# Iniciar o Maestro
python maestro.py

# Dentro do Maestro:
/init
/check
/run
```

### Useful Standalone Commands
```bash
# Monitoramento
python monitor.py MIGRACAO_XXXX

# Re-enable de constraints (emergência)
python enable_constraints.py

# Comparação estrutural
python compara_estrutura_fb2pg.py --work-dir MIGRACAO_XXXX
```

## Development Conventions

### 1. Adding New Steps
- Crie uma classe em `lib/steps/sXX_name.py` que herde de `StepBase`.
- Implemente o método `run()`.
- Registre o passo no `MaestroCLI` dentro de `lib/cli.py`.

### 2. State Management
- Nunca modifique o `migration.db` manualmente a menos que seja estritamente necessário. Use as ferramentas do `Maestro` ou o agente IA.
- O estado de cada tabela é controlado individualmente para garantir que `TRUNCATE` ocorra apenas em caso de `--reset`.

### 3. Logging & Errors
- Logs são gerados no diretório da migração ativa (`MIGRACAO_XXXX/logs/`).
- Erros críticos devem ser registrados no `MigrationDB` via `step.log_error()`.

### 4. AI & MCP
- O arquivo `mcps/db_migration_server.py` define as ferramentas disponíveis para o agente IA.
- Ao expandir as capacidades do agente, adicione novas ferramentas ao servidor MCP e documente-as em `skills/`.

## Directory Structure
- `lib/`: Núcleo do sistema (DB, Config, Steps, CLI).
- `mcps/`: Servidores de protocolo MCP.
- `skills/`: Conhecimento especializado para o agente IA.
- `MIGRACAO_XXXX/`: Dados, logs e estado de instâncias de migração.
- `work/`: Arquivos temporários e DDLs gerados.
