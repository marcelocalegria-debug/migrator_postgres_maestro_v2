╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌
 Plano de Implementacao — Maestro V2

 Context

 O projeto atual de migracao Firebird 3 → PostgreSQL 18 funciona, mas exige que um DBA siga manualmente um script de ~15 passos (PRD.md linhas 135-877), disparando scripts
 Python e SQL na ordem correta. O Maestro.py automatiza esse processo completo numa CLI interativa, com controle centralizado, monitoramento unificado e assistencia de IA
 para erros criticos.

 A versao anterior dos migrators esta guardada separadamente — podemos ser agressivos nas mudancas.

 ---
 Ajustes Propostos ao PRD

 1. Banco de Controle Unico (PREMISSA OBRIGATORIA)

 PRD pede: Um unico SQLite em vez de ~908 bancos separados.
 Proposta: MigrationDB centralizado em MIGRACAO_<SEQ>/migration.db com tabelas:
 - migration_meta — metadata da migracao (seq, status, config, timestamps)
 - steps — cada passo da pipeline (status, inicio, fim, erro)
 - tables — estado de cada tabela (source, dest, rows_total, rows_migrated, status, speed, eta)
 - batches — telemetria por batch (tabela, batch_num, rows, speed, timestamp)
 - constraints — estado de cada constraint (tabela, tipo, nome, sql_enable, status)
 - errors — log de erros com contexto para o AI agent

 Os 4 migrators existentes serao modificados minimamente: recebem --master-db <path> e gravam diretamente neste DB centralizado em vez de criar SQLite individual. A classe
 StateManager atual (duplicada nos 4 migrators) sera extraida para lib/state.py como modulo compartilhado, com backend plugavel (single-table DB → master DB).

 2. Agentes de IA — Abordagem Pragmatica

 PRD pede: 2 MCP servers (Firebird + PostgreSQL) + framework de agente + skills.
 Proposta para teste rapido: Chamadas diretas a API Anthropic (sem MCP servers por ora):
 - Quando compara_estrutura detecta diferencas de schema, o AI recebe o diff e gera ALTER scripts
 - Quando um step falha com erro critico, o AI recebe contexto (erro + schema + step) e sugere correcao
 - Human-in-the-loop: AI propoe, DBA aprova no prompt do Maestro ([Y/n/edit])
 - Pasta skills/ com markdown descrevendo padroes de erro conhecidos e guardrails
 - .env com ANTHROPIC_API_KEY, AI_MODEL (default: claude-sonnet-4-20250514)
 - MCP servers como enhancement futuro (nao bloqueia teste)

 3. Ajuste de Sequences — Reescrever em Python

 PRD pede: Incorporar generators-gerar-c6bank-prod.sh e generators-acertar-c6bank-prod-no-postgres.sh.
 Proposta: lib/steps/sequences.py que:
 - Conecta ao Firebird, le RDB$GENERATORS (query direta, sem isql)
 - Gera e executa DROP SEQUENCE / CREATE SEQUENCE / SELECT setval() no PostgreSQL
 - Trata erros individualmente (sequence por sequence)
 - Registra resultado no MigrationDB

 4. CLI Interativa

 PRD pede: CLI tipo Claude Code com comandos /.
 Proposta: prompt_toolkit (ja transitiva do rich) para:
 - Autocomplete de comandos
 - Historico de sessao
 - Prompt colorido com status atual
 - Comandos: /init, /resume, /status, /precheck, /compare, /monitor, /auto, /run, /help, /quit

 5. Monitor Unificado

 Proposta: monitor.py revisado le exclusivamente do MigrationDB centralizado. Dashboard unico mostra big tables + small tables + steps + constraints numa unica tela Rich.

 ---
 Arquitetura de Arquivos

 maestro.py                              # Entry point CLI
 .env                                    # ANTHROPIC_API_KEY, AI_MODEL

 lib/
   __init__.py
   db.py                                 # MigrationDB — SQLite centralizado
   config.py                             # Loader de config + validacao
   project.py                            # Gerencia MIGRACAO_<SEQ> dirs
   state.py                              # StateManager refatorado (shared)
   cli.py                                # CLI prompt_toolkit + command dispatch

   steps/
     __init__.py
     base.py                             # StepBase ABC + StepRunner
     s00_precheck.py                     # Verifica conectividade FB/PG, disco, versoes
     s01_create_database.py              # CREATE DATABASE via psql (se nao existir)
     s02_import_schema.py                # psql -f schema.sql
     s03_compare_pre.py                  # Compara estrutura antes da migracao
     s04_fix_blobs.py                    # fix_blob_text_columns integration
     s05_disable_constraints.py          # ConstraintManager.disable_all() para todas tabelas
     s06_migrate_big.py                  # subprocess: migrator_v2.py x8 + doc_oper + log_eventos
     s07_migrate_small.py                # subprocess: migrator_smalltables_v2.py
     s08_enable_constraints.py           # ConstraintManager.enable_all() + handle errors
     s09_sequences.py                    # Ajuste sequences (rewrite Python)
     s10_compare_post.py                 # Compara estrutura pos-migracao
     s11_validate.py                     # Row counts + checksum BYTEA
     s12_analyze.py                      # ANALYZE VERBOSE
     s13_report.py                       # Relatorio final HTML

   ai/
     __init__.py
     agent.py                            # Chamadas Anthropic API
     prompts.py                          # System prompts + templates

   skills/                               # Markdown — guardrails para o AI
     schema_diff_fix.md                  # Como gerar ALTERs para diferencas de schema
     constraint_error.md                 # Erros comuns em constraints e como corrigir
     data_type_mismatch.md               # Mapeamento FB→PG problematico
     blob_handling.md                    # Regras de BLOB SUB_TYPE 0 vs 1
     sequence_errors.md                  # Problemas com sequences/generators

 MIGRACAO_0001/                          # Criado pelo /init
   config.yaml                           # Copia da config usada
   schema.sql                            # SQL de schema fornecido pelo DBA
   migration.db                          # MigrationDB centralizado
   logs/                                 # Todos os logs da migracao
   sql/                                  # Scripts SQL gerados
   json/                                 # Constraint state JSONs
   reports/                              # Relatorios HTML

 ---
 MigrationDB Schema (lib/db.py)

 -- Metadata da migracao
 CREATE TABLE migration_meta (
     id INTEGER PRIMARY KEY AUTOINCREMENT,
     seq TEXT NOT NULL,                    -- '0001'
     status TEXT DEFAULT 'created',        -- created|running|paused|completed|failed
     config_yaml TEXT,                     -- conteudo do config.yaml
     schema_sql_path TEXT,                 -- path do .sql de schema
     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
     updated_at TIMESTAMP,
     completed_at TIMESTAMP
 );

 -- Pipeline steps
 CREATE TABLE steps (
     id INTEGER PRIMARY KEY AUTOINCREMENT,
     migration_id INTEGER REFERENCES migration_meta(id),
     step_number INTEGER NOT NULL,         -- 0..13
     step_name TEXT NOT NULL,              -- 'precheck', 'create_database', etc.
     status TEXT DEFAULT 'pending',         -- pending|running|completed|failed|skipped
     started_at TIMESTAMP,
     completed_at TIMESTAMP,
     error_message TEXT,
     details_json TEXT                      -- detalhes step-specific
 );

 -- Estado por tabela
 CREATE TABLE tables (
     id INTEGER PRIMARY KEY AUTOINCREMENT,
     migration_id INTEGER REFERENCES migration_meta(id),
     source_table TEXT NOT NULL,           -- UPPERCASE (Firebird)
     dest_table TEXT NOT NULL,             -- lowercase (PostgreSQL)
     category TEXT DEFAULT 'small',        -- big|small|parallel_pk|parallel_dbkey
     total_rows INTEGER DEFAULT 0,
     rows_migrated INTEGER DEFAULT 0,
     rows_failed INTEGER DEFAULT 0,
     current_batch INTEGER DEFAULT 0,
     total_batches INTEGER DEFAULT 0,
     batch_size INTEGER DEFAULT 5000,
     last_pk_value TEXT,                   -- JSON: [val1, val2] ou hex db_key
     pk_columns TEXT,                      -- JSON: ["col1", "col2"]
     use_db_key BOOLEAN DEFAULT 0,
     status TEXT DEFAULT 'pending',         -- pending|running|completed|failed|paused
     speed_rows_per_sec REAL DEFAULT 0,
     eta_seconds REAL,
     started_at TIMESTAMP,
     updated_at TIMESTAMP,
     completed_at TIMESTAMP,
     error_message TEXT,
     worker_id TEXT                         -- 't0', 't1', 'main', 'w3'
 );

 -- Telemetria de batches
 CREATE TABLE batches (
     id INTEGER PRIMARY KEY AUTOINCREMENT,
     table_id INTEGER REFERENCES tables(id),
     batch_number INTEGER,
     rows_in_batch INTEGER,
     speed_rps REAL,
     eta_seconds REAL,
     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
 );

 -- Constraints
 CREATE TABLE constraints (
     id INTEGER PRIMARY KEY AUTOINCREMENT,
     migration_id INTEGER REFERENCES migration_meta(id),
     dest_table TEXT NOT NULL,
     constraint_type TEXT,                 -- pk|fk_own|fk_child|unique|check|index|trigger
     constraint_name TEXT,
     sql_disable TEXT,
     sql_enable TEXT,
     status TEXT DEFAULT 'active',          -- active|disabled|enabled|failed
     error_message TEXT
 );

 -- Log de erros para contexto AI
 CREATE TABLE errors (
     id INTEGER PRIMARY KEY AUTOINCREMENT,
     migration_id INTEGER REFERENCES migration_meta(id),
     step_number INTEGER,
     table_name TEXT,
     error_type TEXT,                       -- schema_diff|constraint_fail|data_error|connection
     error_message TEXT,
     context_json TEXT,                     -- contexto completo para o AI
     ai_suggestion TEXT,                    -- resposta do AI (se chamado)
     resolution TEXT,                       -- approved|rejected|manual
     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
 );

 ---
 Pipeline de Steps (Maestro)

 /init → cria MIGRACAO_<SEQ>/ → registra steps no DB

 Step 0  PRECHECK           Conectividade FB/PG, disco, versoes, schema.sql existe
 Step 1  CREATE_DATABASE     CREATE DATABASE c6_producao (se nao existir, se existir → erro + manual)
 Step 2  IMPORT_SCHEMA       psql -f schema.sql → importa DDL no PG vazio
 Step 3  COMPARE_PRE         compara_estrutura FB vs PG → mostra diffs → AI sugere fixes → human aprova
 Step 4  FIX_BLOBS           fix_blob_text_columns → gera e executa ALTER COLUMN TYPE text
 Step 5  DISABLE_CONSTRAINTS ConstraintManager.disable_all() para TODAS as tabelas (salva state no DB)
 Step 6  MIGRATE_BIG         subprocess: 8x migrator_v2.py + doc_oper + log_eventos (paralelo)
 Step 7  MIGRATE_SMALL       subprocess: migrator_smalltables_v2.py --small-tables
 Step 8  ENABLE_CONSTRAINTS  ConstraintManager.enable_all() (ordem correta, handle errors)
 Step 9  SEQUENCES           Python: le RDB$GENERATORS → DROP/CREATE/SETVAL no PG
 Step 10 COMPARE_POST        compara_estrutura pos-migracao (inclui sequences agora)
 Step 11 VALIDATE            compara_cont + checksum BYTEA
 Step 12 ANALYZE             SET default_statistics_target=10000; ANALYZE VERBOSE;
 Step 13 REPORT              Gera relatorio HTML final consolidado

 Cada step:
   - Verifica pre-condicoes (step anterior completed)
   - Atualiza status no MigrationDB
   - Em caso de erro critico + /auto desligado → chama AI → human aprova
   - Registra resultado + timing

 ---
 Plano de Implementacao Multi-Agente

 Wave 1 — Fundacao (sequencial, 1 agente)

 Agente A: Core Framework
 Arquivos: lib/__init__.py, lib/db.py, lib/config.py, lib/project.py, lib/state.py, lib/steps/__init__.py, lib/steps/base.py

 Tarefas:
 1. lib/db.py — Classe MigrationDB com todo o schema acima, metodos CRUD para cada tabela, context manager, WAL mode, retry em SQLITE_BUSY
 2. lib/config.py — MigrationConfig carrega YAML, valida campos obrigatorios, resolve paths relativos ao MIGRACAO_
 3. lib/project.py — MigrationProject cria/abre MIGRACAO_, cria subdiretorios, detecta proximo SEQ, lista migracoes existentes
 4. lib/state.py — StateManager refatorado do codigo atual: mesma interface mas backend pode ser o MigrationDB centralizado. Metodos: save_progress(), load_progress(),
 log_batch(), reset(). Mapeamento table_name → table_id no DB central.
 5. lib/steps/base.py — StepBase ABC com run(), skip(), status, pre/post hooks, logging, timing. StepRunner executa steps em sequencia, verifica pre-condicoes, atualiza DB.

 Criterio de pronto: Testes unitarios passam para MigrationDB (CRUD), MigrationProject (cria/lista dirs), StepRunner (executa mock steps).

 ---
 Wave 2 — Implementacao paralela (5 agentes simultaneos)

 Agente B: Steps de Migracao (s05, s06, s07, s08)
 Arquivos: lib/steps/s05_disable_constraints.py, lib/steps/s06_migrate_big.py, lib/steps/s07_migrate_small.py, lib/steps/s08_enable_constraints.py

 Tarefas:
 1. s05_disable_constraints.py — Descobre todas tabelas no PG, instancia ConstraintManager para cada, executa disable_all(), salva state nas tabelas constraints do
 MigrationDB. Gera SQL scripts em MIGRACAO_<SEQ>/sql/.
 2. s06_migrate_big.py — Spawna subprocesses:
   - 8x python migrator_v2.py --table <X> --config <path> --master-db <path> para as 8 tabelas grandes
   - 1x python migrator_parallel_doc_oper_v2.py --config <path> --master-db <path> para DOCUMENTO_OPERACAO
   - 1x python migrator_log_eventos_v2.py --config <path> --master-db <path> para LOG_EVENTOS
   - Monitora subprocesses, atualiza DB com progresso, lida com falhas
 3. s07_migrate_small.py — Spawna python migrator_smalltables_v2.py --small-tables --config <path> --master-db <path>. Monitora progresso.
 4. s08_enable_constraints.py — Executa enable_constraints para todas tabelas completed. Usa ConstraintManager.enable_all(). Registra erros "already exists" como skip.

 Requer tambem: Patch minimo nos 4 migrators para aceitar --master-db e usar lib/state.py em vez do StateManager embarcado. Este agente faz esses patches.

 ---
 Agente C: Steps de Pre-Migracao (s00, s01, s02, s03, s04)
 Arquivos: lib/steps/s00_precheck.py, lib/steps/s01_create_database.py, lib/steps/s02_import_schema.py, lib/steps/s03_compare_pre.py, lib/steps/s04_fix_blobs.py

 Tarefas:
 1. s00_precheck.py — Verifica:
   - Conexao Firebird (fdb.connect + SELECT 1)
   - Conexao PostgreSQL (psycopg2.connect + SELECT 1)
   - Arquivo schema.sql existe em MIGRACAO_/
   - fbclient.dll/libfbclient.so disponivel
   - Python version >= 3.13
   - Espaco em disco suficiente
   - Conta tabelas no Firebird (RDB$RELATIONS)
 2. s01_create_database.py — Gera e executa CREATE DATABASE SQL (baseado no template do PRD linhas 51-127). Se banco ja existe → erro com instrucao para DROP manual.
 3. s02_import_schema.py — Executa psql -f schema.sql via subprocess. Captura e analisa output/erros.
 4. s03_compare_pre.py — Importa funcoes de compara_estrutura_fb2pg.py. Compara tables, PKs, FKs, indices, checks. Se diffs encontradas → chama AI (se configurado) → mostra
 para DBA aprovar correção.
 5. s04_fix_blobs.py — Importa fix_blob_text_columns.py. Gera e executa ALTER COLUMN TYPE text.

 ---
 Agente D: Steps de Pos-Migracao (s09, s10, s11, s12, s13)
 Arquivos: lib/steps/s09_sequences.py, lib/steps/s10_compare_post.py, lib/steps/s11_validate.py, lib/steps/s12_analyze.py, lib/steps/s13_report.py

 Tarefas:
 1. s09_sequences.py — Rewrite Python dos shell scripts:
   - Conecta FB: SELECT RDB$GENERATOR_NAME, RDB$GENERATOR_VALUE FROM RDB$GENERATORS WHERE RDB$SYSTEM_FLAG = 0
   - Para cada generator: DROP SEQUENCE IF EXISTS sq_{name}; CREATE SEQUENCE sq_{name}; SELECT setval('sq_{name}', {value});
   - Executa no PG com tratamento individual de erros
   - Salva script em MIGRACAO_<SEQ>/sql/gen_sequences.sql
 2. s10_compare_post.py — Mesma logica do s03 mas pos-migracao. Adiciona comparacao de sequences (FB generators vs PG sequences, incluindo valores).
 3. s11_validate.py — Importa compara_cont_fb2pg.py para row counts. Importa PosMigracao_comparaChecksum_bytea.py para checksum BYTEA (com --workers 1 para nao estourar
 memoria).
 4. s12_analyze.py — Executa SET default_statistics_target = 10000; ANALYZE VERBOSE; via psycopg2.
 5. s13_report.py — Gera relatorio HTML consolidado:
   - Resumo da migracao (tempos, rows totais, erros)
   - Resultado de cada step
   - Tabela de row counts (FB vs PG)
   - Diferencas de estrutura encontradas
   - Constraints habilitadas/falhas
   - Sequences ajustadas
   - Checksum BYTEA

 ---
 Agente E: AI Agent + Skills
 Arquivos: lib/ai/__init__.py, lib/ai/agent.py, lib/ai/prompts.py, skills/*.md

 Tarefas:
 1. lib/ai/agent.py — Classe MigrationAI:
   - __init__(api_key, model) — carrega config do .env
   - suggest_schema_fix(diff_context: str) -> str — recebe diff de schema, retorna SQL sugerido
   - diagnose_error(error_context: str) -> str — recebe erro + contexto, retorna diagnostico
   - ask_with_approval(question, suggestion) -> bool — mostra sugestao ao DBA, espera Y/n/edit
   - Rate limiting, retry, token tracking
   - Carrega skills da pasta skills/ como contexto do system prompt
 2. lib/ai/prompts.py — Templates de prompts:
   - System prompt base (especialista em migracao FB→PG)
   - Template para schema diff fix
   - Template para erro de constraint
   - Template para erro de dados
 3. skills/ — 5 arquivos markdown com:
   - Padroes de erro conhecidos
   - Guardrails (nunca DROP DATABASE, nunca alterar dados ja migrados, etc.)
   - Regras de mapeamento FB→PG
   - Procedimentos de rollback

 ---
 Agente F: CLI + Monitor
 Arquivos: maestro.py, lib/cli.py, atualizacao de monitor.py

 Tarefas:
 1. lib/cli.py — Classe MaestroCLI com prompt_toolkit:
   - Prompt: maestro [MIGRACAO_0001] >>
   - Comandos: /init, /resume <SEQ>, /status, /precheck, /compare, /monitor, /run [step], /auto, /help, /quit
   - Autocomplete para comandos e args
   - Historico persistente (.maestro_history)
   - Coloracao de status (verde=ok, vermelho=erro, amarelo=running)
 2. maestro.py — Entry point:
   - Parse args iniciais (maestro.py ou maestro.py --resume 0001)
   - Inicializa CLI
   - Main loop
 3. monitor.py (patch) — Adicionar modo --migration-db <path>:
   - Le de MigrationDB (tabela tables) em vez de descobrir SQLite files
   - Painel unificado: big tables + small tables + steps + constraints
   - Manter backward compatibility com modo antigo

 ---
 Wave 3 — Integracao (1 agente)

 Agente G: Integracao + Testes

 Tarefas:
 1. Integrar todos os modulos: CLI chama StepRunner que executa Steps que usam MigrationDB
 2. Testar fluxo completo em dry-run:
   - /init → cria MIGRACAO_0001/
   - /precheck → verifica conectividade
   - /run → executa steps 0-13 em sequencia
   - /status → mostra progresso
   - /monitor → abre dashboard
 3. Corrigir conflitos de integracao
 4. Atualizar requirements.txt (adicionar prompt_toolkit, python-dotenv, anthropic)
 5. Atualizar CLAUDE.md com nova arquitetura

 ---
 Modificacoes nos Migrators Existentes

 Mudancas minimas necessarias nos 4 migrators (migrator_v2.py, migrator_parallel_doc_oper_v2.py, migrator_log_eventos_v2.py, migrator_smalltables_v2.py):

 1. Extrair StateManager para lib/state.py (codigo identico, apenas movido)
 2. Adicionar --master-db flag no argparse de cada migrator
 3. Se --master-db fornecido: StateManager grava no DB centralizado (tabela tables + batches) em vez de criar SQLite proprio
 4. Se --master-db NAO fornecido: comportamento atual (backward compatible)
 5. Adicionar --work-dir para gerar arquivos dentro de MIGRACAO_/ em vez do diretorio raiz

 Estas mudancas sao isoladas ao inicio (parse args) e ao StateManager. O core de migracao (COPY, BLOB, etc.) NAO muda.

 ---
 Arquivos Criticos (paths)

 Existentes (reusar/importar):

 - pg_constraints.py — ConstraintManager (importar diretamente)
 - compara_estrutura_fb2pg.py — funcoes de comparacao (importar)
 - compara_cont_fb2pg.py — row counts (importar)
 - PosMigracao_comparaChecksum_bytea.py — checksum (importar)
 - fix_blob_text_columns.py — fix blobs (importar)
 - gera_relatorio_compara_estrutura_fb2pg_html.py — report HTML (importar/adaptar)

 Existentes (modificar minimamente):

 - migrator_v2.py — adicionar --master-db, --work-dir, usar lib/state.py
 - migrator_parallel_doc_oper_v2.py — idem
 - migrator_log_eventos_v2.py — idem
 - migrator_smalltables_v2.py — idem
 - monitor.py — adicionar modo --migration-db

 Novos:

 - maestro.py + todo lib/ + skills/ + .env.example

 ---
 Verificacao

 Teste local (dry-run completo):

 # 1. Instalar novas dependencias
 pip install prompt_toolkit python-dotenv anthropic

 # 2. Iniciar Maestro
 python maestro.py

 # 3. No prompt do Maestro:
 /init                           # Cria MIGRACAO_0001/
 /precheck                       # Verifica conectividade (deve falhar local se nao tem FB/PG)
 /status                         # Mostra steps pendentes

 # 4. Testar no servidor com FB/PG:
 /init                           # Cria MIGRACAO_0001/ com config
 /precheck                       # Verifica FB + PG + schema.sql
 /run precheck                   # Roda step 0
 /run create_database            # Roda step 1
 /run import_schema              # Roda step 2
 /run compare_pre                # Roda step 3 (mostra diffs)
 /run --all                      # Roda steps 0-13 sequencialmente
 /monitor                        # Abre dashboard em outra janela
 /status                         # Verifica resultado

 Teste de integracao:

 # Migrator standalone (backward compatible)
 python migrator_v2.py --table CONTROLEVERSAO --dry-run
 # Deve funcionar exatamente como antes

 # Migrator com master DB
 python migrator_v2.py --table CONTROLEVERSAO --master-db MIGRACAO_0001/migration.db --dry-run
 # Deve gravar estado no DB centralizado

 Teste AI (se .env configurado):

 # No Maestro:
 /compare --tipo ALL             # Se diffs → AI sugere fix → prompt Y/n