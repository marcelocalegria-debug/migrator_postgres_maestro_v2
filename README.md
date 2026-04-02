migrator.py — Guia de Operação
Ferramenta de migração de dados Firebird 3 → PostgreSQL com suporte a restart por checkpoint, gerenciamento automático de constraints/índices e inserção via protocolo COPY.

Visão Geral
O migrador opera em 3 fases para cada execução:

Fase	Nome	O que faz
0	Coleta e desabilitação	Lê todas as constraints/PKs/índices/triggers do PostgreSQL, salva estado em JSON e scripts SQL, e remove tudo da tabela destino para acelerar a carga
1	Carga de dados	Transfere as linhas do Firebird para o PostgreSQL via protocolo COPY (padrão) ou INSERT; suporta restart por checkpoint
Pós	Reabilitação manual	Recriar constraints/índices/triggers via scripts SQL gerados na Fase 0
Importante: A Fase 2 (reabilitação) não é executada automaticamente pelo migrador. Ela deve ser executada manualmente via scripts enable_constraints_*.sql após confirmar que a carga foi bem-sucedida.

Pré-requisitos
Python 3.11+
fdb (driver Firebird), psycopg2-binary, PyYAML, rich
fbclient.dll no diretório do script ou em C:\Program Files\Firebird\Firebird_X_X\
Tabelas já criadas no PostgreSQL (o migrador não cria o schema)
Instalar dependências:

pip install -r requirements.txt
# ou
uv sync
Configuração (config.yaml)
firebird:
  host: "localhost"
  port: 3050
  database: "/firebird/data/c6emb.fdb"
  user: "SYSDBA"
  password: "masterkey"
  charset: "WIN1252"

postgresql:
  host: "localhost"
  port: 5435
  database: "c6_producao"
  user: "c6_producao_user"
  password: "senha"
  schema: "public"

migration:
  tables:
    - source: "LOG_EVENTOS"       # nome no Firebird (MAIÚSCULAS)
      dest: "log_eventos"         # nome no PostgreSQL (minúsculas)
  batch_size: 10000
  fetch_array_size: 10000
  max_retries: 3
  retry_delay_seconds: 3

performance:
  work_mem: "256MB"
  maintenance_work_mem: "1GB"

logging:
  level: "INFO"
  file: "migration.log"
  console: true
Para migrar múltiplas tabelas, liste todas em migration.tables. O migrador as processa sequencialmente.

Arquivos Gerados por Tabela
Para cada tabela {dest} na lista, o migrador gera:

Arquivo	Descrição
constraint_state_{dest}.json	Estado completo de constraints/PKs/índices/triggers (usado para recriar)
disable_constraints_{dest}.sql	Script para remover constraints (executado automaticamente na Fase 0)
enable_constraints_{dest}.sql	Script para recriar constraints (executado manualmente no pós-migração)
migration_state_{dest}.db	Banco SQLite com checkpoint e histórico de batches
migration_{dest}.log	Log detalhado da carga quando --table é usado
Como Executar
Execução padrão (tabela do config.yaml)
python migrator.py
Tabela específica via CLI (recomendado)
Sobrescreve a lista do config.yaml. Útil para executar tabelas individualmente:

python migrator.py --table OPERACAO_CREDITO
O nome da tabela é em MAIÚSCULAS (Firebird). A conversão para minúsculas (PostgreSQL) é automática.

Modo dry-run (sem gravar dados)
Conta linhas e calcula batches sem escrever nada no PostgreSQL:

python migrator.py --table OPERACAO_CREDITO --dry-run
Apenas gerar scripts SQL
Gera os arquivos disable_constraints_*.sql, enable_constraints_*.sql e constraint_state_*.json sem desabilitar nada nem migrar dados:

python migrator.py --table OPERACAO_CREDITO --generate-scripts-only
# ou para todas as tabelas do config:
python migrator.py --generate-scripts-only
Reiniciar do zero (descartar checkpoint)
python migrator.py --table OPERACAO_CREDITO --reset
Alterar tamanho de batch
python migrator.py --table OPERACAO_CREDITO --batch-size 5000
Usar INSERT em vez de COPY
COPY é o padrão (3-5× mais rápido). Use INSERT apenas se houver problemas de compatibilidade:

python migrator.py --table OPERACAO_CREDITO --use-insert
Arquivo de log personalizado
python migrator.py --table OPERACAO_CREDITO --log-file minha_migracao.log
Quando --table é usado sem --log-file, o log é gravado automaticamente em migration_{tabela}.log.

Restart Automático (Checkpoint)
Se a migração for interrompida (Ctrl+C, queda de conexão, etc.), ao reexecutar o mesmo comando o migrador detecta o checkpoint salvo e pergunta:

  Continuar [OPERACAO_CREDITO] do checkpoint? (s/n):
s — retoma do último batch confirmado
n — descarta o checkpoint e recomeça do zero (trunca a tabela)
O checkpoint usa:

PK simples: WHERE pk > ultimo_valor ORDER BY pk
PK composta: expansão OR — (a > ?) OR (a = ? AND b > ?) OR ...
Sem PK: RDB$DB_KEY do Firebird como cursor posicional
O que Acontece com Constraints, PKs e Índices
Fase 0 — Desabilitação (automática)
Para cada tabela na lista, antes de iniciar a carga:

Coleta todos os objetos dependentes via pg_catalog:

Foreign Keys de tabelas filhas que referenciam a tabela destino
Foreign Keys próprias da tabela (referenciando outras tabelas)
Primary Key
Unique constraints
Check constraints
Índices explícitos (não criados por constraint)
Triggers do usuário
Salva o estado completo em constraint_state_{dest}.json com os SQLs de DROP e CREATE

Gera os scripts disable_constraints_{dest}.sql e enable_constraints_{dest}.sql

Remove todos os objetos coletados do PostgreSQL (DROP)

O objetivo é eliminar overhead de validação durante a carga em massa, reduzindo o tempo de inserção em ordens de magnitude para tabelas grandes.

Fase 1 — Carga
Com constraints desabilitadas, a inserção usa o protocolo COPY do PostgreSQL com:

SET synchronous_commit = off (WAL assíncrono)
SET jit = off (evita overhead de JIT em INSERTs simples)
autovacuum_enabled = false na tabela destino (se o usuário tiver permissão)
Pós-migração — Reabilitação (manual)
Após confirmar que todas as tabelas foram carregadas corretamente, recriar as constraints na ordem correta:

Índices explícitos
Primary Key
Unique constraints
Check constraints
Foreign Keys próprias
Foreign Keys de tabelas filhas
Triggers
Os scripts enable_constraints_*.sql já estão ordenados corretamente.

Pós-migração: Reabilitação de Constraints
Execute no PowerShell na ordem abaixo (respeita dependências entre tabelas):

$env:PGCLIENTENCODING = "latin1"
$env:PGPASSWORD = "senha_prod"

psql -p 5435 c6_producao -U postgres -f enable_constraints_ocorrencia.sql
psql -p 5435 c6_producao -U postgres -f enable_constraints_controleversao.sql
psql -p 5435 c6_producao -U postgres -f enable_constraints_documento_operacao.sql
psql -p 5435 c6_producao -U postgres -f enable_constraints_historico_operacao.sql
psql -p 5435 c6_producao -U postgres -f enable_constraints_log_eventos.sql
psql -p 5435 c6_producao -U postgres -f enable_constraints_nmov.sql
psql -p 5435 c6_producao -U postgres -f enable_constraints_ocorrencia.sql
psql -p 5435 c6_producao -U postgres -f enable_constraints_ocorrencia_sisat.sql
psql -p 5435 c6_producao -U postgres -f enable_constraints_operacao_credito.sql
psql -p 5435 c6_producao -U postgres -f enable_constraints_parcelasctb.sql
psql -p 5435 c6_producao -U postgres -f enable_constraints_pessoa_pretendente.sql
Atenção: $env:PGCLIENTENCODING = "latin1" é necessário para que o psql interprete corretamente os nomes de objetos com acentos/caracteres especiais gerados nos scripts SQL.

Cada script executa dentro de uma transação (BEGIN/COMMIT) e ao final executa ANALYZE e REINDEX TABLE na tabela.

Se um script falhar (ex.: violação de FK por dados inconsistentes), corrija os dados antes de reexecutar. Os scripts são idempotentes para drops (DROP ... IF EXISTS) mas o ADD CONSTRAINT falhará se a constraint já existir — remova-a manualmente antes de reexecutar nesse caso.

Monitoramento em Tempo Real
# Monitorar tabela em migração (aponte para o .db correto)
python monitor.py --state-db migration_state_operacao_credito.db

# Resumo rápido
python monitor.py --state-db migration_state_operacao_credito.db --summary

# Ver histórico dos últimos 50 batches
python monitor.py --state-db migration_state_operacao_credito.db --history 50

# Ver status de constraints de todas as tabelas
python monitor.py --constraints

# Refresh a cada 5 segundos (padrão: 2s)
python monitor.py --state-db migration_state_operacao_credito.db -i 5
O monitor.py monitora uma tabela por vez — como a migração é sequencial, aponte para o .db da tabela que está em carga no momento.

Mapeamento de Tipos Firebird → PostgreSQL
Firebird	PostgreSQL
SMALLINT	SMALLINT
INTEGER	INTEGER
BIGINT	BIGINT
NUMERIC(p,s)	NUMERIC(p,s)
REAL	REAL
DOUBLE PRECISION	DOUBLE PRECISION
CHAR(n)	CHAR(n)
VARCHAR(n)	VARCHAR(n)
DATE	DATE
TIME	TIME
TIMESTAMP	TIMESTAMP
BLOB SUB_TYPE TEXT	TEXT (decodificado WIN1252 → UTF-8)
BLOB SUB_TYPE BINARY	BYTEA
Campos DADO, TE_IMAGEM_REDUZIDA e IMAGEM são forçados para BYTEA independentemente do subtipo declarado.

Utilitários
repair_fk_scripts.py
Corrige arquivos constraint_state_*.json e enable_constraints_*.sql que foram gerados com FKs compostas duplicadas (colunas repetidas). Conecta ao Firebird como fonte autoritativa das definições de FK:

python repair_fk_scripts.py
pg_constraints.py
Módulo interno usado pelo migrator.py. Gerencia todo o ciclo de vida de constraints/índices/triggers no PostgreSQL. Usa pg_catalog (em vez de information_schema) para consultas de FK, evitando produto cartesiano em FKs compostas.

Referência de Argumentos CLI
Argumento	Padrão	Descrição
-c, --config	config.yaml	Arquivo de configuração
--table NOME	(do config)	Tabela a migrar (MAIÚSCULAS). Sobrescreve config.yaml
--log-file ARQUIVO	migration_{tabela}.log	Arquivo de log
--reset	false	Descarta checkpoint e reinicia do zero
--dry-run	false	Mostra estatísticas sem gravar dados
--generate-scripts-only	false	Gera apenas os scripts SQL de constraints
--batch-size N	(do config)	Linhas por batch
--use-insert	false	Usa INSERT em vez de COPY
