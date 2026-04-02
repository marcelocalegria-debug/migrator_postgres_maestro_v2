---
status: awaiting_human_verify
trigger: "monitor.py mostra AGUARDANDO (0%) para tabela ocorrencia_sisat, mas migrator.py está migrando ela ativamente conforme log"
created: 2026-04-02T00:00:00
updated: 2026-04-02T00:15:00
---

## Current Focus

hypothesis: monitor.py usa BASE_DIR (diretório do script) para descobrir os .db files, mas migrator.py cria os .db files no CWD (diretório de trabalho atual), que é /migracao_firebird/. São diretórios diferentes.
test: Confirmado - nenhum migration_state_*.db existe em C:/Python/Migracao_Fire2pg_c6_prod/. O chama_migrator.sh faz cd /migracao_firebird antes de rodar o migrator.
expecting: fix = fazer monitor.py e migrator.py concordarem no diretório dos .db files
next_action: Aplicar fix em migrator.py para criar .db files no diretório do script (BASE_DIR), igual ao monitor.py

## Symptoms

expected: monitor.py deve mostrar progresso em tempo real da migração de ocorrencia_sisat (percentual, linhas, ETA, velocidade)
actual: monitor.py mostra status "AGUARDANDO" com 0.00% para ocorrencia_sisat enquanto o migrator.py está processando ~4% da tabela
errors: Nenhum erro explícito. Somente status incorreto na UI do monitor.
reproduction: Rodar monitor.py enquanto migrator.py está migrando ocorrencia_sisat. A tabela aparece como AGUARDANDO em vez de mostrar o progresso real.
timeline: Bug em produção agora.

## Eliminated

- hypothesis: Falha de parse do formato de log do ocorrencia_sisat
  evidence: Monitor não lê arquivos de log de texto. Lê SQLite .db files. Não é questão de parsing de log.
  timestamp: 2026-04-02T00:05:00

- hypothesis: Case sensitivity no nome do arquivo de log
  evidence: Não é log de texto - é SQLite. Não se aplica.
  timestamp: 2026-04-02T00:05:00

- hypothesis: Lógica de correspondência de nome de tabela no monitor
  evidence: O problema é anterior - os .db files simplesmente não estão no diretório onde monitor.py os procura.
  timestamp: 2026-04-02T00:10:00

- hypothesis: Estado/checkpoint de execução anterior interferindo
  evidence: Não há nenhum .db file presente para causar conflito.
  timestamp: 2026-04-02T00:10:00

## Evidence

- timestamp: 2026-04-02T00:03:00
  checked: monitor.py linha 38 e função _discover_dbs()
  found: BASE_DIR = Path(__file__).parent (diretório do script). _discover_dbs() faz glob('migration_state_*.db') em BASE_DIR.
  implication: Monitor só lê .db files que estejam no mesmo diretório que monitor.py

- timestamp: 2026-04-02T00:05:00
  checked: ls /c/Python/Migracao_Fire2pg_c6_prod/migration_state_*.db
  found: "No .db files found" - nenhum arquivo migration_state_*.db existe no diretório do script
  implication: Monitor não encontra nenhum state file => mostra AGUARDANDO para todos

- timestamp: 2026-04-02T00:07:00
  checked: migrator.py StateManager.__init__ e _resolve_tables()
  found: state_db é apenas um nome de arquivo relativo como 'migration_state_ocorrencia_sisat.db'. sqlite3.connect() sem path absoluto cria o arquivo no CWD.
  implication: migrator.py cria os .db files no diretório de trabalho corrente, não no diretório do script.

- timestamp: 2026-04-02T00:09:00
  checked: chama_migrator.sh linha 26
  found: cd "$DIRETORIO_BASE" onde DIRETORIO_BASE="/migracao_firebird". Depois: python migrator.py --table OCORRENCIA_SISAT
  implication: CWD quando migrator.py roda é /migracao_firebird/. Os .db files são criados lá.

- timestamp: 2026-04-02T00:11:00
  checked: LEMBRETES_MIGRATOR.txt
  found: Confirma que todos os comandos usam cd /migracao_firebird/ antes do python migrator.py
  implication: Todos os state .db files estão em /migracao_firebird/, mas monitor.py os procura em C:/Python/Migracao_Fire2pg_c6_prod/

- timestamp: 2026-04-02T00:12:00
  checked: migrator.py linha 411 e 477
  found: state_db = f'migration_state_{table.lower()}.db' - path relativo simples
  implication: ROOT CAUSE CONFIRMADO. O fix deve tornar o path absoluto, relativo ao diretório do script em migrator.py

## Resolution

root_cause: migrator.py cria os arquivos migration_state_*.db no diretório de trabalho corrente (CWD=/migracao_firebird/) enquanto monitor.py os procura em BASE_DIR (diretório do script = C:/Python/Migracao_Fire2pg_c6_prod/). São diretórios diferentes, então monitor nunca encontra os .db files e exibe AGUARDANDO para todas as tabelas.

fix: Adicionado BASE_DIR = Path(__file__).parent em migrator.py (linha 77). Todas as três origens de state_db agora usam caminhos absolutos: str(BASE_DIR / f'migration_state_{name}.db') — para --table override (linha 413), para tables: list (linha 479), e para backward compat (linha 491). Assim migrator.py e monitor.py sempre concordam no mesmo diretório.

verification: Fix aplicado. Aguardando confirmação humana de que monitor.py passa a mostrar o progresso correto.
files_changed: [migrator.py]
