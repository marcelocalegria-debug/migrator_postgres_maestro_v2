# Instalacao no linux

source .venv/bin/activate
export PYTHONIOENCODING=utf-8
export LANG=C.UTF-8
export LC_ALL=C.UTF-8

## SERVIDOR EQUINIX FIREBIRD  - 10.3.98.143  (NOVO PARA TESTES)

postgres@postgres-server-sp2:~$ sudo mkdir /migracao_maestro_v2
postgres@postgres-server-sp2:~$ sudo chown -R postgres:postgres /migracao_maestro_v2
postgres@postgres-server-sp2:~$ sudo chmod o+w -R postgres:postgres /migracao_maestro_v2
postgres@postgres-server-sp2:~$ sudo chmod o+w -R /migracao_maestro_v2


*** Instalar o python 3 SEPARADAMENTE do python do Linxu!!!! CRITICO PARA NÃO CORROMPER O Python nativo do linux e quebrar o servidor Linux ***


postgres@postgres-server-sp2:~$ vi .bashrc

postgres@postgres-server-sp2:~$ cat .bashrc
export PATH="/opt/python313/bin:$PATH"
#cd /migracao_maestro_v2/
#source .venv/bin/activate
export PYTHONIOENCODING=utf-8
export LANG=C.UTF-8
export LC_ALL=C.UTF-8

postgres@postgres-server-sp2:~$ whereis python3
python3: /usr/bin/python3 /usr/lib/python3 /etc/python3 /usr/share/python3 /opt/python313/bin/python3 /usr/share/man/man1/python3.1.gz

sudo curl -LsSf https://astral.sh/uv/install.sh | sh

postgres@postgres-server-sp2:~$ vi .bashrc
postgres@postgres-server-sp2:~$ cat .bashrc
export PATH="/opt/python313/bin:$PATH"
#cd /migracao_firebird/
#source .venv/bin/activate
export PYTHONIOENCODING=utf-8
export LANG=C.UTF-8
export LC_ALL=C.UTF-8

. "$HOME/.local/bin/env"

postgres@postgres-server-sp2:~$ source .bashrc

postgres@postgres-server-sp2:~$ uv
An extremely fast Python package manager.

Usage: uv [OPTIONS] <COMMAND>

Commands:
  auth     Manage authentication

- cria o ambiente venv com uv

uv init
uv venv .venv

- Testa se o python está ok

     export LANG=C.UTF-8
     export LC_ALL=C.UTF-8
     export PYTHONIOENCODING=utf-8
     source .venv/bin/activate

- Copiar toda a pasta do projeto, exceto .venv , __pycache__, .vscode, .planning, .claude para o linux na pasta /migracao_maestro_v2

- Instalar as libs do projeto: 

(.venv) postgres@postgres-server-sp2:/migracao_maestro_v2$ source .venv/bin/activate
(.venv) postgres@postgres-server-sp2:/migracao_maestro_v2$
(.venv) postgres@postgres-server-sp2:/migracao_maestro_v2$
(.venv) postgres@postgres-server-sp2:/migracao_maestro_v2$ python --version
Python 3.13.0
(.venv) postgres@postgres-server-sp2:/migracao_maestro_v2$ whereis python
python: /migracao_maestro_v2/.venv/bin/python
(.venv) postgres@postgres-server-sp2:/migracao_maestro_v2$

 uv add -r requirements.txt

Resolved 146 packages in 80ms
error: Distribution `pywin32==311 @ registry+https://pypi.org/simple` can't be installed because it doesn't have a source distribution or wheel for the current platform

hint: You're on Linux (`manylinux_2_41_x86_64`), but `pywin32` (v311) only has wheels for the following platforms: `win32`, `win_amd64`, `win_arm64`; consider adding "sys_platform == 'linux' and platform_machine == 'x86_64'" to `tool.uv.required-environments` to ensure uv resolves to a version with compatible wheels

SOLUÇÃO -  remover a linha que contém pywin32. Depois, execute o comando novamente:

(.venv) postgres@postgres-server-sp2:/migracao_maestro_v2$ vi requirements.txt

(.venv) postgres@postgres-server-sp2:/migracao_maestro_v2$ uv add -r requirements.txt




# Verifica se o firebird está instalado e disponível

(.venv) postgres@postgres-server-sp2:/migracao_firebird$ ls -alrt /databases/scci.gdb
total 21231600
-rw-r--r--  1 root root          13 Mar 31 10:40 senhabd-firebird
drwxr-xr-x 21 root root        4096 Apr  4 13:41 ..
drwxr-xr-x  2 root root        4096 Apr  7 18:01 .
-rw-r-----  1 root root 21741142016 Apr  8 13:50 scci.gdb
Bstikqxd_yat


sudo su - postgres

cd /migracao_firebird
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONIOENCODING=utf-8
source .venv/bin/activate



###  O que o projeto precisa no Linux (Firebird client)

  O driver Python fdb precisa da biblioteca compartilhada do Firebird Client. No Linux, ela se chama libfbclient.so (não .dll). Os scripts só carregam fbclient.dll
  quando os.name == 'nt' (Windows), então no Linux o fdb procura automaticamente a libfbclient.so pelo LD_LIBRARY_PATH ou nos caminhos padrão do sistema.

  Como instalar no Debian/Ubuntu (servidor Linux):

  # Opção 1 — via apt (Firebird 3, recomendado para compatibilidade com fdb)
  sudo apt-get update
  sudo apt-get install firebird3.0-utils libfbclient2

  # Verifica se a lib foi instalada
  ldconfig -p | grep libfbclient

  Se o servidor já roda o Firebird localmente (o .fdb está em /firebird/data/c6emb.fdb), a libfbclient.so já deve estar instalada. Confirme com:

  find /usr /opt /firebird -name "libfbclient.so*" 2>/dev/null

  ---
  Os plugins são necessários para a migração?

  ┌────────────────────────┬─────────────────────────────────────────────┬────────────────────────────────────────────────┐
  │         Plugin         │             Função no Firebird              │           Necessário para migração?            │
  ├────────────────────────┼─────────────────────────────────────────────┼────────────────────────────────────────────────┤
  │ legacy_usermanager.dll │ Gerenciamento de usuários (modo legado)     │ Não — só importa conexão de leitura            │
  ├────────────────────────┼─────────────────────────────────────────────┼────────────────────────────────────────────────┤
  │ udr_engine.dll         │ UDR (User Defined Routines) em DLL externas │ Não — a migração só lê dados, não executa UDRs │
  └────────────────────────┴─────────────────────────────────────────────┴────────────────────────────────────────────────┘

  Esses plugins são do servidor Firebird, não do cliente. O migrator só usa o cliente (driver fdb + libfbclient.so) para ler dados via SQL. Plugins de servidor
  ficam na máquina onde o Firebird Server roda — não na máquina que faz a migração.

  ---
  Resumo do que copiar para o Linux

  ┌────────────────────────────────────────────┬────────────────────────┐
  │                O que copiar                │      Necessário?       │
  ├────────────────────────────────────────────┼────────────────────────┤
  │ Código do projeto (.py, config.yaml, etc.) │ Sim                    │
  ├────────────────────────────────────────────┼────────────────────────┤
  │ fbclient.dll (Windows)                     │ Não                    │
  ├────────────────────────────────────────────┼────────────────────────┤
  │ fb\plugins\*.dll                           │ Não                    │
  ├────────────────────────────────────────────┼────────────────────────┤
  │ libfbclient.so no Linux                    │ Sim (instalar via apt) │
  └────────────────────────────────────────────┴────────────────────────┘

# Mini Manual — Referência de Comandos

     Instalação e ambiente

     # Instalar dependências (preferir uv)
     uv sync
     # ou
     pip install -r requirements.txt

     # Ativar venv (Linux/Mac)
     source .venv/bin/activate

     # Ativar venv (Windows)
     . .venv/Scripts/activate

     Maestro V2 — Orquestrador principal

     # Iniciar (auto-resume da última migração)
     python maestro.py

     # Resume explícito de uma migração
     python maestro.py --resume 0005

     Comandos dentro do Maestro:

     ┌──────────────┬────────────────────────────────────────────────────────────────────┐
|   Comando    │                             O que faz                         |
     ├──────────────┼────────────────────────────────────────────────────────────────────┤
| /init   | Cria nova migração MIGRACAO_<SEQ>/, copia config.yaml         |
     ├──────────────┼────────────────────────────────────────────────────────────────────┤
| /resume 0005 │ Carrega migração 0005 (alias: /load 0005)                     |
     ├──────────────┼────────────────────────────────────────────────────────────────────┤
| /status | Mostra steps S00–S13 com status, duração e tabelas            |
     ├──────────────┼────────────────────────────────────────────────────────────────────┤
| /check  | Valida conexões FB e PG (equivale a S00 isolado)              |
     ├──────────────┼────────────────────────────────────────────────────────────────────┤
| /compare| Roda comparação estrutural FB×PG e abre relatório HTML        |
     ├──────────────┼────────────────────────────────────────────────────────────────────┤
| /run    | Executa pipeline completo a partir do primeiro step pendente  |
     ├──────────────┼────────────────────────────────────────────────────────────────────┤
| /run 6  | Executa a partir do step 6 (migrate_big)                      |
     ├──────────────┼────────────────────────────────────────────────────────────────────┤
| /rerun 7| Força re-execução do step 7 mesmo se já completed             |
     ├──────────────┼────────────────────────────────────────────────────────────────────┤
| /monitor| Abre monitor Rich TUI em tempo real                           |
     ├──────────────┼────────────────────────────────────────────────────────────────────┤
| /agent  | Abre chat com agente IA (diagnóstico e diff de schema)        |
     ├──────────────┼────────────────────────────────────────────────────────────────────┤
| /quit   | Sai (Ctrl+C também funciona — matar subprocessos manualmente após) │
     └──────────────┴────────────────────────────────────────────────────────────────────┘

     Steps do pipeline

     ┌──────┬─────────────────────┬──────────────────────────────────────────────────────────────────┐
| Step │        Nome    |                            O que faz                        |
     ├──────┼─────────────────────┼──────────────────────────────────────────────────────────────────┤
| S00  │ PRECHECK       | Conectividade FB+PG, Python version, disk space             |
     ├──────┼─────────────────────┼──────────────────────────────────────────────────────────────────┤
| S01  │ CREATE_DATABASE| Valida parâmetros (DBA cria banco manualmente via SQL fornecido) │
     ├──────┼─────────────────────┼──────────────────────────────────────────────────────────────────┤
| S02  │ IMPORT_SCHEMA  | Aplica DDL .sql no PostgreSQL via psql                      |
     ├──────┼─────────────────────┼──────────────────────────────────────────────────────────────────┤
| S03  │ COMPARE_PRE    | Compara estrutura FB×PG, aciona agente IA se diferenças     |
     ├──────┼─────────────────────┼──────────────────────────────────────────────────────────────────┤
| S05  │ DISABLE_CONSTRAINTS │ Remove FK/PK/índices/triggers via pg_constraints.py         |
     ├──────┼─────────────────────┼──────────────────────────────────────────────────────────────────┤
| S06  │ MIGRATE_BIG    | Migra 10 tabelas grandes em paralelo (subprocess)           |
     ├──────┼─────────────────────┼──────────────────────────────────────────────────────────────────┤
| S07  │ MIGRATE_SMALL  | Migra ~901 tabelas pequenas (ProcessPoolExecutor)           |
     ├──────┼─────────────────────┼──────────────────────────────────────────────────────────────────┤
| S08  │ ENABLE_CONSTRAINTS  │ Re-habilita FK/PK/índices na ordem certa                    |
     ├──────┼─────────────────────┼──────────────────────────────────────────────────────────────────┤
| S09  │ SEQUENCES      | Ajusta sequences PostgreSQL para max(PK) do Firebird        |
     ├──────┼─────────────────────┼──────────────────────────────────────────────────────────────────┤
| S10  │ COMPARE_POST   | Comparação estrutural pós-carga                             |
     ├──────┼─────────────────────┼──────────────────────────────────────────────────────────────────┤
| S11  │ VALIDATE       | Count comparison FB×PG                                      |
     ├──────┼─────────────────────┼──────────────────────────────────────────────────────────────────┤
| S12  │ ANALYZE        | ANALYZE VERBOSE no PostgreSQL                               |
     ├──────┼─────────────────────┼──────────────────────────────────────────────────────────────────┤
| S13  │ REPORT         | Gera relatório HTML final                                   |
     └──────┴─────────────────────┴──────────────────────────────────────────────────────────────────┘

     Migrators standalone (linha de comando direta)

     # Tabela individual (seq, com checkpoint/restart)
     python migrator_v2.py --table OPERACAO_CREDITO
     python migrator_v2.py --table OPERACAO_CREDITO --reset         # reinicia do zero
     python migrator_v2.py --table OPERACAO_CREDITO --dry-run       # sem escrita PG
     python migrator_v2.py --table OPERACAO_CREDITO --batch-size 5000
     python migrator_v2.py --table OPERACAO_CREDITO --use-insert    # COPY → INSERT

     # DOCUMENTO_OPERACAO (paralelo por range de PK)
     python migrator_parallel_doc_oper_v2.py --threads 4

     # LOG_EVENTOS (paralelo por RDB$DB_KEY, sem PK)
     python migrator_log_eventos_v2.py --threads 8
     python migrator_log_eventos_v2.py --dry-run
     python migrator_log_eventos_v2.py --reset

     # ~901 tabelas pequenas (ProcessPoolExecutor)
     python migrator_smalltables_v2.py --small-tables
     python migrator_smalltables_v2.py --small-tables --workers 6
     python migrator_smalltables_v2.py --small-tables --dry-run
     python migrator_smalltables_v2.py --small-tables --reset

     Monitor

     # Monitor da migração ativa (migration.db)
     python monitor_oldschool_v2_updated.py MIGRACAO_0005
     python monitor_oldschool_v2_updated.py MIGRACAO_0005 --big-tables
     python monitor_oldschool_v2_updated.py MIGRACAO_0005 --small-tables

     Validação e comparação

     # Comparação estrutural (PKs, FKs, índices, constraints)
     python compara_estrutura_fb2pg.py
     python compara_estrutura_fb2pg.py --verbose

     # Contagem de rows (todas as tabelas)
     python compara_cont_fb2pg.py

     # Checksum BYTEA/BLOB (verificação de integridade binária)
     python PosMigracao_comparaChecksum_bytea.py --table OPERACAO_CREDITO
     python PosMigracao_comparaChecksum_bytea.py --workers 1   # 1 de cada vez (evita OOM)

     # Relatório HTML consolidado
     python gera_relatorio_compara_estrutura_fb2pg_html.py --output relatorio_final.html

     Re-enable de constraints (emergência)

     # Se pipeline foi interrompido após disable_constraints
     python enable_constraints.py

     # Ou manualmente via psql (usando o SQL gerado)
     psql -h HOST -U USER -d DB -f MIGRACAO_0005/sql/enable_constraints_TABELA.sql

     Processos órfãos (após Ctrl+C)

     # Linux: matar todos os migrators
     pkill -f "python.*migrator"

     # Verificar o que ainda está rodando
     ps aux | grep -E "migrator|maestro"

     # Windows: usar Task Manager → python.exe → End Task

     Arquivo de configuração chave

     MIGRACAO_<SEQ>/config.yaml (cópia criada pelo /init):
     - firebird.host/port/database/user/password/charset
     - postgresql.host/port/database/user/password
     - migration.batch_size — linhas por lote COPY (padrão 10.000; reduzir para tabelas com BLOBs grandes)
     - migration.parallel_workers — workers do ProcessPoolExecutor (padrão 4)
     - migration.exclude_tables — tabelas excluídas do migrator_smalltables (as 10 grandes)

     Troubleshooting rápido


┌────────────────────────────────┬────────────────────────────────┬───────────────────────────────────┐
|            Sintoma             |         Causa provável         |               Ação                |
├────────────────────────────────┼────────────────────────────────┼───────────────────────────────────┤
| database is locked em SQLite   │ Múltiplos workers contestando  │ Reduzir parallel_workers          |
├────────────────────────────────┼────────────────────────────────┼───────────────────────────────────┤
| Processo parado sem progresso  │ Thread travada (join sem       | pkill + /rerun <step>             |
| no monitor                     | timeout)                       |                                   |
├────────────────────────────────┼────────────────────────────────┼───────────────────────────────────┤
| FK violation ao re-enable      | Dados migrados com        | Verificar log                          │
| constraints                    | inconsistência            | enable_constraints_*.log               │
├────────────────────────────────┼────────────────────────────────┼───────────────────────────────────┤
| OOM durante migrate_small      | BLOB grande × batch_size alto  │ Reduzir batch_size para           │
|                                |                                | 1.000–2.000                       |
├────────────────────────────────┼────────────────────────────────┼───────────────────────────────────┤
| Step marcado completed mas| S06 marcou S07 como done            | /rerun 7                          │
| dados faltam              | prematuramente                      |                                   │
├────────────────────────────────┼────────────────────────────────┼───────────────────────────────────┤
| Constraints desabilitadas após │ S05 OK, S06 falhou, S08 não    │ python enable_constraints.py      │
|  falha                         | rodou                          |                                   |
├────────────────────────────────┼────────────────────────────────┼───────────────────────────────────┤
│ Agente IA não responde         | Timeout OpenRouter             | /quit + reiniciar agente          │
│                                |                                | manualmente                       |
└────────────────────────────────┴────────────────────────────────┴───────────────────────────────────┘

     ---
     Verificação após implementação dos ajustes

     1. Executar python maestro.py → /init → /check — deve passar em todos os pré-requisitos
     2. Simular kill durante S06: kill -9 <PID do migrator> → reiniciar → verificar que checkpoint funciona
     sem duplicação
     3. Verificar log do MCP após query malformada: exception não deve conter password
     4. Confirmar que /rerun pede confirmação antes de deletar arquivos de estado
     5. Confirmar que monitor exibe alerta se thread sem progresso > 5 min
     6. python compara_cont_fb2pg.py + python compara_estrutura_fb2pg.py — diff deve ser zero após migração
     completa



# TROUBLESHOOTING

DUVIDA: Deu falha na migração das small-tables e o processo caiu. Eu consigo reiniciar só ele pelo maestro.py em outro terminal?  A migração das big-tables está rolando em um outro

RESP:

  Não abra um segundo maestro.py na mesma pasta — ele ia conflitar no migration.db (state management do pipeline
  inteiro).

  Mas você pode rodar o migrator_smalltables_v2.py diretamente, como standalone. Ele está projetado para isso. O
  StateManager detecta automaticamente o migration.db e já faz checkpoint/restart — tabelas completas serão puladas.

  Confirma o migration-id primeiro:

  sqlite3 MIGRACAO_0001/migration.db "SELECT id FROM migrations LIMIT 1"

  Depois roda direto no Linux:

  uv run migrator_smalltables_v2.py \
    --work-dir MIGRACAO_0001 \
    --small-tables \
    --master-db MIGRACAO_0001/migration.db \
    --migration-id 1

  Com --master-db apontando pro mesmo migration.db, o monitor vai mostrar o progresso normalmente. Sem ele, a migração
  funciona mas cria arquivos SQLite separados por tabela e o monitor não exibe.

  O SQLite no modo WAL aguenta os dois processos escrevendo em paralelo (big tables e small tables escrevem em linhas
  diferentes da tabela tables), então não há risco de corrupção.

---

  DUVIDA: SE PRECISAR FAZER A RECARGA DE UMA UNICA TABELA?

  RESP: python3 migrator_v2.py --work-dir MIGRACAO_0001 --table CEP --reset


---

DUVIDA: Qual a diferença do /rerun para o /run ?

RESP:  A diferença reside em como o Maestro lida com o estado (progresso) de cada passo:

  1. /run [step_number]
   * Comportamento: É o modo "inteligente" ou "incremental".
   * O que faz: Ele verifica se o passo solicitado já foi concluído anteriormente (status completed).
       * Se já estiver concluído, ele pula o passo automaticamente.
       * Se estiver pendente ou falhou, ele inicia a execução.
   * Uso Ideal: Para continuar a migração de onde ela parou ou seguir o fluxo normal sem repetir trabalho já feito.

  2. /rerun [step_number]
   * Comportamento: É o modo "forçar execução" ou "reset".
   * O que faz: Ele ignora o status atual do passo. Antes de iniciar a execução, ele limpa qualquer registro de sucesso ou falha anterior no banco de dados e
     força o script a começar do zero.
       * No caso de dados (Passo 5/6), ele instrui os migradores a executarem o comando de limpeza (TRUNCATE) e recarregar tudo.
   * Uso Ideal: Quando você corrigiu algum erro no código ou nos dados de origem e quer que aquele passo específico seja totalmente refeito, garantindo que nada
     de execuções anteriores interfira.
