# Projeto Migrator V2 - Fork do projeto original

- Este projeto foi comcebido para fazer uma nova versão do Migrator para automatizar o máximo possivel o processo completo de migração do Firebird para o Postgres. Atualmente existe um script para o DBA que irá conduzir seguir, passo a passo com cada etapa. Iniciando os scripts python de migração, criando o banco Postgres, fazendo o import da estrutura (schema-only) do banco postgres com as tabelas, pks, indices, fks, checks e tudo mais, menos os dados, na mesma versão do sistema SCCI.

- Tudo deve ser mantido em Python

- Considere a utilização de Subprocesses do Python para disparo em paralelo dos scripts de migração dos dados.

- É possível a utilização de Agentes de IA, obrigatóriamente com "Humman in the loop" - Em caso de erros ou decisões criticos na migração.  Crie 2 MCPs (cuidado com os prints dos outros scripts para não atrapalhar o STDIO ou use HTTP/SSE) um para conectar com o firebird e outro com o Postgres, caso necessário. Seria MUITO IMPORTANTE ter uma pasta de SKILL para que o agente possa consultar o que pode/deve fazer em casos de erro e guardrails de segurança. A API_URL, API_KEY e MODEL deverão ficar no .env . Pode considerar o uso dos modelos da Antropic mas também de outros modelos. Pode usar o framework de agente que voce achar mais adequado para a tarefa. 

- A utilização de agentes de IA só deverá ocorrer em pontos onde a programação do Maestro.py pode ficar muito exaustiva e com risco de qualidade no resultado. Por exemplo, para correção de erros de diferença no esquema entre Firebird e Postgres (alvo), para gerar os scripts de ajustes.

- A ideia seria fazer uma ferramenta CLI, interativa, capaz de iniciar um projeto de migração novo, ou retomar ou verificar um projeto de migração em andamento. 

- O processo final de migração deverá ter uma varredura por busca de erros que possam comprometer a migração e um relatório final. 

- Script sempre sequencial seguindo o passo a passo da migração. Deve ter um check antes de iniciar, se os bancos firebird e postgres estão disponíveis e se é possivel se conectar neles com as configurações que estão no config.yaml, se a verificação da estrutura das tabelas, indices , fks, pks, check contraint está igual no banco de destino (Postgres vazio, só schema) com o banco de origem Firebird para evitar dezenas de erros durante a migração.  VEJA MAIS ABAIXO: ## SCRIPT HUMANO DE PASSO A PASSO DAS ATIVIDADES:

- A parte de criação do banco novo (destino) deverá ser feita pelo script, bem como o import da estrutura que deverá ser fornecida pelo DBA em um arquivo padrao postgres, na pasta MIGRACAO_<SEQ> , por exemplo, c6_producao_pg_converter_equinix.sql. Caso o banco já exista o Maestro não poderá apagar (DROP DATABASE) deverá ser solicitado ao DBA para que apague manualmente.

- Caso tenha diferenças de estrutura, deverá ser mostrada as diferenças (usar o compara_estrutura_fb2pg.py como base para esta comparação ou o gera_relatorio_compara_estrutura_fb2pg_html.py) 

- O que vai definir a chave de migração é um subiretório dentro da pasta do projeto, com o prefixo MIGRACAO_<SEQ>  sendo o <SEQ>  um identificador de 4 digitos, começando com 0001. Cada nova migração, gera um novo dir MIGRACAO_<SEQ> (novo diretorio).  Neste diretorio deverão ficar todos os arquivos .sql, .json, .yaml, logs e .dbs da migração. 

- Acerto das SEQUENCES no banco postgres destino no final da migração: Existem 2 scripts que foram fornecidos pelo cliente, generators-gerar-c6bank-prod.sh e generators-acertar-c6bank-prod-no-postgres.sh que apesar de funcionar parecem bem frágis em segurança de sucesso da operação. Coloque eles de forma automática pelo Python ao final da migração, incluindo também no compara_estrutura_fb2pg.py e gera_relatorio_compara_estrutura_fb2pg_html.py)

- A criação final dos indices, pks, fks, comparação da estrutura, count das tabelas, ajustes das sequences, deverão ser feitos e controlados pelo Maestro.py. 

- O script Maestro.py deverá coordenar todos os passos da migração. Ele será uma ferramenta CLI, tipo o Claude Code, onde ao entrar voce poderá usar os comandos / (dash) para:
  1. Criar uma nova migracao /init
  2. Retomar uma migração em andamento /resume <SEQ>
  3. Verificar o status da migraçao /status
  4. Checkar os pre-requisito /precheck
  5. Verificar a comparação do banco origem e destino /compare --tipo <ALL/INDEX/COUNT/FK/PK/CHECKSUM/CONSTRAINT>
  6. Monitorar a migração /monitor <options>
  7. Permitir auto-aprove para ligar ou desligar o Human in the loop em tarefas criticas - /auto

- O monitor.py deverá ser revisado para usar a pasta do projeto e ser possivel montorar em uma única tela todo o processo da migração, não só o small-tables e big-tables

- A parte de remoção/disable de indices, PKs de FKs antes da migração e sua ativação e verificação de erros e falhas deve ser obrigatoriamente feito neste processo

- Seria bom ter um único banco de dados de controle da migração, em vez de 1 banco por tabela (são 908 tabelas), o que vai facilitar o controle e monitoramenteo (PREMISSA OBRIGATORIA)

- Ter uma subpasta de ./logs, ./jsons etc organizada dentro de cada subpasta da MIGRACAO_<SEQ>



---
## SCRIPT DE CRIAÇÃO DO BANCO POSGRES

-- DROP DATABASE IF EXISTS c6_producao;
-- 
CREATE DATABASE c6_producao
    WITH
    OWNER = c6_producao_user
	TEMPLATE = template0
    ENCODING = 'LATIN1'
	LOCALE_PROVIDER = 'libc'
    LC_COLLATE = 'pt_BR.iso88591'
    LC_CTYPE   = 'pt_BR.iso88591'
    TABLESPACE = tbs_c6_producao
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
	

ALTER DATABASE c6_producao SET default_tablespace TO 'tbs_c6_producao';

COMMENT ON DATABASE c6_producao IS 'Banco c6 para sistema Prognum  ';

GRANT ALL PRIVILEGES ON DATABASE c6_producao TO "c6_producao_user";

-- serch path por prioridade (PADRAO)
ALTER DATABASE c6_producao SET search_path TO "$user", public, pg_catalog;


\c c6_producao

-- Cria, se já existir ignora o erro pois coloquei isso no TEMPLATE
CREATE EXTENSION pg_stat_statements;

----------------------------------------
--3 Criar o esquema(mesmo nome banco) conectado como user postgres no banco 
----------------------------------------
--------------------------------------------------------------------------------------------
-- ATENÇÃO !!  TROCAR O DATABASE ATUAL PELO CRIADO, ANTES DE EXECUTAR OS COMANDOS ABAIXO ---
--------------------------------------------------------------------------------------------
\c c6_producao
-->>>>>>>>>>>>>
postgres=# \c c6_producao
c6_producao=#
-->>>>>>>>>>>>

-- NAO UTILIZADO NA PROGNUM - CREATE SCHEMA IF NOT EXISTS c6 AUTHORIZATION "c6_producao_user";   --<===== ATENÇÃO !!  TROCAR O DATABASE ATUAL PELO CRIADO, ANTES DE EXECUTAR OS COMANDOS ABAIXO


-- AUDITORIA
-- alter role "c6_producao_user"   set pgaudit.log to 'ddl, role';


\c c6_producao

-- Permissões 
ALTER DEFAULT PRIVILEGES FOR ROLE "c6_producao_user" IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES     TO "c6_producao_user"; 
ALTER DEFAULT PRIVILEGES FOR ROLE "c6_producao_user" IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES  TO "c6_producao_user";
ALTER DEFAULT PRIVILEGES FOR ROLE "c6_producao_user" IN SCHEMA public GRANT ALL PRIVILEGES ON FUNCTIONS  TO "c6_producao_user";
ALTER DEFAULT PRIVILEGES FOR ROLE "c6_producao_user" IN SCHEMA public GRANT ALL PRIVILEGES ON TYPES      TO "c6_producao_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "c6_producao_user";
GRANT ALL PRIVILEGES ON SCHEMA public TO "c6_producao_user";

ALTER TABLESPACE tbs_c6_producao   OWNER TO "c6_producao_user";


-- conecta no banco postgres agora

\c postgres

GRANT POSTGRES to "c6_producao_user" WITH SET TRUE;

ALTER USER c6_producao_user with superuser;



-- teste conexao
psql -h localhost -p 5432 -U "c6_producao_user" -d c6_producao -W
5tEkZZwRydTUXarJ



-- IMPORT DO DDL GERADO PELO DBA DO SCHEMA
psql -h localhost -p 5432 -U "c6_producao_user" -d c6_producao -W < /migracao_firebird/cria976_pg.sql  | tee -a /migracao_firebird/imp_cria_976_7abril.log


## SCRIPT HUMANO DE PASSO A PASSO DAS ATIVIDADES:



(.venv) postgres@postgres-server-sp2:/migracao_firebird$ cd /databases
(.venv) postgres@postgres-server-sp2:/databases$ ls -alrt
total 21231600
-rw-r--r--  1 root root          13 Mar 31 10:40 senhabd-firebird
drwxr-xr-x 21 root root        4096 Apr  4 13:41 ..
drwxr-xr-x  2 root root        4096 Apr  7 18:01 .
-rw-r-----  1 root root 21741142016 Apr  7 18:09 scci.gdb
(.venv) postgres@postgres-server-sp2:/databases$


testa a conectividade

#  root

/opt/firebird/bin/isql
CONNECT '/backup/firebird/scci.gdb' USER 'SYSDBA' PASSWORD '';
Tkyn6ws@qga89
SELECT COUNT(RDB$RELATION_NAME) 
FROM RDB$RELATIONS 
WHERE RDB$VIEW_BLR IS NULL 
AND RDB$SYSTEM_FLAG = 0;

-- postgres

sudo su - postgres
psql

\c c6_producao

SELECT count(*) 
FROM information_schema.tables 
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
AND table_type = 'BASE TABLE';

EXIT;

##########################################################################################################
## INICIO - 18:40
##########################################################################################################

----------------------------------------------------------------------------------------------------------
0 -  Apaga *.log *.md para nao confundir dos testes anteriores e do ./logs
----------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------
1- Criar o database zerado c6_producao usando o CRIACAO_NOVO_BANCO_POSTGRES_C6_PROD_EC2_V2.SQL
----------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------
2- Criar o esquema c6_producao usando c6_producao_pg_converter_equinix
----------------------------------------------------------------------------------------------------------
-- c6_producao_pg_converter_equinix.sql

psql -h localhost -U c6_producao_user -W -d c6_producao -f c6_producao_pg_converter_equinix.sql  | tee -a /migracao_firebird/c6_producao_pg_converter_equinix.log
5tEkZZwRydTUXarJ

----------------------------------------------------------------------------------------------------------
4- Liga o monitor - 19:10
----------------------------------------------------------------------------------------------------------


-- liga o monitory.py em outra sessao
sudo su - postgres

cd /migracao_firebird
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONIOENCODING=utf-8
source .venv/bin/activate
# Monitorar em outra janela
python monitor.py --big-tables

################# SO DOCUMENTACAO - NAO USAR 

                        python monitor.py                          # painel com todas as migrações detectadas
                        python monitor.py --state-db X.db          # detalhe de uma tabela específica

                                             Comando                           │                    O que mostra                    │
                     ├────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────┤
                     │ python monitor.py                                          │ Tudo: todas as tabelas + as 5 linhas do doc_oper   │
                     │                                                            │ (total + t0…t3)                                    │
                     ├────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────┤
                     │ python monitor.py --big-tables                             │ Só as 10 tabelas grandes, incluindo as threads do  │
                     │                                                            │ doc_oper (total + t0…t3)                           │
                     ├────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────┤
                     │ python monitor.py --state-db                               │ Só o painel detalhado do total agregado            │
                     │ migration_state_documento_operacao.db                      │                                                    │
                     ├────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────┤
                     │ python monitor.py --state-db                               │ Só o detalhe da thread 0                           │
                     │ migration_state_documento_operacao_t0.db                   │                                                    │
                     └────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────┘

                        python monitor.py --summary                # resumo tabular de todas as tabelas
                        python monitor.py --history 50             # histórico de batches (requer --state-db)
                        python monitor.py --json                   # JSON (requer --state-db)
                        python monitor.py --constraints            # ver constraints de todas as tabelas
                        python monitor.py -i 5                     # refresh a cada 5s (padrão: 2s)

                        python monitor.py --big-tables              # painel ao vivo das 10 tabelas grandes
                        python monitor.py --big-tables -i 5        # refresh a cada 5s
                        python monitor.py --big-tables --summary   # resumo tabular das 10 grandes



----------------------------------------------------------------------------------------------------------
5- Rodar a migracao das 10 maiores abaixo - incicio 15:44 termino 16:55
----------------------------------------------------------------------------------------------------------

-- 8 maiores

sudo su - postgres

cd /migracao_firebird
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONIOENCODING=utf-8
source .venv/bin/activate

nohup ./chama_migrator.sh PARCELASCTB &
nohup ./chama_migrator.sh CONTROLEVERSAO &
nohup ./chama_migrator.sh HISTORICO_OPERACAO &
nohup ./chama_migrator.sh OCORRENCIA &

nohup ./chama_migrator.sh OPERACAO_CREDITO &
nohup ./chama_migrator.sh NMOV &
nohup ./chama_migrator.sh OCORRENCIA_SISAT &
nohup ./chama_migrator.sh PESSOA_PRETENDENTE &


-- ou pelo Windows
-- ou pelo Windows
python.exe .\migrator_v2.py --table PARCELASCTB
python.exe .\migrator_v2.py --table CONTROLEVERSAO    
python.exe .\migrator_v2.py --table HISTORICO_OPERACAO
python.exe .\migrator_v2.py --table OCORRENCIA        
python.exe .\migrator_v2.py --table OPERACAO_CREDITO  
python.exe .\migrator_v2.py --table NMOV              
python.exe .\migrator_v2.py --table OCORRENCIA_SISAT  
python.exe .\migrator_v2.py --table PESSOA_PRETENDENTE


----------------------------------------------------------------------------------------------------------
6- Rodar a migracao das 2 maiores LOG_EVENTOS e DOCUMENTO_OPERACAO em paralelo  abaixo - 19:11 - 20:11
----------------------------------------------------------------------------------------------------------


-- DOCUMENTO_OPERACAO

nohup ./chama_migrator_doc_oper.sh   &


-- ou pelo windows - python migrator_parallel_doc_oper_v2.py --threads 4



-- LOG_EVENTOS PARALLEL

# Migração completa com 8 threads (padrão)

nohup ./migrator_log_eventos.sh > nohup_log_eventos.out 2>&1 &



-- ou pelo windows 
python migrator_log_eventos_v2.py 
                                                                        ##### ALTERNATIVAS SÓ PARA DOCUMENTAR -- NAO USAR !!!! #####
                                                                        ### Sobrescrever número de threads
                                                                        ##bash migrator_log_eventos.sh --threads 4
                                                                        ### Dry-run (conta linhas, não migra)
                                                                        ##bash migrator_log_eventos.sh --dry-run
                                                                        ### Resetar estado e recomeçar do zero
                                                                        ##bash migrator_log_eventos.sh --reset
                                                                        ### Monitorar em outra janela
                                                                        ##python monitor.py --big-tables



----------------------------------------------------------------------------------------------------------
7- Rodar a migracao das 901 tabelas - SMALL TABLES
----------------------------------------------------------------------------------------------------------

-- SMALL TABLES

  # Dar permissão (uma vez)
  chmod +x migrator_smalltables.sh

  # Rodar em nohup
  nohup ./migrator_smalltables.sh > nohup_smalltables.out 2>&1 &



  -- OU PELO WINDOWS - python migrator_smalltables_v2.py --small-tables
                                                                        
                                                                        
                                                                        ### troubleshooting se faltar alguma coisa
                                                                        ##nohup ./migrator_smalltables.sh > nohup_smalltables_3faltando.out 2>&1 &
                                                                        #### Acompanhar log
                                                                        ###tail -f nohup_smalltables.out

  # Monitor em outra janela
  python monitor.py --small-tables

                                                                        ### troubleshooting se faltar alguma coisa
                                                                        O "$@" passa qualquer argumento extra diretamente ao Python, então funciona:
                                                                        nohup ./migrator_smalltables.sh --dry-run > nohup_smalltables.out 2>&1 &
                                                                        nohup ./migrator_smalltables.sh --reset   > nohup_smalltables.out 2>&1 &
                                                                        nohup ./migrator_smalltables.sh --workers 6 > nohup_smalltables.out 2>&1 &


  python monitor.py --big-tables  


    ### MONITOR.py

   sudo su - postgres

   cd /migracao_firebird
   export LANG=C.UTF-8
   export LC_ALL=C.UTF-8
   export PYTHONIOENCODING=utf-8
   source .venv/bin/activate
   python monitor.py



    python monitor.py                          # painel com todas as migrações detectadas
    python monitor.py --state-db X.db          # detalhe de uma tabela específica

                         Comando                           │                    O que mostra                    │
  ├────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────┤
  │ python monitor.py                                          │ Tudo: todas as tabelas + as 5 linhas do doc_oper   │
  │                                                            │ (total + t0…t3)                                    │
  ├────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────┤
  │ python monitor.py --big-tables                             │ Só as 10 tabelas grandes, incluindo as threads do  │
  │                                                            │ doc_oper (total + t0…t3)                           │
  ├────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────┤
  │ python monitor.py --state-db                               │ Só o painel detalhado do total agregado            │
  │ migration_state_documento_operacao.db                      │                                                    │
  ├────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────┤
  │ python monitor.py --state-db                               │ Só o detalhe da thread 0                           │
  │ migration_state_documento_operacao_t0.db                   │                                                    │
  └────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────┘

    python monitor.py --summary                # resumo tabular de todas as tabelas
    python monitor.py --history 50             # histórico de batches (requer --state-db)
    python monitor.py --json                   # JSON (requer --state-db)
    python monitor.py --constraints            # ver constraints de todas as tabelas
    python monitor.py -i 5                     # refresh a cada 5s (padrão: 2s)

    python monitor.py --big-tables              # painel ao vivo das 10 tabelas grandes
    python monitor.py --big-tables -i 5        # refresh a cada 5s
    python monitor.py --big-tables --summary   # resumo tabular das 10 grandes

### APOS A CARGA


----------------------------------------------------------------------------------------------------------
7- Rodar  HABILITA CONSTRAINTS BIG-TABLES - inicio 17:04
----------------------------------------------------------------------------------------------------------

#### HABILITA CONSTRAINTS BIG-TABLES

   (.venv) postgres@postgres-server-sp2:/migracao_firebird/logs$ cd ..
    (.venv) postgres@postgres-server-c6-prod:/migracao_firebird$ python enable_constraints.py > log_enable_constraints_10bigs.log
    ^Z
    [1]+  Stopped                 python enable_constraints.py > log_enable_constraints_10bigs.log
    (.venv) postgres@postgres-server-c6-prod:/migracao_firebird$ bg
    [1]+ python enable_constraints.py > log_enable_constraints_10bigs.log &
    (.venv) postgres@postgres-server-c6-prod:/migracao_firebird$ tail -f log_enable_constraints_10bigs.log
    17:06:11 [INFO   ]
    17:06:11 [INFO   ] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    17:06:11 [INFO   ]   Tabela: nmov  (enable_constraints_nmov.sql)
    17:06:11 [INFO   ] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    17:06:11 [INFO   ]   6 statements encontrados
    17:06:13 [INFO   ]   [OK]   index           nmovx01  (2104ms)
    17:06:18 [INFO   ]   [OK]   primary_key     xpk_nmov  (5640ms)
    17:06:18 [INFO   ]   [OK]   config          synchronous_commit  (0ms)
    17:06:18 [INFO   ]   [OK]   config          jit  (0ms)
    17:06:19 [INFO   ]   [OK]   analyze         nmov  (546ms)
    ...

        
                            RESUMO FINAL — ENABLE CONSTRAINTS
    ╭───────────────────────────┬────────┬────────┬────────┬───────────┬────────────╮
    │ Tabela                    │     OK │   Skip │   Fail │     Tempo │   Status   │
    ├───────────────────────────┼────────┼────────┼────────┼───────────┼────────────┤
    │ parcelasctb               │      5 │      0 │      0 │      2.2s │    OK ✓    │
    ├───────────────────────────┼────────┼────────┼────────┼───────────┼────────────┤
    │ nmov                      │      6 │      0 │      0 │     15.3s │    OK ✓    │
    ├───────────────────────────┼────────┼────────┼────────┼───────────┼────────────┤
    │ historico_operacao        │     10 │      0 │      0 │      3.9s │    OK ✓    │
    ├───────────────────────────┼────────┼────────┼────────┼───────────┼────────────┤
    │ log_eventos               │     14 │      0 │      0 │     56.0s │    OK ✓    │
    ├───────────────────────────┼────────┼────────┼────────┼───────────┼────────────┤
    │ operacao_credito          │     43 │      0 │      0 │     25.7s │    OK ✓    │
    ├───────────────────────────┼────────┼────────┼────────┼───────────┼────────────┤
    │ pessoa_pretendente        │     25 │      0 │      0 │     21.2s │    OK ✓    │
    ├───────────────────────────┼────────┼────────┼────────┼───────────┼────────────┤
    │ ocorrencia                │      6 │      0 │      0 │     317ms │    OK ✓    │
    ├───────────────────────────┼────────┼────────┼────────┼───────────┼────────────┤
    │ ocorrencia_sisat          │     39 │      0 │      0 │     34.1s │    OK ✓    │
    ├───────────────────────────┼────────┼────────┼────────┼───────────┼────────────┤
    │ controleversao            │      4 │      0 │      0 │     52.4s │    OK ✓    │
    ├───────────────────────────┼────────┼────────┼────────┼───────────┼────────────┤
    │ documento_operacao        │     26 │      0 │      0 │     59.6s │    OK ✓    │
    ├───────────────────────────┼────────┼────────┼────────┼───────────┼────────────┤
    │ TOTAL                     │    178 │      0 │      0 │    4.5min │ SUCESSO ✓  │
    ╰───────────────────────────┴────────┴────────┴────────┴───────────┴────────────╯

    Concluído em 4.5min. 178 OK | 0 já existiam.
    17:10:39 [INFO   ]


   ####### OBSERVAÇÃO ############
   A FASE 2 do migrator_smalltables.py chama cman.enable_all() para cada tabela completada, que
   recria nesta ordem:

   ┌───────┬────────────────────────────────┐
   │ Ordem │              Tipo              │
   ├───────┼────────────────────────────────┤
   │ 1º    │ Índices explícitos             │
   ├───────┼────────────────────────────────┤
   │ 2º    │ PKs                            │
   ├───────┼────────────────────────────────┤
   │ 3º    │ Unique constraints             │
   ├───────┼────────────────────────────────┤
   │ 4º    │ Check constraints              │
   ├───────┼────────────────────────────────┤
   │ 5º    │ FKs próprias (tabela → outras) │
   ├───────┼────────────────────────────────┤
   │ 6º    │ FKs filhas (outras → tabela)   │
   ├───────┼────────────────────────────────┤

   │ 7º    │ Triggers                       │
   └───────┴────────────────────────────────┘

   Detalhe importante: só re-habilita as tabelas com status completed no master state DB. Se uma tabela falhar, as
   constraints dela não são reabilitadas — e o log vai avisar com a instrução manual para executar o
   enable_constraints_{dest}.sql gerado.

----------------------------------------------------------------------------------------------------------
7.1-  AJUSTES (CRIACAO)  DOS INDICE DE LOG_EVENTOS QUE FICARAM FALTANDO NO CRIA.SQL
----------------------------------------------------------------------------------------------------------

  -- log_eventos: índices do Firebird migrados para PostgreSQL
  -- ATENÇÃO: colunas multi-coluna estão em ordem alfabética (verificar ordem original no FB)

  CREATE INDEX IF NOT EXISTS idx_log_eventos_co_aplic
      ON public.log_eventos (co_aplic);

  CREATE INDEX IF NOT EXISTS idx_log_eventos_dt_data_hora
      ON public.log_eventos (dt_data_hora);

  CREATE INDEX IF NOT EXISTS idx_log_eventos_nu_contrato
      ON public.log_eventos (nu_contrato);

  CREATE INDEX IF NOT EXISTS idx_log_eventos_co_aplic_dt_data_hora
      ON public.log_eventos (co_aplic, dt_data_hora);

  CREATE INDEX IF NOT EXISTS idx_log_eventos_dt_data_hora_no_base
      ON public.log_eventos (dt_data_hora, no_base);

  CREATE INDEX IF NOT EXISTS idx_log_eventos_dt_data_hora_no_tabela
      ON public.log_eventos (dt_data_hora, no_tabela);

  CREATE INDEX IF NOT EXISTS idx_log_eventos_dt_data_hora_nu_contrato
      ON public.log_eventos (dt_data_hora, nu_contrato);

  CREATE INDEX IF NOT EXISTS idx_log_eventos_dt_data_hora_nu_pretendente
      ON public.log_eventos (dt_data_hora, nu_pretendente);

  CREATE INDEX IF NOT EXISTS idx_log_eventos_co_aplic_dt_data_hora_no_usuario
      ON public.log_eventos (co_aplic, dt_data_hora, no_usuario);

  CREATE INDEX IF NOT EXISTS idx_log_eventos_co_aplic_co_detalhe_dt_data_detalhe
      ON public.log_eventos (co_aplic, co_detalhe, dt_data_detalhe);


  ----------------------------------------------------------------------------------------------------------
  7.2 - movimentos_passivo — IDX só no PG
 ----------------------------------------------------------------------------------------------------------

  O FB tem co_contrato e nu_cercefhab (ambos existem também no PG, então se cancelam). Os 3 extras (dt_geracao_arquivo,
  no_arquivo_gerado, dt_geracao_arquivo,nu_cercefhab) existem só no PG — foram criados diretamente lá, provavelmente por
   um DBA para tuning de performance. Não é um problema, são índices a mais.

  ---
  status_documento_operacao — IDX só no FB

  O índice (nu_documento, nu_sequencial) existe no FB mas não foi migrado para PG. Criar se necessário:

  ----------------------------------------------------------------------------------------------------------
  CREATE INDEX idx_status_documento_operacao_nu_doc_seq
      ON public.status_documento_operacao (nu_documento, nu_sequencial);
  ----------------------------------------------------------------------------------------------------------
  ---
  tab_cartorio — IDX só no FB

  O PG já tem tab_cartoriox01/02/03 (que batem com o FB), mas o índice nu_municipio não foi migrado:

  ----------------------------------------------------------------------------------------------------------
  CREATE INDEX idx_tab_cartorio_nu_municipio
      ON public.tab_cartorio (nu_municipio);
  ----------------------------------------------------------------------------------------------------------
  ---
  Em resumo: 2 índices realmente faltando no PG, e 1 tabela com índices extras criados no PG após a migração. Tudo
  esperado, nada crítico


  -- OUTRAS DIFERENÇAS QUE TAVA NO SCRIPT MAS NÃO CRIOU:
  ALTER TABLE CONTROLEVERSAO ADD PRIMARY KEY (VERSAO, ID);
  ALTER TABLE DOCUMENTO_CIP ADD CONSTRAINT R_CONTROLEVERSAO_DOC_CIP_MERGE FOREIGN KEY (NU_VERSAO_COM_MERGE, ID_DOC_COM_MERGE) REFERENCES CONTROLEVERSAO;
  ALTER TABLE DOCUMENTO_CIP ADD CONSTRAINT R_CONTROLEVERSAO_DOCUMENTO_CIP FOREIGN KEY (NU_VERSAO_SEM_MERGE, ID_DOC_SEM_MERGE) REFERENCES CONTROLEVERSAO;
  CREATE INDEX CONTROLEVERSAODATA ON CONTROLEVERSAO(ALT_DATA,ID);
  CREATE INDEX AKCONTROLEVERSAOUSUARIO ON CONTROLEVERSAO( CO_USUARIO_IMPLANTACAO );

 ----------------------------------------------------------------------------------------------------------
  7.3 - Ajsutes diferenças FKs , PKs e indices
 ----------------------------------------------------------------------------------------------------------
 5tEkZZwRydTUXarJ

 psql -h localhost -p 5432 -U "c6_producao_user" -d c6_producao -W < /migracao_firebird/tabelas_10_so_constraints_fk.sql  | tee -a /migracao_firebird/tabelas_10_so_constraints_fk.log

 psql -h localhost -p 5432 -U "c6_producao_user" -d c6_producao -W < /migracao_firebird/tabelas_10_so_pks_constraints_check_e_indices.sql  | tee -a /migracao_firebird/tabelas_10_so_constraints_fk.log

 psql -h localhost -p 5432 -U "c6_producao_user" -d c6_producao -W < /migracao_firebird/03_create_foreign_key_tudo.sql | tee -a /migracao_firebird/03_create_foreign_key_tudo.log
  
 ----------------------------------------------------------------------------------------------------------
  7.4 - Ajsutes generator SEQUENCES
 ----------------------------------------------------------------------------------------------------------

(.venv) postgres@postgres-server-c6-prod:/migracao_firebird$ ./generators-gerar-c6bank-prod.sh

##  GERA O DDL DE DROP E CREATE DAS SEQUENCES
(.venv) postgres@postgres-server-c6-prod:/migracao_firebird$ ls -alrt | tail -4
-rwxr-xr-x  1 postgres  postgres        321 Apr  9 17:27 generators-gerar-c6bank-prod.sh
-rwxrwxr-x  1 postgres  postgres         93 Apr  9 17:27 generators-acertar-c6bank-prod-no-postgres.sh
drwxrwxrwx  5 postgres  postgres     282624 Apr  9 17:28 .
-rw-rw-r--  1 postgres  postgres      38208 Apr  9 17:28 gen-c6bank-prod.sql

## CONFERE
(.venv) postgres@postgres-server-c6-prod:/migracao_firebird$ vi gen-c6bank-prod.sql
## RODA
(.venv) postgres@postgres-server-c6-prod:/migracao_firebird$ ./generators-acertar-c6bank-prod-no-postgres.sh
Password:
Pager usage is off.
DROP SEQUENCE
CREATE SEQUENCE
 setval
--------
...


----------------------------------------------------------------------------------------------------------
8- Rodar Compara estrutura e count
----------------------------------------------------------------------------------------------------------

#### 

   python gera_relatorio_compara_estrutura_fb2pg_html.py  --output relatorio_final_09abr.html

   /*
   #Executar verificação:
   #bash# Verificação completa

   #python compara_estrutura_fb2pg.py

   # Com progresso detalhado
   python compara_estrutura_fb2pg.py --verbose

   # Gerar relatório HTML visual
   #python gera_relatorio_html.py --output relatorio.html
   */



----------------------------------------------------------------------------------------------------------
9- Checksum em paralelo das 10 maiores - Feito durante a migração, após cada carga
----------------------------------------------------------------------------------------------------------

(.venv) postgres@postgres-server-c6-prod:/migracao_firebird$ tail -n 25 *.log^C
(.venv) postgres@postgres-server-c6-prod:/migracao_firebird$ cd logs
(.venv) postgres@postgres-server-c6-prod:/migracao_firebird/logs$ tail -n 25 *.log
==> CONTROLEVERSAO_090426_154157.log <==
│ dado                 │       0 │       0 │  ✓ OK  │
│ te_observacao_versao │       0 │       0 │  ✓ OK  │
│ te_imagem_reduzida   │ 438,913 │ 438,913 │  ✓ OK  │
╰──────────────────────┴─────────┴─────────┴────────╯

───────────────────────────────── Resumo Final ─────────────────────────────────

╔══════════════════╤══════════════╤══════════════╤═══════════════╤═════════════╗
║                  │     Colunas  │      Linhas  │               │             ║
║  Tabela          │       BYTEA  │      verif.  │  Divergênci…  │  Resultado  ║
╟──────────────────┼──────────────┼──────────────┼───────────────┼─────────────╢
║  controleversao  │           3  │           —  │            0  │    ✓ OK     ║
╚══════════════════╧══════════════╧══════════════╧═══════════════╧═════════════╝

╭──────────────────────────────────────────────────────────────────────────────╮
│                                                                              │
│                 Todas as 1 tabelas passaram na verificação.                  │
│                 Integridade dos dados BYTEA confirmada.                      │
│                                                                              │
╰──────────────────────────────────────────────────────────────────────────────╯

Tempo total: 5.7s  |  Fim: 09/04/2026 16:04:04

------------------------------------------
Finalizado em: Thu Apr  9 16:04:04 -03 2026

==> HISTORICO_OPERACAO_090426_154315.log <==
│                                                        │
╰────────────────────────────────────────────────────────╯
Início: 09/04/2026 15:44:18

Analisando historico_operacao... nenhuma coluna BLOB binário↔BYTEA encontrada (FB=[], PG=[])
───────────────────────────────── Resumo Final ─────────────────────────────────

╔══════════════════════╤═════════════╤═════════════╤═════════════╤═════════════╗
║                      │    Colunas  │     Linhas  │             │             ║
║  Tabela              │      BYTEA  │     verif.  │  Divergên…  │  Resultado  ║
╟──────────────────────┼─────────────┼─────────────┼─────────────┼─────────────╢
║  historico_operacao  │          0  │          —  │          0  │    ✓ OK     ║
╚══════════════════════╧═════════════╧═════════════╧═════════════╧═════════════╝

╭──────────────────────────────────────────────────────────────────────────────╮
│                                                                              │
│                 Todas as 1 tabelas passaram na verificação.                  │
│                 Integridade dos dados BYTEA confirmada.                      │
│                                                                              │
╰──────────────────────────────────────────────────────────────────────────────╯

Tempo total: 0.1s  |  Fim: 09/04/2026 15:44:19

------------------------------------------
Finalizado em: Thu Apr  9 15:44:19 -03 2026

==> NMOV_090426_154315.log <==
╭──────────────┬───────────┬───────────┬────────╮
│ Coluna BYTEA │    Qtd FB │    Qtd PG │ Status │
├──────────────┼───────────┼───────────┼────────┤
│ te_campos    │ 4,515,732 │ 4,515,732 │  ✓ OK  │
╰──────────────┴───────────┴───────────┴────────╯

───────────────────────────────── Resumo Final ─────────────────────────────────

╔═══════════╤═════════════════╤═════════════════╤════════════════╤═════════════╗
║  Tabela   │  Colunas BYTEA  │  Linhas verif.  │  Divergências  │  Resultado  ║
╟───────────┼─────────────────┼─────────────────┼────────────────┼─────────────╢
║  nmov     │              1  │              —  │             0  │    ✓ OK     ║
╚═══════════╧═════════════════╧═════════════════╧════════════════╧═════════════╝

╭──────────────────────────────────────────────────────────────────────────────╮
│                                                                              │
│                 Todas as 1 tabelas passaram na verificação.                  │
│                 Integridade dos dados BYTEA confirmada.                      │
│                                                                              │
╰──────────────────────────────────────────────────────────────────────────────╯

Tempo total: 26.2s  |  Fim: 09/04/2026 16:55:23

------------------------------------------
Finalizado em: Thu Apr  9 16:55:23 -03 2026

==> OCORRENCIA_090426_154315.log <==
├─────────────────────────┼────────┼────────┼────────┤
│ te_descricao_ocorrencia │  8,807 │  8,807 │  ✓ OK  │
│ te_observacao_execucao  │  6,706 │  6,706 │  ✓ OK  │
╰─────────────────────────┴────────┴────────┴────────╯

───────────────────────────────── Resumo Final ─────────────────────────────────

╔══════════════╤════════════════╤════════════════╤═══════════════╤═════════════╗
║              │       Colunas  │        Linhas  │               │             ║
║  Tabela      │         BYTEA  │        verif.  │  Divergênci…  │  Resultado  ║
╟──────────────┼────────────────┼────────────────┼───────────────┼─────────────╢
║  ocorrencia  │             2  │             —  │            0  │    ✓ OK     ║
╚══════════════╧════════════════╧════════════════╧═══════════════╧═════════════╝

╭──────────────────────────────────────────────────────────────────────────────╮
│                                                                              │
│                 Todas as 1 tabelas passaram na verificação.                  │
│                 Integridade dos dados BYTEA confirmada.                      │
│                                                                              │
╰──────────────────────────────────────────────────────────────────────────────╯

Tempo total: 0.4s  |  Fim: 09/04/2026 15:44:15

------------------------------------------
Finalizado em: Thu Apr  9 15:44:16 -03 2026

==> OCORRENCIA_SISAT_090426_154315.log <==
│ te_descricao_ocorrencia │ 1,496,907 │ 1,496,907 │  ✓ OK  │
│ te_observacao_execucao  │   897,464 │   897,464 │  ✓ OK  │
│ te_variavel_tarefa      │ 1,505,569 │ 1,505,569 │  ✓ OK  │
╰─────────────────────────┴───────────┴───────────┴────────╯

───────────────────────────────── Resumo Final ─────────────────────────────────

╔════════════════════╤══════════════╤══════════════╤═════════════╤═════════════╗
║                    │     Colunas  │      Linhas  │             │             ║
║  Tabela            │       BYTEA  │      verif.  │  Divergên…  │  Resultado  ║
╟────────────────────┼──────────────┼──────────────┼─────────────┼─────────────╢
║  ocorrencia_sisat  │           3  │           —  │          0  │    ✓ OK     ║
╚════════════════════╧══════════════╧══════════════╧═════════════╧═════════════╝

╭──────────────────────────────────────────────────────────────────────────────╮
│                                                                              │
│                 Todas as 1 tabelas passaram na verificação.                  │
│                 Integridade dos dados BYTEA confirmada.                      │
│                                                                              │
╰──────────────────────────────────────────────────────────────────────────────╯

Tempo total: 9.4s  |  Fim: 09/04/2026 16:43:06

------------------------------------------
Finalizado em: Thu Apr  9 16:43:06 -03 2026

==> OPERACAO_CREDITO_090426_154315.log <==
│ te_qualificacao_iq             │       0 │       0 │  ✓ OK  │
│ te_prioridade                  │       0 │       0 │  ✓ OK  │
│ te_exigencia_cartorio          │   1,608 │   1,608 │  ✓ OK  │
╰────────────────────────────────┴─────────┴─────────┴────────╯

───────────────────────────────── Resumo Final ─────────────────────────────────

╔════════════════════╤══════════════╤══════════════╤═════════════╤═════════════╗
║                    │     Colunas  │      Linhas  │             │             ║
║  Tabela            │       BYTEA  │      verif.  │  Divergên…  │  Resultado  ║
╟────────────────────┼──────────────┼──────────────┼─────────────┼─────────────╢
║  operacao_credito  │          33  │           —  │          0  │    ✓ OK     ║
╚════════════════════╧══════════════╧══════════════╧═════════════╧═════════════╝

╭──────────────────────────────────────────────────────────────────────────────╮
│                                                                              │
│                 Todas as 1 tabelas passaram na verificação.                  │
│                 Integridade dos dados BYTEA confirmada.                      │
│                                                                              │
╰──────────────────────────────────────────────────────────────────────────────╯

Tempo total: 91.7s  |  Fim: 09/04/2026 16:27:19

------------------------------------------
Finalizado em: Thu Apr  9 16:27:19 -03 2026

==> PARCELASCTB_090426_154315.log <==
│ Coluna BYTEA │ Qtd FB │ Qtd PG │ Status │
├──────────────┼────────┼────────┼────────┤
│ te_dados     │ 41,107 │ 41,107 │  ✓ OK  │
╰──────────────┴────────┴────────┴────────╯

───────────────────────────────── Resumo Final ─────────────────────────────────

╔═══════════════╤═══════════════╤════════════════╤═══════════════╤═════════════╗
║               │      Colunas  │        Linhas  │               │             ║
║  Tabela       │        BYTEA  │        verif.  │  Divergênci…  │  Resultado  ║
╟───────────────┼───────────────┼────────────────┼───────────────┼─────────────╢
║  parcelasctb  │            1  │             —  │            0  │    ✓ OK     ║
╚═══════════════╧═══════════════╧════════════════╧═══════════════╧═════════════╝

╭──────────────────────────────────────────────────────────────────────────────╮
│                                                                              │
│                 Todas as 1 tabelas passaram na verificação.                  │
│                 Integridade dos dados BYTEA confirmada.                      │
│                                                                              │
╰──────────────────────────────────────────────────────────────────────────────╯

Tempo total: 0.4s  |  Fim: 09/04/2026 15:44:51

------------------------------------------
Finalizado em: Thu Apr  9 15:44:52 -03 2026

==> PESSOA_PRETENDENTE_090426_154315.log <==
│ te_envio_consulta_crivo      │ 42,659 │ 42,659 │  ✓ OK  │
│ te_contrato_social           │ 42,659 │ 42,659 │  ✓ OK  │
│ te_inf_adicional             │ 12,592 │ 12,592 │  ✓ OK  │
╰──────────────────────────────┴────────┴────────┴────────╯

───────────────────────────────── Resumo Final ─────────────────────────────────

╔══════════════════════╤═════════════╤═════════════╤═════════════╤═════════════╗
║                      │    Colunas  │     Linhas  │             │             ║
║  Tabela              │      BYTEA  │     verif.  │  Divergên…  │  Resultado  ║
╟──────────────────────┼─────────────┼─────────────┼─────────────┼─────────────╢
║  pessoa_pretendente  │         15  │          —  │          0  │    ✓ OK     ║
╚══════════════════════╧═════════════╧═════════════╧═════════════╧═════════════╝

╭──────────────────────────────────────────────────────────────────────────────╮
│                                                                              │
│                 Todas as 1 tabelas passaram na verificação.                  │
│                 Integridade dos dados BYTEA confirmada.                      │
│                                                                              │
╰──────────────────────────────────────────────────────────────────────────────╯

Tempo total: 29.6s  |  Fim: 09/04/2026 16:15:54

------------------------------------------
Finalizado em: Thu Apr  9 16:15:54 -03 2026


(.venv) postgres@postgres-server-c6-prod:/migracao_firebird/logs$

           


            ## OBSERVACAO NAO PRECISA RODAR  ############# outras formas de chamar caso necessario
                            (.venv) postgres@postgres-server-sp2:/migracao_firebird$ 

                            Fluxo paralelo: NAO FUNCIONOU, ESGOTOU 100% DA MEMORIA DO SERVIDOR
                            1. Dispara 10 threads simultaneamente, cada uma com conexões FB+PG próprias
                            2. Conforme terminam, imprime ✓ tabela  X.Xs  (N/10) em tempo real
                            3. Após todas completarem, exibe os detalhes de cada tabela na ordem original
                            4. Exibe o resumo consolidado final

                            # Todas as 10 em paralelo (padrão) - NAO FUNCIONOU, ESGOTOU 100% DA MEMORIA DO SERVIDOR
                            #####  NAO FUNCIONOU, ESGOTOU 100% DA MEMORIA DO SERVIDOR  ###python PosMigracao_comparaChecksum_bytea.py
                            
                            -- inicio 20:51 (com 1 em paralelo)
                            python PosMigracao_comparaChecksum_bytea.py --workers 1

                            # Verificar só uma tabela (modo sequencial, com barra de progresso)
                            #python PosMigracao_comparaChecksum_bytea.py --table DOCUMENTO_OPERACAO
                            # Controle de concorrência
                            #python PosMigracao_comparaChecksum_bytea.py --workers 5




----------------------------------------------------------------------------------------------------------
10 - ANLYZE VERBOSE (100% da tabela para maior precisao no PLANNER do postgres, o padrao é 100 linhas)
----------------------------------------------------------------------------------------------------------


   psql
   \c c6_producao

   SET default_statistics_target = 10000; -- Máximo permitido (10.000)
   ANALYZE VERBOSE;

