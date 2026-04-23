--### ATENÇÃO - OS NOMES DE DATABASE, USUARIOS, ESQUEMAS E TABLESPACE SÃO CASE SENSITIVE - DAR PREFERENCIA A CAIXA BAIXA (MINUSCULO)
----------------------------------------
-- 1-Criação de Tablespaces  (SO PRECISA FAZER 1 VEZ, POIS USUARIO(ROLES) E TABLESPACE É DA INSTANCIA E NÃO DO DATABASE)
----------------------------------------
-- Através do (linux), conectado como user postgres, criar os diretórios para tabelas e índices do projeto em /database  (ver se tem subpasta da versão postgres)
-- obs.: É necessário criar a ROLE owner antes da tablespace. 
-- Em bancos replicas, criar sem as tablespaces, o processo de replica é quem cria

sudo su - postgres

mkdir -p /database/tablespaces/18/main
mkdir -p /database/tablespaces/18/main/tbs_<nome_banco>

postgres$ psql
psql (18.3 (Debian 18.3-1.pgdg13+1))
Type "help" for help.

postgres=# 

CREATE TABLESPACE tbs_<nome_banco>
OWNER postgres
LOCATION '/database/tablespaces/18/main/tbs_<nome_banco>';

-- LOCATION 'C:\database\tablespaces';
----------------------------------------
-- 5 - Criação dos logins para database
----------------------------------------
-- cria o usuario de serviço da aplicação srv<nome_do_banco> 
-- ATENÇÃO  - USUÁRIO AUTENTICAÇÃO LOCAL OBRIGATORIAMENTE, SALVO SE FOR SOLICITADO AO CONTRARIO!!


--(SO PRECISA FAZER 1 VEZ, POIS USUARIO(ROLES) E TABLESPACE É DA INSTANCIA E NÃO DO DATABASE)
CREATE USER "<nome_banco>_user" WITH LOGIN
NOSUPERUSER
NOCREATEDB
NOCREATEROLE
INHERIT
NOREPLICATION 
CONNECTION LIMIT -1  PASSWORD '#### METROQUE ###';



-- DROP DATABASE IF EXISTS <nome_banco>;
-- 
CREATE DATABASE <nome_banco>
    WITH
    OWNER = <nome_banco>_user
	TEMPLATE = template0
    ENCODING = 'LATIN1'
	LOCALE_PROVIDER = 'libc'
    LC_COLLATE = 'pt_BR.iso88591'
    LC_CTYPE   = 'pt_BR.iso88591'
    TABLESPACE = tbs_<nome_banco>
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
	

ALTER DATABASE <nome_banco> SET default_tablespace TO 'tbs_<nome_banco>';


COMMENT ON DATABASE <nome_banco> IS 'Banco XXXXX para sistema SCCI  ';

GRANT ALL PRIVILEGES ON DATABASE <nome_banco> TO "<nome_banco>_user";

-- serch path por prioridade (PADRAO)
ALTER DATABASE <nome_banco> SET search_path TO "$user", public, pg_catalog;


\c <nome_banco>

-- Cria, se já existir ignora o erro pois coloquei isso no TEMPLATE
CREATE EXTENSION pg_stat_statements;

----------------------------------------
--3 Criar o esquema(mesmo nome banco) conectado como user postgres no banco 
----------------------------------------
--------------------------------------------------------------------------------------------
-- ATENÇÃO !!  TROCAR O DATABASE ATUAL PELO CRIADO, ANTES DE EXECUTAR OS COMANDOS ABAIXO ---
--------------------------------------------------------------------------------------------
\c <nome_banco>

postgres=# \c <nome_banco>

-- Permissões 
ALTER DEFAULT PRIVILEGES FOR ROLE "<nome_banco>_user" IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES     TO "<nome_banco>_user"; 
ALTER DEFAULT PRIVILEGES FOR ROLE "<nome_banco>_user" IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES  TO "<nome_banco>_user";
ALTER DEFAULT PRIVILEGES FOR ROLE "<nome_banco>_user" IN SCHEMA public GRANT ALL PRIVILEGES ON FUNCTIONS  TO "<nome_banco>_user";
ALTER DEFAULT PRIVILEGES FOR ROLE "<nome_banco>_user" IN SCHEMA public GRANT ALL PRIVILEGES ON TYPES      TO "<nome_banco>_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "<nome_banco>_user";
GRANT ALL PRIVILEGES ON SCHEMA public TO "<nome_banco>_user";

ALTER TABLESPACE tbs_<nome_banco>   OWNER TO "<nome_banco>_user";

-- conecta no banco postgres agora 

\c postgres

GRANT POSTGRES to "<nome_banco>_user" WITH SET TRUE;

ALTER USER <nome_banco>_user with superuser;

\q

-- teste conexao
psql -h localhost -p 5432 -U "<nome_banco>_user" -d <nome_banco> 

