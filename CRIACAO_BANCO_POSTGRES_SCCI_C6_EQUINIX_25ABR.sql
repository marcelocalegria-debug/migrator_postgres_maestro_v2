--### ATENÇÃO - OS NOMES DE DATABASE, USUARIOS, ESQUEMAS E TABLESPACE SÃO CASE SENSITIVE - DAR PREFERENCIA A CAIXA BAIXA (MINUSCULO)
----------------------------------------
-- 1-Criação de Tablespaces  (SO PRECISA FAZER 1 VEZ, POIS USUARIO(ROLES) E TABLESPACE É DA INSTANCIA E NÃO DO DATABASE)
----------------------------------------
-- Através do (linux), conectado como user postgres, criar os diretórios para tabelas e índices do projeto em /database  (ver se tem subpasta da versão postgres)
-- obs.: É necessário criar a ROLE owner antes da tablespace. 
-- Em bancos replicas, criar sem as tablespaces, o processo de replica é quem cria

# se no powershell
$env:PGCLIENTENCODING='LATIN1'

sudo su - postgres

mkdir -p /database/tablespaces/18/main
mkdir -p /database/tablespaces/18/main/tbs_c6_producao

postgres$ psql
psql (18.3 (Debian 18.3-1.pgdg13+1))
Type "help" for help.

postgres=# 

CREATE TABLESPACE tbs_c6_producao
OWNER postgres
LOCATION '/database/tablespaces/18/main/tbs_c6_producao';

-- LOCATION 'C:\database\tablespaces';
----------------------------------------
-- 5 - Criação dos logins para database
----------------------------------------
-- cria o usuario de serviço da aplicação srv<nome_do_banco> 
-- ATENÇÃO  - USUÁRIO AUTENTICAÇÃO LOCAL OBRIGATORIAMENTE, SALVO SE FOR SOLICITADO AO CONTRARIO!!


--(SO PRECISA FAZER 1 VEZ, POIS USUARIO(ROLES) E TABLESPACE É DA INSTANCIA E NÃO DO DATABASE)
CREATE USER "c6_producao_user" WITH LOGIN
NOSUPERUSER
NOCREATEDB
NOCREATEROLE
INHERIT
NOREPLICATION 
CONNECTION LIMIT -1  PASSWORD '5tEkZZwRydTUXarJ';



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


COMMENT ON DATABASE c6_producao IS 'Banco XXXXX para sistema SCCI  ';

GRANT ALL PRIVILEGES ON DATABASE c6_producao TO "c6_producao_user";

-- serch path por prioridade (PADRAO)
ALTER DATABASE c6_producao SET search_path TO "$user", public, pg_catalog;


\c c6_producao

-- Cria, se já existir ignora o erro pois coloquei isso no TEMPLATE
CREATE EXTENSION pg_stat_statements;

----------------------------------------
-- 3 Criar o esquema(mesmo nome banco) conectado como user postgres no banco 
----------------------------------------
--------------------------------------------------------------------------------------------
-- ATENÇÃO !!  TROCAR O DATABASE ATUAL PELO CRIADO, ANTES DE EXECUTAR OS COMANDOS ABAIXO ---
--------------------------------------------------------------------------------------------
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

\q

-- teste conexao
psql -h localhost -p 5432 -U "c6_producao_user" -d c6_producao 

