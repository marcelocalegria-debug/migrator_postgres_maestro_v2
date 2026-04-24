import psycopg2
from .base import StepBase

class CreateDatabaseStep(StepBase):
    """
    Valida se o banco de dados de destino, usuário e tablespace existem no PostgreSQL.
    Não cria automaticamente; exige intervenção do DBA se não houver conformidade.
    """

    def run(self) -> bool:
        pg = self.config.postgres
        target_db = pg['database']
        target_user = pg['user']
        target_ts = pg.get('tablespace', 'DEFAULT')

        print(f"--- Validando Ambiente PostgreSQL para: {target_db} ---")

        try:
            # Conecta ao banco 'postgres' como superuser para validação
            conn = psycopg2.connect(
                host=pg['host'], 
                port=pg.get('port', 5432),
                database='postgres',
                user=pg['user'], 
                password=pg['password']
            )
            cur = conn.cursor()

            # 1. Validação de Usuário (Role)
            if target_user.lower() == 'postgres':
                print("[ERROR] O usuário 'postgres' não pode ser usado para a migração.")
                self._print_manual_sql(target_db, target_user, target_ts)
                return False

            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (target_user,))
            if not cur.fetchone():
                print(f"[ERROR] Usuário '{target_user}' não encontrado no PostgreSQL.")
                self._print_manual_sql(target_db, target_user, target_ts)
                return False

            # 2. Validação de Tablespace
            if target_ts.lower() in ('pg_default', 'pg_global', 'default'):
                print(f"[ERROR] Tablespace '{target_ts}' (padrão do sistema) não é permitida para o banco de dados da aplicação. Use uma tablespace dedicada (ex: tbs_{target_db}).")
                self._print_manual_sql(target_db, target_user, target_ts)
                return False

            cur.execute("SELECT 1 FROM pg_tablespace WHERE spcname = %s", (target_ts,))
            if not cur.fetchone():
                print(f"[ERROR] Tablespace '{target_ts}' não encontrada no PostgreSQL.")
                self._print_manual_sql(target_db, target_user, target_ts)
                return False

            # 3. Validação de Banco de Dados
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
            if not cur.fetchone():
                print(f"[ERROR] Banco de dados '{target_db}' não encontrado.")
                self._print_manual_sql(target_db, target_user, target_ts)
                return False

            # 4. Validação de Ownership e Tablespace do Banco
            cur.execute("""
                SELECT pg_catalog.pg_get_userbyid(datdba), spcname 
                FROM pg_database d 
                JOIN pg_tablespace t ON d.dattablespace = t.oid 
                WHERE datname = %s
            """, (target_db,))
            owner, ts = cur.fetchone()
            
            if owner != target_user:
                print(f"[WARNING] O owner do banco '{target_db}' é '{owner}', mas o config espera '{target_user}'.")
            
            if ts != target_ts:
                print(f"[ERROR] O banco '{target_db}' está na tablespace '{ts}', mas o config espera '{target_ts}'.")
                return False

            cur.close()
            conn.close()
            
            print(f"[OK] Ambiente PostgreSQL validado com sucesso para '{target_db}'.")
            return True

        except Exception as e:
            print(f"[ERROR] Falha na conexão de validação: {str(e)}")
            return False

    def _print_manual_sql(self, db, user, ts):
        if ts in ('pg_default', 'pg_global', 'DEFAULT'):
            ts = f"tbs_{db}"
        
        print("\n" + "="*80)
        print(" ATENÇÃO - PRE-REQUISITOS NÃO ATENDIDOS. SOLICITE AO DBA A EXECUÇÃO DO SCRIPT:")
        print("="*80)
        print(f"""
-- 1. Criação de diretórios (Linux) como root/postgres
-- sudo mkdir -p /database/tablespaces/18/main/{ts}
-- sudo chown postgres:postgres /database/tablespaces/18/main/{ts}

-- 2. Conectado como superuser 'postgres' no psql:

CREATE TABLESPACE {ts} OWNER postgres LOCATION '/database/tablespaces/18/main/{ts}';

CREATE USER "{user}" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION CONNECTION LIMIT -1 PASSWORD '#### SENHA ###';

CREATE DATABASE {db}
    WITH OWNER = "{user}"
    TEMPLATE = template0
    ENCODING = 'LATIN1'
    LOCALE_PROVIDER = 'libc'
    LC_COLLATE = 'pt_BR.iso88591'
    LC_CTYPE   = 'pt_BR.iso88591'
    TABLESPACE = {ts}
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

ALTER DATABASE {db} SET default_tablespace TO '{ts}';
ALTER DATABASE {db} SET search_path TO "$user", public, pg_catalog;

\\c {db}

GRANT ALL PRIVILEGES ON DATABASE {db} TO "{user}";
GRANT ALL PRIVILEGES ON SCHEMA public TO "{user}";
ALTER TABLESPACE {ts} OWNER TO "{user}";

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "{user}";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "{user}";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO "{user}";

-- Se necessário permissão temporária de superuser para carga:
-- ALTER USER "{user}" WITH SUPERUSER;
        """)
        print("="*80 + "\n")
