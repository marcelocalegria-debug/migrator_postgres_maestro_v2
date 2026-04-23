import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from .base import StepBase

class CreateDatabaseStep(StepBase):
    """Cria o banco de dados de destino no PostgreSQL com configurações avançadas."""

    def run(self) -> bool:
        pg = self.config.postgres
        target_db = pg['database']
        target_user = pg['user']
        
        # Parâmetros opcionais baseados no PRD
        owner = pg.get('owner', target_user)
        template = pg.get('template', 'template0')
        encoding = pg.get('encoding', 'UTF8')
        tablespace = pg.get('tablespace', 'DEFAULT')
        lc_collate = pg.get('lc_collate', 'pt_BR.UTF-8')
        lc_ctype = pg.get('lc_ctype', lc_collate)

        print(f"--- Criando Banco: {target_db} (Owner: {owner}, Encoding: {encoding}) ---")

        try:
            # Conecta ao banco 'postgres' como superuser para criar o novo banco
            conn = psycopg2.connect(
                host=pg['host'], 
                port=pg.get('port', 5432),
                database='postgres',
                user=pg['user'], 
                password=pg['password']
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()

            # 1. Verifica se o banco já existe
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
            if cur.fetchone():
                print(f"[WARNING] Banco {target_db} já existe. Ignorando criação.")
                cur.close()
                conn.close()
                return True

            # 2. Constrói o comando CREATE DATABASE
            # Nota: Alguns parâmetros como LOCALE_PROVIDER exigem PG15+
            sql = f'CREATE DATABASE "{target_db}" WITH OWNER = "{owner}" '
            
            if template != 'DEFAULT':
                sql += f'TEMPLATE = {template} '
            
            sql += f"ENCODING = '{encoding}' "
            
            # Se for pt_BR.iso88591 (PRD), precisamos garantir que o sistema operacional tenha o locale
            if lc_collate:
                sql += f"LC_COLLATE = '{lc_collate}' LC_CTYPE = '{lc_ctype}' "
            
            if tablespace != 'DEFAULT':
                sql += f'TABLESPACE = {tablespace} '

            print(f"Executando: CREATE DATABASE {target_db}...")
            cur.execute(sql)
            
            # 3. Ajustes de permissões e search_path
            print(f"Configurando permissões para {target_user}...")
            
            # Reconecta agora no banco recém-criado para rodar os GRANTS e ALTERs
            conn_target = psycopg2.connect(
                host=pg['host'], 
                port=pg.get('port', 5432),
                database=target_db,
                user=pg['user'], 
                password=pg['password']
            )
            conn_target.autocommit = True
            cur_target = conn_target.cursor()
            
            cur_target.execute(f'GRANT ALL PRIVILEGES ON DATABASE "{target_db}" TO "{target_user}"')
            cur_target.execute(f'ALTER DATABASE "{target_db}" SET search_path TO "$user", public, pg_catalog')
            
            if tablespace != 'DEFAULT':
                cur_target.execute(f'ALTER DATABASE "{target_db}" SET default_tablespace TO {tablespace}')

            cur_target.close()
            conn_target.close()
            
            print(f"[OK] Banco {target_db} criado e configurado com sucesso.")
            cur.close()
            conn.close()
            return True
        except Exception as e:
            print(f"[ERROR] Falha ao criar/configurar banco de dados: {str(e)}")
            return False
