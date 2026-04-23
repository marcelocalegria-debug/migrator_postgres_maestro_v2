import os
import sys
import shutil
import fdb
import psycopg2
from pathlib import Path
from .base import StepBase

class PrecheckStep(StepBase):
    """Verifica conectividade FB/PG, disco, versões e pré-requisitos."""

    def run(self) -> bool:
        print("--- Iniciando Precheck ---")
        success = True
        
        # 1. Verifica Versão Python
        if sys.version_info < (3, 13):
            print(f"[ERROR] Python 3.13+ requerido. Versão atual: {sys.version}")
            success = False
        else:
            print(f"[OK] Python version: {sys.version.split()[0]}")

        # 2. Verifica Conexão Firebird
        try:
            if os.name == 'nt':
                fb_dll = os.path.abspath("fbclient.dll")
                if os.path.exists(fb_dll):
                    try:
                        fdb.load_api(fb_dll)
                    except: pass

            fb = self.config.firebird
            conn_fb = fdb.connect(
                host=fb['host'], database=fb['database'],
                user=fb['user'], password=fb['password'],
                charset='UTF8'
            )
            print("[OK] Conexão Firebird estabelecida.")
            
            # Cria usuário de auditoria no Firebird
            self._create_firebird_audit_user(conn_fb)
            conn_fb.close()
        except Exception as e:
            print(f"[ERROR] Falha na conexão Firebird: {str(e)}")
            success = False

        # 3. Verifica Conexão PostgreSQL (banco postgres)
        try:
            pg = self.config.postgres
            conn_pg = psycopg2.connect(
                host=pg['host'], 
                port=pg.get('port', 5432),
                database='postgres',
                user=pg['user'], 
                password=pg['password']
            )
            print("[OK] Conexão PostgreSQL (banco 'postgres') estabelecida.")
            
            # Cria usuário de auditoria no PostgreSQL
            self._create_postgres_audit_user(conn_pg)
            conn_pg.close()
        except Exception as e:
            print(f"[ERROR] Falha na conexão PostgreSQL: {str(e)}")
            success = False

        # 4. Verifica Espaço em Disco
        total, used, free = shutil.disk_usage(".")
        free_gb = free // (2**30)
        if free_gb < 5:
            print(f"[WARNING] Pouco espaço em disco: {free_gb}GB livres.")
        else:
            print(f"[OK] Espaço em disco: {free_gb}GB livres.")

        # 5. Verifica schema.sql no diretório da migração
        mig_dir = Path(f"MIGRACAO_{self.db.get_migration(self.migration_id)['seq']}")
        schema_path = mig_dir / "schema.sql"
        if not schema_path.exists():
            print(f"[ERROR] Arquivo schema.sql não encontrado em {mig_dir}")
            success = False
        else:
            print(f"[OK] Arquivo schema.sql encontrado.")

        return success

    def _create_firebird_audit_user(self, conn):
        """Cria o usuário MIGRATION_AUDIT no Firebird para leitura segura."""
        user = "MIGRATION_AUDIT"
        password = "migra_audit_123"
        print(f"Verificando usuário de auditoria Firebird '{user}'...")
        try:
            cur = conn.cursor()
            # No Firebird 3+, usuários são globais. Tenta criar, ignora se já existir.
            try:
                cur.execute(f"CREATE USER {user} PASSWORD '{password}'")
                conn.commit()
                print(f"[OK] Usuário {user} criado no Firebird.")
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"[INFO] Usuário {user} já existe no Firebird.")
                else:
                    print(f"[WARNING] Não foi possível criar usuário FB: {e}")

            # Grant SELECT em tabelas de sistema pelo menos
            # Em Firebird 3.0, não há GRANT SELECT ON ALL TABLES. 
            # O migrador fará grants sob demanda se necessário, ou o DBA pode fazer.
            cur.close()
        except Exception as e:
            print(f"[WARNING] Erro ao configurar auditoria FB: {e}")

    def _create_postgres_audit_user(self, conn):
        """Cria o usuário migration_audit no PostgreSQL para leitura segura."""
        user = "migration_audit"
        password = "migra_audit_123"
        db_name = self.config.postgres['database']
        print(f"Verificando usuário de auditoria PostgreSQL '{user}'...")
        try:
            conn.autocommit = True
            cur = conn.cursor()
            
            # Verifica se role existe
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,))
            if not cur.fetchone():
                cur.execute(f"CREATE USER {user} WITH PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE")
                print(f"[OK] Usuário {user} criado no PostgreSQL.")
            
            # Tenta conectar no banco de destino para dar grants de leitura
            try:
                conn_target = psycopg2.connect(
                    host=self.config.postgres['host'],
                    port=self.config.postgres.get('port', 5432),
                    database=db_name,
                    user=self.config.postgres['user'],
                    password=self.config.postgres['password']
                )
                conn_target.autocommit = True
                cur_target = conn_target.cursor()
                
                cur_target.execute(f"GRANT CONNECT ON DATABASE {db_name} TO {user}")
                cur_target.execute(f"GRANT USAGE ON SCHEMA public TO {user}")
                cur_target.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {user}")
                cur_target.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {user}")
                
                cur_target.close()
                conn_target.close()
                print(f"[OK] Permissões de leitura concedidas a {user} no banco {db_name}.")
            except Exception as e:
                print(f"[WARNING] Não foi possível dar grants no banco {db_name}: {e}")

            cur.close()
        except Exception as e:
            print(f"[WARNING] Erro ao configurar auditoria PG: {e}")
