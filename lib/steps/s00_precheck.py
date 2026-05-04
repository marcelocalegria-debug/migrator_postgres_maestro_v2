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

        # Confirmação dos bancos de origem e destino antes de qualquer check
        fb = self.config.firebird
        pg = self.config.postgres
        print("\n" + "=" * 60)
        print("  BANCOS DE DADOS — CONFIRME ANTES DE PROSSEGUIR")
        print("=" * 60)
        print(f"  ORIGEM  (Firebird):   host={fb.get('host')}  "
              f"db={fb.get('database')}  user={fb.get('user')}")
        print(f"  DESTINO (PostgreSQL): host={pg.get('host')}  "
              f"port={pg.get('port', 5432)}  db={pg.get('database')}  user={pg.get('user')}")
        print("=" * 60)
        mig_seq = self.db.get_migration(self.migration_id)['seq']
        print(f"\n  Config: MIGRACAO_{mig_seq}/config.yaml")
        confirma = input("\nOs bancos estão corretos? (s/N): ").strip().lower()
        if confirma != 's':
            print("[INFO] Operação cancelada. Edite o config.yaml na pasta da migração e re-execute.")
            return False
        print()

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

        # 5. Verifica arquivos obrigatórios no diretório da migração
        mig_dir = Path(f"MIGRACAO_{self.db.get_migration(self.migration_id)['seq']}")
        
        # 5.1 Verifica schema.sql
        schema_path = mig_dir / "schema.sql"
        if not schema_path.exists():
            print(f"[ERROR] Arquivo schema.sql não encontrado em {mig_dir}")
            success = False
        else:
            print(f"[OK] Arquivo schema.sql encontrado.")

        # 5.2 Verifica ajusta_base_firebird.sql
        adjust_path = mig_dir / "ajusta_base_firebird.sql"
        if not adjust_path.exists():
            print(f"[ERROR] Arquivo ajusta_base_firebird.sql não encontrado em {mig_dir}")
            print(f"[INFO] Este script é obrigatório para corrigir dados no Firebird antes da migração.")
            success = False
        else:
            print(f"[OK] Arquivo ajusta_base_firebird.sql encontrado.")

        return success

    def _create_firebird_audit_user(self, conn):
        """Cria o usuário MIGRATION_AUDIT e a ROLE de auditoria no Firebird."""
        import json as _json

        # Flag de idempotência: evita reaplicar grants a cada /check ou /run
        step_record = self.db.get_step(self.migration_id, self.step_number)
        if step_record and step_record.get('details_json'):
            try:
                if _json.loads(step_record['details_json']).get('firebird_audit_done'):
                    print("[INFO] Auditoria Firebird já configurada. Pulando.")
                    return
            except Exception:
                pass

        user = "MIGRATION_AUDIT"
        password = "migra_audit_123"
        role = "MIGRATION_AUDIT_ROLE"

        print(f"Configurando auditoria Firebird (User: {user}, Role: {role})...")
        try:
            cur = conn.cursor()
            
            # 1. Cria Usuário
            try:
                cur.execute(f"CREATE USER {user} PASSWORD '{password}'")
                conn.commit()
                print(f"[OK] Usuário {user} criado.")
            except Exception as e:
                if "already exists" in str(e).lower() or "primary or unique key" in str(e).lower():
                    print(f"[INFO] Usuário {user} já existe.")
                else: print(f"[WARNING] Erro ao criar usuário: {e}")

            # 2. Cria Role
            try:
                cur.execute(f"CREATE ROLE {role}")
                conn.commit()
                print(f"[OK] Role {role} criada.")
            except Exception as e:
                err_msg = str(e).lower()
                if "already exists" in err_msg or "primary or unique key" in err_msg or "integ_5" in err_msg:
                    print(f"[INFO] Role {role} já existe.")
                else: 
                    print(f"[WARNING] Erro ao criar role: {e}")

            # 3. Associa Role ao Usuário
            try:
                cur.execute(f"GRANT {role} TO {user}")
                conn.commit()
            except Exception as e:
                err_msg = str(e).lower()
                if "already exists" not in err_msg and "integ" not in err_msg:
                    print(f"[WARNING] Erro ao associar role: {e}")

            # 4. Grant iterativo idempotente (Alternativa ao EXECUTE BLOCK que falha no FB3)
            cur.execute("SELECT TRIM(RDB$RELATION_NAME) FROM RDB$RELATIONS WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0 AND RDB$RELATION_TYPE IN (0, 1)")
            tables = [row[0] for row in cur.fetchall()]

            cur.execute("SELECT TRIM(RDB$RELATION_NAME) FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG = 1 AND RDB$RELATION_NAME STARTING WITH 'RDB$'")
            sys_tables = [row[0] for row in cur.fetchall()]

            all_objects = tables + sys_tables

            # Verifica grants já existentes para evitar iterar 900+ objetos desnecessariamente
            cur.execute(
                "SELECT TRIM(RDB$RELATION_NAME) FROM RDB$USER_PRIVILEGES "
                "WHERE TRIM(RDB$USER) = ? AND RDB$PRIVILEGE = 'S'",
                (role,)
            )
            already_granted = {row[0] for row in cur.fetchall()}

            pending = [obj for obj in all_objects if obj not in already_granted]

            if not pending:
                print(f"[INFO] Privilégios já concedidos a {role} em todos os {len(all_objects)} objetos. Nada a fazer.")
            else:
                if already_granted:
                    print(f"Aplicando privilégios em {len(pending)} objetos pendentes para a ROLE {role} ({len(already_granted)} já existentes)...")
                else:
                    print(f"Aplicando privilégios em {len(pending)} objetos para a ROLE {role}...")

                error_count = 0
                for obj in pending:
                    try:
                        cur.execute(f'GRANT SELECT ON "{obj}" TO ROLE {role}')
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        if "already exists" not in str(e).lower() and "integ_5" not in str(e).lower():
                            error_count += 1
                        # Fallback for the specific user
                        try:
                            cur.execute(f'GRANT SELECT ON "{obj}" TO USER {user}')
                            conn.commit()
                        except Exception:
                            conn.rollback()

                if error_count == 0:
                    print(f"[OK] Privilégios de leitura concedidos.")
                else:
                    print(f"[WARNING] Privilégios concedidos com {error_count} erros (ignorados).")

            # Grava flag de idempotência para não reaplicar em execuções futuras
            self.db.set_step_details(self.migration_id, self.step_number, {'firebird_audit_done': True}, step_name='PRECHECK')
            cur.close()
        except Exception as e:
            print(f"[WARNING] Erro geral na auditoria FB: {e}")

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
