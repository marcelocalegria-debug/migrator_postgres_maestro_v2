import psycopg2
from pathlib import Path
from .base import StepBase
from pg_constraints import ConstraintManager

class DisableConstraintsStep(StepBase):
    """Desabilita PKs, FKs e Índices no PostgreSQL para acelerar a carga de todas as tabelas."""

    def run(self) -> bool:
        print("--- Desabilitando Constraints ---")
        pg = self.config.postgres
        schema = pg.get('schema', 'public')
        
        try:
            # 1. Obter a lista de todas as tabelas no esquema destino
            conn = psycopg2.connect(
                host=pg['host'], database=pg['database'],
                user=pg['user'], password=pg['password']
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """, (schema,))
            tables = [r[0] for r in cur.fetchall()]
            cur.close()
            conn.close()
            
            if not tables:
                print("[WARNING] Nenhuma tabela encontrada no PostgreSQL.")
                return True

            print(f"Encontradas {len(tables)} tabelas. Coletando constraints...")

            # Parâmetros de conexão para o ConstraintManager
            conn_params = {
                'host': pg['host'], 'database': pg['database'],
                'user': pg['user'], 'password': pg['password'],
                'port': pg.get('port', 5432)
            }

            total_objects = 0
            for table_name in tables:
                # 2. Instancia o ConstraintManager para a tabela específica
                manager = ConstraintManager(conn_params, schema, table_name)
                
                # 3. Coleta todos os objetos (PK, FK, Index, Check, Trigger)
                count = manager.collect_all()
                if count > 0:
                    print(f"  [{table_name}] Coletados {count} objetos.")
                    total_objects += count
                    
                    # 4. Salva no DB central do Maestro
                    for obj in manager.dropped_objects:
                        self.db.add_constraint(
                            self.migration_id,
                            dest_table=table_name,
                            constraint_type=obj.obj_type,
                            constraint_name=obj.obj_name,
                            sql_disable=obj.drop_sql,
                            sql_enable=obj.create_sql
                        )
                    
                    # 5. Executa o DISABLE/DROP
                    ok = manager.disable_all()
                    if ok < count:
                        print(f"    [WARNING] Apenas {ok}/{count} objetos desabilitados.")

            # Atualiza status no DB central para todas as constraints desta migração
            constraints_db = self.db.list_constraints(self.migration_id)
            for c in constraints_db:
                # Somente as que acabamos de adicionar e estão com status inicial 'active'
                if c['status'] == 'active':
                    self.db.update_constraint_status(c['id'], 'disabled')
            
            print(f"[OK] {total_objects} objetos desabilitados em {len(tables)} tabelas.")
            return True
            
        except Exception as e:
            print(f"[ERROR] Falha ao desabilitar constraints: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
