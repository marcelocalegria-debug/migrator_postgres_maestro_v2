import psycopg2
from pathlib import Path
from .base import StepBase
from pg_constraints import REENABLE_ORDER

class EnableConstraintsStep(StepBase):
    """Reabilita PKs, FKs e Índices no PostgreSQL após a carga, respeitando a ordem de dependência."""

    def run(self) -> bool:
        print("--- Reabilitando Constraints ---")
        pg = self.config.postgres
        mig_info = self.db.get_migration(self.migration_id)
        
        try:
            # 1. Recupera todas as constraints da migração que estão 'disabled' ou 'failed'
            constraints_db = self.db.list_constraints(self.migration_id)
            if not constraints_db:
                print("[WARNING] Nenhuma constraint encontrada para reabilitar.")
                return True

            # Filtra apenas as que precisam ser habilitadas
            to_enable = [c for c in constraints_db if c['status'] in ('disabled', 'failed')]
            if not to_enable:
                print("[OK] Todas as constraints já estão habilitadas.")
                return True

            # 2. Conecta ao PostgreSQL
            conn = psycopg2.connect(
                host=pg['host'], 
                port=pg.get('port', 5432),
                database=pg['database'],
                user=pg['user'], 
                password=pg['password']
            )
            conn.autocommit = True
            cur = conn.cursor()
            
            # 3. Agrupar por tipo para seguir a REENABLE_ORDER
            # REENABLE_ORDER = ['index', 'primary_key', 'unique', 'check', 'foreign_key_own', 'foreign_key_child', 'trigger']
            
            total_ok = 0
            total_fail = 0
            
            # Conjunto de tabelas afetadas para rodar ANALYZE no final
            affected_tables = set()

            for obj_type in REENABLE_ORDER:
                group = [c for c in to_enable if c['constraint_type'] == obj_type]
                if not group:
                    continue
                
                print(f"Habilitando objetos do tipo: {obj_type} ({len(group)} itens)...")
                for c in group:
                    try:
                        cur.execute(c['sql_enable'])
                        self.db.update_constraint_status(c['id'], 'enabled')
                        affected_tables.add(c['dest_table'])
                        total_ok += 1
                    except psycopg2.Error as e:
                        # Se o erro for "já existe" (duplicate_object=42710 ou duplicate_table=42P07 para índices)
                        if e.pgcode in ('42710', '42P07'):
                            # print(f"  [INFO] {c['constraint_name']} já existe, ignorando.")
                            self.db.update_constraint_status(c['id'], 'enabled')
                            affected_tables.add(c['dest_table'])
                            total_ok += 1
                        else:
                            print(f"  [ERROR] Falha ao habilitar {c['constraint_name']} em {c['dest_table']}: {str(e)}")
                            self.db.update_constraint_status(c['id'], 'failed', error_message=str(e))
                            total_fail += 1
                    except Exception as e:
                        print(f"  [ERROR] Erro inesperado em {c['constraint_name']}: {str(e)}")
                        self.db.update_constraint_status(c['id'], 'failed', error_message=str(e))
                        total_fail += 1

            # 4. Rodar ANALYZE e REINDEX (opcional, mas recomendado pelo pg_constraints)
            print(f"Finalizando: Executando ANALYZE em {len(affected_tables)} tabelas...")
            for table in affected_tables:
                try:
                    cur.execute(f'ANALYZE "public"."{table}"')
                except:
                    pass

            cur.close()
            conn.close()
            
            print(f"[OK] Processo concluído: {total_ok} sucessos, {total_fail} falhas.")
            return total_fail == 0 # Sucesso do step se não houver falhas
            
        except Exception as e:
            print(f"[ERROR] Falha catastrófica ao habilitar constraints: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
