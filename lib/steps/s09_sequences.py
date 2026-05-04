import os
import sys
import fdb
import psycopg2
from pathlib import Path
from .base import StepBase

# ─── Firebird DLL auto-discovery (Windows) ────────────────────────────────────
if os.name == "nt":
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    _project_root = os.path.dirname(os.path.dirname(_script_dir))
    for _p in [
        os.path.join(_project_root, "fbclient.dll"),
        os.path.join(_script_dir, "fbclient.dll"),
        os.path.abspath("fbclient.dll"),
        r"C:\Program Files\Firebird\Firebird_3_0\fbclient.dll",
        r"C:\Program Files\Firebird\Firebird_4_0\fbclient.dll",
        r"C:\Program Files\Firebird\Firebird_5_0\fbclient.dll",
        r"C:\Program Files (x86)\Firebird\Firebird_3_0\fbclient.dll",
    ]:
        if os.path.exists(_p):
            try:
                fdb.load_api(_p)
                break
            except Exception:
                pass

class SequencesStep(StepBase):
    """Ajusta os valores das Sequences no PostgreSQL com base nos generators do Firebird."""

    def run(self) -> bool:
        print("--- Ajustando Sequences ---")
        fb = self.config.firebird
        pg = self.config.postgres

        mig_info = self.db.get_migration(self.migration_id)
        mig_dir = Path(f"MIGRACAO_{mig_info['seq']}")
        output_sql_path = mig_dir / "sql" / "gen_sequences.sql"

        generators = []

        # 1. Busca generators e valores no Firebird
        try:
            conn_fb = fdb.connect(
                host=fb['host'], database=fb['database'],
                user=fb['user'], password=fb['password'],
                charset='UTF8'
            )
            cur_fb = conn_fb.cursor()

            # Firebird 3: RDB$GENERATORS contém os metadados
            cur_fb.execute("""
                SELECT RDB$GENERATOR_NAME 
                FROM RDB$GENERATORS 
                WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0
            """)
            gen_names = [r[0].strip() for r in cur_fb.fetchall()]

            for name in gen_names:
                # Obtém o valor atual sem incrementar
                cur_fb.execute(f"SELECT GEN_ID({name}, 0) FROM RDB$DATABASE")
                val = cur_fb.fetchone()[0]
                generators.append((name, val))

            conn_fb.close()
            print(f"[OK] {len(generators)} generators lidos do Firebird.")
        except Exception as e:
            print(f"[ERROR] Falha ao ler generators do Firebird: {str(e)}")
            return False

        if not generators:
            print("[WARNING] Nenhum generator encontrado no Firebird.")
            return True

        # 2. Gera SQL e aplica no PostgreSQL
        sql_commands = []
        ok_count = 0
        fail_count = 0
        failed_list = []
        try:
            conn_pg = psycopg2.connect(
                host=pg['host'],
                port=pg.get('port', 5432),
                database=pg['database'],
                user=pg['user'],
                password=pg['password']
            )
            conn_pg.autocommit = True
            cur_pg = conn_pg.cursor()

            for name, val in generators:
                seq_name = f"sq_{name.lower()}"
                # Valor mínimo 1 para setval
                target_val = max(1, val)

                # Comandos SQL
                sql_commands.append(f"-- Generator: {name}")
                sql_commands.append(f"DROP SEQUENCE IF EXISTS {seq_name} CASCADE;")
                sql_commands.append(f"CREATE SEQUENCE {seq_name} START WITH {target_val};")
                sql_commands.append(f"SELECT setval('{seq_name}', {target_val}, true);")
                sql_commands.append("")

                try:
                    cur_pg.execute(f"DROP SEQUENCE IF EXISTS {seq_name} CASCADE")
                    cur_pg.execute(f"CREATE SEQUENCE {seq_name}")
                    cur_pg.execute(f"SELECT setval(%s, %s, true)", (seq_name, target_val))
                    result = cur_pg.fetchone()
                    if result is None or result[0] != target_val:
                        print(f"  [ERROR] Sequence {seq_name}: setval retornou {result} (esperado {target_val})")
                        fail_count += 1
                        failed_list.append(seq_name)
                        self.db.log_error(self.migration_id, self.step_number,
                                          table_name=seq_name, error_type='sequence_fail',
                                          error_message=f"setval retornou {result}, esperado {target_val}")
                    else:
                        ok_count += 1
                except Exception as e:
                    print(f"  [WARNING] Erro na sequence {seq_name}: {str(e)}")
                    fail_count += 1
                    failed_list.append(seq_name)
                    self.db.log_error(self.migration_id, self.step_number,
                                      table_name=seq_name, error_type='sequence_fail',
                                      error_message=str(e))

            cur_pg.close()
            conn_pg.close()

            # Salva o script para auditoria
            with open(output_sql_path, 'w', encoding='utf-8') as f:
                f.write("\n".join(sql_commands))

            self.db.set_step_details(self.migration_id, self.step_number, {
                'total_generators': len(generators),
                'sequences_ok': ok_count,
                'sequences_failed': fail_count,
                'failed_sequences': failed_list,
                'sql_script_path': str(output_sql_path)
            })

            print(f"[OK] {ok_count}/{len(generators)} sequences ajustadas no PostgreSQL.")
            if fail_count:
                print(f"[WARNING] {fail_count} sequences falharam: {', '.join(failed_list)}")
            print(f"[INFO] Script de auditoria salvo em: {output_sql_path}")
            return True

        except Exception as e:
            print(f"[ERROR] Falha ao aplicar sequences no PostgreSQL: {str(e)}")
            return False

