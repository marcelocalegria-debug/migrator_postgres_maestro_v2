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
            # [FIX] Lógica para localizar fbclient.dll no Windows
            if os.name == 'nt':
                fb_dll = os.path.abspath("fbclient.dll")
                if os.path.exists(fb_dll):
                    try:
                        fdb.load_api(fb_dll)
                        # print(f"[INFO] fbclient.dll carregada de: {fb_dll}")
                    except: pass

            fb = self.config.firebird
            conn_fb = fdb.connect(
                host=fb['host'], database=fb['database'],
                user=fb['user'], password=fb['password'],
                charset='UTF8'
            )
            print("[OK] Conexão Firebird estabelecida.")
            conn_fb.close()
        except Exception as e:
            print(f"[ERROR] Falha na conexão Firebird: {str(e)}")
            success = False

        # 3. Verifica Conexão PostgreSQL (banco postgres para ver se host/user/pass estão ok)
        try:
            pg = self.config.postgres
            conn_pg = psycopg2.connect(
                host=pg['host'], database='postgres',
                user=pg['user'], password=pg['password']
            )
            print("[OK] Conexão PostgreSQL (banco 'postgres') estabelecida.")
            conn_pg.close()
        except Exception as e:
            print(f"[ERROR] Falha na conexão PostgreSQL: {str(e)}")
            success = False

        # 4. Verifica Espaço em Disco (diretório de logs/work)
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
