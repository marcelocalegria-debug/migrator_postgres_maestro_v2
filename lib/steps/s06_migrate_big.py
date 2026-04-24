import subprocess
import os
import sys
import time
from pathlib import Path
from .base import StepBase

class MigrateBigStep(StepBase):
    """Migra todas as 10 tabelas grandes em paralelo usando migradores especializados ou v2."""

    def run(self) -> bool:
        print("--- Migrando Tabelas Grandes (Paralelo) ---")
        mig_info = self.db.get_migration(self.migration_id)
        mig_dir = Path(f"MIGRACAO_{mig_info['seq']}")
        config_path = mig_dir / "config.yaml"
        master_db = mig_dir / "migration.db"
        
        # Definição das tabelas grandes e seus scripts
        # Tabelas com migrador especializado
        specialized = [
            ('DOCUMENTO_OPERACAO', 'migrator_parallel_doc_oper_v2.py'),
            ('LOG_EVENTOS', 'migrator_log_eventos_v2.py')
        ]
        
        # Tabelas grandes que usam o migrador universal v2
        universal_v2 = [
            'HISTORICO_OPERACAO',
            'OCORRENCIA_SISAT',
            'OCORRENCIA',
            'NMOV',
            'OPERACAO_CREDITO',
            'PARCELASCTB',
            'PESSOA_PRETENDENTE',
            'CONTROLEVERSAO'
        ]
        
        processes = []
        
        # 1. Inicia Migradores Especializados
        for table, script in specialized:
            # Verifica status da tabela antes de iniciar
            t_info = self.db.get_table_by_name(self.migration_id, table)
            if t_info and t_info['status'] == 'completed':
                print(f"  [SKIP] {table} já concluída.")
                continue

            print(f"Iniciando especializado: {table}...")
            cmd = [
                sys.executable, script,
                '--config', str(config_path.absolute()),
                '--master-db', str(master_db.absolute()),
                '--migration-id', str(self.migration_id),
                '--work-dir', str(mig_dir.absolute())
            ]
            log_f = open(mig_dir / "logs" / f"migrate_{table.lower()}.stdout.log", "w")
            p = subprocess.Popen(cmd, stdout=log_f, stderr=subprocess.STDOUT)
            processes.append((table, p, log_f))

        # 2. Inicia Migradores V2 para o restante das Big Tables
        for table in universal_v2:
            # Verifica status da tabela antes de iniciar
            t_info = self.db.get_table_by_name(self.migration_id, table)
            if t_info and t_info['status'] == 'completed':
                print(f"  [SKIP] {table} já concluída.")
                continue

            print(f"Iniciando V2: {table}...")
            cmd = [
                sys.executable, 'migrator_v2.py',
                '--table', table,
                '--config', str(config_path.absolute()),
                '--master-db', str(master_db.absolute()),
                '--migration-id', str(self.migration_id),
                '--work-dir', str(mig_dir.absolute())
            ]
            log_f = open(mig_dir / "logs" / f"migrate_{table.lower()}.stdout.log", "w")
            p = subprocess.Popen(cmd, stdout=log_f, stderr=subprocess.STDOUT)
            processes.append((table, p, log_f))

        # 3. Inicia Migrador de Tabelas Pequenas (Paralelo)
        print(f"Iniciando Tabelas Pequenas em paralelo...")
        small_cmd = [
            sys.executable, 'migrator_smalltables_v2.py',
            '--config', str(config_path.absolute()),
            '--small-tables',
            '--master-db', str(master_db.absolute()),
            '--migration-id', str(self.migration_id),
            '--work-dir', str(mig_dir.absolute()),
            '--workers', str(self.config.get('migration', {}).get('parallel_workers', 4))
        ]
        log_f_small = open(mig_dir / "logs" / "migrate_small.stdout.log", "w")
        p_small = subprocess.Popen(small_cmd, stdout=log_f_small, stderr=subprocess.STDOUT)
        processes.append(("SMALL_TABLES", p_small, log_f_small))

        # 4. Aguarda todos finalizarem
        print(f"Monitorando {len(processes)} processos (Big + Small Tables)...")
        success = True
        
        # Pequeno loop de monitoramento simples
        active_count = len(processes)
        while active_count > 0:
            active_count = 0
            for i, (table, p, log_f) in enumerate(processes):
                if p is None: continue # Já finalizado e processado
                
                exit_code = p.poll()
                if exit_code is not None:
                    log_f.close()
                    if exit_code == 0:
                        print(f"  [OK] {table} concluída.")
                    else:
                        print(f"  [ERROR] {table} falhou (RC={exit_code}).")
                        success = False
                    processes[i] = (table, None, None) # Marca como processado
                else:
                    active_count += 1
            
            if active_count > 0:
                time.sleep(5) # Aguarda 5s entre checagens

        return success
