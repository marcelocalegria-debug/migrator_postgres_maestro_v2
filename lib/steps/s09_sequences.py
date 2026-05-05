import os
import sys
import fdb
import psycopg2
from pathlib import Path
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
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
        console = Console()
        console.print("[bold cyan]--- Ajustando Sequences ---[/bold cyan]")
        
        fb = self.config.firebird
        pg = self.config.postgres

        mig_info = self.db.get_migration(self.migration_id)
        mig_dir = Path(f"MIGRACAO_{mig_info['seq']}")
        
        # Garante diretórios
        (mig_dir / "logs").mkdir(parents=True, exist_ok=True)
        (mig_dir / "sql").mkdir(parents=True, exist_ok=True)

        output_sql_path = mig_dir / "sql" / "gen_sequences.sql"
        log_path = mig_dir / "logs" / "s09_sequences.log"
        html_report_path = mig_dir / "reports" / "s09_sequences_report.html"

        # Limpa log anterior se existir
        if log_path.exists():
            log_path.unlink()

        generators = []
        report_data = []

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
                # Obtém o valor atual sem incrementar (conforme requisito 1.3)
                cur_fb.execute(f"SELECT GEN_ID({name}, 0) FROM RDB$DATABASE")
                val = cur_fb.fetchone()[0]
                generators.append((name, val))

            conn_fb.close()
            console.print(f"[green][OK][/green] {len(generators)} generators lidos do Firebird.")
        except Exception as e:
            console.print(f"[red][ERROR][/red] Falha ao ler generators do Firebird: {str(e)}")
            return False

        if not generators:
            console.print("[yellow][WARNING][/yellow] Nenhum generator encontrado no Firebird.")
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

            with open(log_path, "a", encoding="utf-8") as log_f:
                log_f.write(f"Início do ajuste de sequences: {datetime.now()}\n")
                log_f.write("-" * 80 + "\n")

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

                    status = "OK"
                    pg_val = None
                    error_msg = ""

                    try:
                        cur_pg.execute(f"DROP SEQUENCE IF EXISTS {seq_name} CASCADE")
                        cur_pg.execute(f"CREATE SEQUENCE {seq_name}")
                        cur_pg.execute(f"SELECT setval(%s, %s, true)", (seq_name, target_val))
                        result = cur_pg.fetchone()
                        
                        if result and result[0] == target_val:
                            pg_val = result[0]
                            ok_count += 1
                        else:
                            status = "ERROR"
                            pg_val = result[0] if result else "None"
                            error_msg = f"setval retornou {pg_val}, esperado {target_val}"
                            fail_count += 1
                            failed_list.append(seq_name)
                    except Exception as e:
                        status = "ERROR"
                        error_msg = str(e)
                        fail_count += 1
                        failed_list.append(seq_name)

                    # Log em arquivo (Requisito 1.1)
                    log_line = f"[{status}] Sequence: {seq_name} | FB: {val} | PG: {pg_val} | {error_msg}".strip(" | ")
                    log_f.write(log_line + "\n")

                    # Dados para relatório HTML (Requisito 1.2)
                    report_data.append({
                        'fb_name': name,
                        'pg_name': seq_name,
                        'fb_val': val,
                        'pg_val': pg_val,
                        'status': status,
                        'error': error_msg
                    })

                    if status == "ERROR":
                        self.db.log_error(self.migration_id, self.step_number,
                                          table_name=seq_name, error_type='sequence_fail',
                                          error_message=error_msg)

            # 3. Validação de Quantidade (Requisito 1.4)
            cur_pg.execute("""
                SELECT count(*) 
                FROM pg_class c 
                JOIN pg_namespace n ON n.oid = c.relnamespace 
                WHERE c.relkind = 'S' 
                  AND n.nspname = 'public' 
                  AND c.relname LIKE 'sq_%'
            """)
            pg_count = cur_pg.fetchone()[0]
            
            cur_pg.close()
            conn_pg.close()

            # Salva o script para auditoria
            with open(output_sql_path, 'w', encoding='utf-8') as f:
                f.write("\n".join(sql_commands))

            # Gera relatório HTML (Requisito 1.2)
            self._generate_html_report(html_report_path, report_data, len(generators), pg_count)

            # Salva no DB
            self.db.set_step_details(self.migration_id, self.step_number, {
                'total_fb': len(generators),
                'total_pg': pg_count,
                'sequences_ok': ok_count,
                'sequences_failed': fail_count,
                'failed_sequences': failed_list,
                'sql_script_path': str(output_sql_path),
                'log_path': str(log_path),
                'html_report': str(html_report_path)
            })

            # Output Maestro (Requisito 1.5)
            self._print_rich_summary(console, len(generators), pg_count, ok_count, fail_count, log_path, html_report_path)

            return True

        except Exception as e:
            console.print(f"[red][ERROR][/red] Falha ao aplicar sequences no PostgreSQL: {str(e)}")
            return False

    def _generate_html_report(self, path: Path, data: list, total_fb: int, total_pg: int):
        rows = ""
        for item in data:
            status_style = "color: green; font-weight: bold;" if item['status'] == "OK" else "color: white; background-color: red; font-weight: bold;"
            rows += f"""
            <tr>
                <td>{item['fb_name']}</td>
                <td>{item['pg_name']}</td>
                <td>{item['fb_val']}</td>
                <td>{item['pg_val']}</td>
                <td style="{status_style}">{item['status']}</td>
                <td>{item['error']}</td>
            </tr>
            """
        
        count_status = "OK" if total_fb == total_pg else "ERRO (Diferença detectada)"
        count_style = "color: green;" if total_fb == total_pg else "color: red; font-weight: bold;"

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Relatório de Sequences - Firebird vs PostgreSQL</title>
            <style>
                body {{ font-family: sans-serif; margin: 20px; background-color: #f4f4f9; }}
                h2 {{ color: #333; }}
                table {{ border-collapse: collapse; width: 100%; background-color: white; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #007bff; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .summary {{ margin-bottom: 20px; padding: 15px; background-color: white; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
            </style>
        </head>
        <body>
            <h2>Relatório de Sincronização de Sequences</h2>
            <div class="summary">
                <p><strong>Data/Hora:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
                <p><strong>Total Firebird:</strong> {total_fb}</p>
                <p><strong>Total PostgreSQL:</strong> {total_pg}</p>
                <p><strong>Status Contagem:</strong> <span style="{count_style}">{count_status}</span></p>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Generator FB</th>
                        <th>Sequence PG</th>
                        <th>Valor FB</th>
                        <th>Valor PG</th>
                        <th>Status</th>
                        <th>Erro/Detalhe</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        </body>
        </html>
        """
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)

    def _print_rich_summary(self, console, total_fb, total_pg, ok, fail, log_path, html_path):
        table = Table(title="Resumo do Ajuste de Sequences", title_style="bold magenta")
        table.add_column("Métrica", style="cyan")
        table.add_column("Valor", justify="right")

        table.add_row("Total Generators (FB)", str(total_fb))
        table.add_row("Total Sequences (PG)", str(total_pg))
        
        count_color = "green" if total_fb == total_pg else "red"
        table.add_row("Diferença de Contagem", f"[{count_color}]{total_fb - total_pg}[/{count_color}]")
        
        table.add_row("Sucessos", f"[green]{ok}[/green]")
        table.add_row("Falhas", f"[red]{fail}[/red]")

        console.print(table)
        
        paths_info = f"""
[bold]Log Detalhado:[/bold] [underline]{log_path}[/underline]
[bold]Relatório HTML:[/bold] [underline]{html_path}[/underline]
"""
        console.print(Panel(paths_info, title="Arquivos Gerados", border_style="blue"))

