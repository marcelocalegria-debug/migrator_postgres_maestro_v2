import yaml
import fdb
import psycopg2
import os
import subprocess
import sys
import glob
from pathlib import Path
from mcp.server.fastmcp import FastMCP

# ─── Firebird DLL auto-discovery (Windows) ────────────────────────────────────
if os.name == 'nt':
    _fb_paths = [
        os.path.abspath("fbclient.dll"),
        os.path.abspath(os.path.join(os.path.dirname(__file__) or '.', '..', 'fbclient.dll')),
        r'C:\Program Files\Firebird\Firebird_3_0\fbclient.dll',
        r'C:\Program Files\Firebird\Firebird_4_0\fbclient.dll',
    ]
    for _p in _fb_paths:
        if os.path.exists(_p):
            try:
                import fdb
                fdb.load_api(_p)
                break
            except Exception:
                pass

# Inicializa o servidor MCP
mcp = FastMCP("Migracao_DB_Safe_Server")

def load_config():
    """Lê o arquivo de configuração YAML dinamicamente."""
    env_path = os.getenv("MIGRATION_CONFIG_PATH")
    if env_path:
        p = Path(env_path)
        if p.exists():
            with open(p, "r", encoding="utf-8") as file:
                return yaml.safe_load(file)

    paths_to_try = [
        Path("config.yaml"),
        Path(__file__).parent.parent / "config.yaml"
    ]
    
    for p in paths_to_try:
        if p.exists():
            with open(p, "r", encoding="utf-8") as file:
                return yaml.safe_load(file)
                
    raise FileNotFoundError("Arquivo config.yaml não encontrado.")

def get_safe_fb_connection():
    """Conexão Firebird RESTRITA (Audit/ReadOnly)."""
    config = load_config()['firebird']
    # OBRIGATÓRIO usar usuário de auditoria
    user = config.get('audit_user', 'MIGRATION_AUDIT')
    password = config.get('audit_password', 'migra_audit_123')
    
    return fdb.connect(
        host=config['host'],
        port=config.get('port', 3050),
        database=config['database'],
        user=user,
        password=password,
        charset='WIN1252'
    )

def get_safe_pg_connection():
    """Conexão PostgreSQL RESTRITA (Audit/ReadOnly)."""
    cfg_data = load_config()
    config = cfg_data.get('postgresql') or cfg_data.get('postgres')
    
    # OBRIGATÓRIO usar usuário de auditoria
    user = config.get('audit_user', 'migration_audit')
    password = config.get('audit_password', 'migra_audit_123')
    
    return psycopg2.connect(
        host=config['host'],
        port=config.get('port', 5432),
        dbname=config['database'],
        user=user,
        password=password
    )

@mcp.tool()
def list_migration_projects() -> str:
    """Lista todos os diretórios de migração (MIGRACAO_*) encontrados."""
    root = Path(__file__).parent.parent
    dirs = sorted([d.name for d in root.glob("MIGRACAO_*") if d.is_dir()], reverse=True)
    if not dirs:
        return "Nenhum diretório MIGRACAO_ encontrado."
    return "Projetos de migração detectados: " + ", ".join(dirs)

@mcp.tool()
def check_migration_logs(lines: int = 50, filter_error: bool = True) -> str:
    """
    Verifica os logs da migração.
    - lines: quantidade de linhas do final do arquivo.
    - filter_error: se True, retorna apenas linhas com ERROR, WARNING, EXCEPTION ou TRACEBACK.
    """
    # Busca logs no diretório raiz e em subpastas de projeto (MIGRACAO_*)
    log_patterns = ["logs/*.log", "MIGRACAO_*/logs/*.log", "work/logs/*.log"]
    files = []
    for pattern in log_patterns:
        files.extend(glob.glob(pattern))

    # Ordena por data de modificação (mais recentes primeiro)
    files = sorted(files, key=os.path.getmtime, reverse=True)

    if not files:
        return "Nenhum arquivo de log encontrado nos locais pesquisados (./logs, MIGRACAO_*/logs)."

    # Vamos verificar os 3 arquivos mais recentes se houver filtro de erro, ou apenas o 1º se não houver
    files_to_check = files[:3] if filter_error else files[:1]
    output = []

    for log_path in files_to_check:
        try:
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.readlines()
                last_lines = content[-lines:]

                if filter_error:
                    matches = [line.strip() for line in last_lines 
                               if any(x in line.upper() for x in ["ERROR", "EXCEPTION", "TRACEBACK", "WARNING", "FAIL"])]
                    if matches:
                        output.append(f"--- Arquivo: {log_path} (Apenas Erros/Avisos) ---")
                        output.extend(matches[-20:]) # Limita a 20 erros por arquivo para economizar tokens
                    continue

                output.append(f"--- Arquivo: {log_path} (Últimas {lines} linhas) ---")
                output.extend([l.strip() for l in last_lines])
        except Exception as e:
            output.append(f"Erro ao ler log {log_path}: {e}")

    if not output:
        return "Nenhum erro encontrado nos logs recentes ou arquivos estão vazios."

    return "\n".join(output)

@mcp.tool()
def get_firebird_table_count(table_name: str) -> str:
    """Conta registros no Firebird usando conexão segura (ReadOnly)."""
    return get_firebird_table_count_safe(table_name)

@mcp.tool()
def get_firebird_table_count_safe(table_name: str) -> str:
    """Conta registros no Firebird usando conexão segura (ReadOnly)."""
    try:
        conn = get_safe_fb_connection()
        cur = conn.cursor()
        cur.execute(f'SELECT count(*) FROM "{table_name.upper()}"')
        count = cur.fetchone()[0]
        conn.close()
        return f"Tabela {table_name.upper()}: {count} registros."
    except Exception as e:
        return f"Erro ao contar registros no Firebird (ReadOnly): {e}"

@mcp.tool()
def run_count_comparison() -> str:
    """Executa o script de comparação de contagem entre Firebird e Postgres."""
    try:
        result = subprocess.run([sys.executable, "compara_cont_fb2pg.py"], capture_output=True, text=True)
        return f"Saída do Comparador:\n{result.stdout}\n{result.stderr}"
    except Exception as e:
        return f"Erro ao executar comparador: {e}"

@mcp.tool()
def generate_migration_report() -> str:
    """Gera o relatório HTML de comparação de estrutura."""
    try:
        result = subprocess.run([sys.executable, "gera_relatorio_compara_estrutura_fb2pg_html.py"], capture_output=True, text=True)
        return f"Relatório gerado com sucesso.\nSaída: {result.stdout}"
    except Exception as e:
        return f"Erro ao gerar relatório: {e}"

@mcp.tool()
def open_html_report() -> str:
    """Tenta abrir o último relatório HTML gerado no navegador."""
    html_files = sorted(glob.glob("*.html"), key=os.path.getmtime, reverse=True)
    if not html_files:
        return "Nenhum arquivo HTML encontrado."
    
    latest_html = html_files[0]
    try:
        if os.name == 'nt':
            os.startfile(latest_html)
            return f"Relatório {latest_html} aberto no navegador."
        return f"Arquivo encontrado: {latest_html}. (Comando 'open' não disponível neste OS)"
    except Exception as e:
        return f"Erro ao abrir arquivo: {e}"

@mcp.tool()
def execute_readonly_sql_postgres(sql: str) -> str:
    """Executa SQL SELECT no Postgres usando conexão segura."""
    if "SELECT" not in sql.upper():
        return "Erro: Apenas comandos SELECT são permitidos por segurança."
    try:
        conn = get_safe_pg_connection()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchmany(100)
        conn.close()
        return str(rows)
    except Exception as e:
        return f"Erro ao executar SQL no Postgres (ReadOnly): {e}"

if __name__ == "__main__":
    mcp.run()
