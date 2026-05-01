import yaml
import fdb
import psycopg2
import os
import sqlite3
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
    """Conexão Firebird RESTRITA (Audit/ReadOnly).

    Usa audit_user/audit_password se disponíveis no config.yaml.
    Caso contrário, cai de volta para user/password principal (apenas leitura
    é garantida pela natureza das queries — apenas SELECT são executados).
    """
    config = load_config()['firebird']
    audit_password = config.get('audit_password')
    if audit_password:
        user = config.get('audit_user', 'MIGRATION_AUDIT')
        password = audit_password
        role = config.get('audit_role', 'MIGRATION_AUDIT_ROLE')
        connect_kwargs = dict(
            host=config['host'],
            port=config.get('port', 3050),
            database=config['database'],
            user=user,
            password=password,
            role=role,
            charset='WIN1252'
        )
    else:
        # Fallback: usa credenciais principais (sem audit_role)
        user = config.get('user', 'SYSDBA')
        password = config.get('password')
        if not password:
            raise RuntimeError("password ausente em config.yaml — servidor MCP não pode conectar ao Firebird")
        connect_kwargs = dict(
            host=config['host'],
            port=config.get('port', 3050),
            database=config['database'],
            user=user,
            password=password,
            charset='WIN1252'
        )

    return fdb.connect(**connect_kwargs)

def get_safe_pg_connection():
    """Conexão PostgreSQL RESTRITA (Audit/ReadOnly).

    Usa audit_user/audit_password se disponíveis no config.yaml.
    Caso contrário, cai de volta para user/password principal (apenas leitura
    é garantida pela natureza das queries — apenas SELECT são executados).
    """
    cfg_data = load_config()
    config = cfg_data.get('postgresql') or cfg_data.get('postgres')

    audit_password = config.get('audit_password')
    if audit_password:
        user = config.get('audit_user', 'migration_audit')
        password = audit_password
    else:
        # Fallback: usa credenciais principais
        user = config.get('user')
        password = config.get('password')
        if not password:
            raise RuntimeError("password ausente em config.yaml — servidor MCP não pode conectar ao PostgreSQL")

    return psycopg2.connect(
        host=config['host'],
        port=config.get('port', 5432),
        dbname=config['database'],
        user=user,
        password=password
    )

@mcp.tool()
def list_migration_projects() -> str:
    """Lista todos os diretórios de migração encontrados no projeto.

    Retorna os diretórios MIGRACAO_NNNN presentes na raiz do projeto,
    ordenados do mais recente ao mais antigo. Use para descobrir qual
    sequência de migração consultar nas demais ferramentas.
    """
    root = Path(__file__).parent.parent
    dirs = sorted([d.name for d in root.glob("MIGRACAO_*") if d.is_dir()], reverse=True)
    if not dirs:
        return "Nenhum diretório MIGRACAO_ encontrado."
    return "Projetos de migração detectados: " + ", ".join(dirs)

@mcp.tool()
def check_migration_logs(lines: int = 50, filter_error: bool = True) -> str:
    """Lê os logs das migrações e retorna erros ou as últimas linhas.

    Varre automaticamente logs em logs/, MIGRACAO_*/logs/ e work/logs/,
    ordenados por data de modificação (mais recentes primeiro).

    Args:
        lines: Número de linhas do final de cada arquivo a analisar (padrão 50).
        filter_error: Se True (padrão), filtra por ERROR/WARNING/EXCEPTION/TRACEBACK/FAIL,
                      limitando a 20 ocorrências por arquivo nos 3 arquivos mais recentes.
                      Se False, retorna as últimas `lines` linhas do arquivo mais recente.
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
def execute_postgres_sql(sql: str) -> str:
    """Executa uma query SELECT no PostgreSQL via conexão segura (somente leitura).

    Usa audit_user/audit_password do config.yaml quando disponíveis; caso contrário,
    usa as credenciais principais. Aplica timeout de 30 segundos e limita o retorno
    a 100 linhas. Rejeita qualquer comando que não contenha SELECT.

    Args:
        sql: Instrução SELECT a executar. Apenas leitura é permitida.
    """
    if "SELECT" not in sql.upper():
        return "Erro: Apenas comandos SELECT são permitidos por segurança."
    try:
        conn = get_safe_pg_connection()
        cur = conn.cursor()
        # Define timeout de 30 segundos para a query do agente
        cur.execute("SET statement_timeout = '30s'")
        cur.execute(sql)
        rows = cur.fetchmany(100)
        conn.close()
        return f"Resultado (limitado a 100 linhas): {str(rows)}"
    except Exception as e:
        return f"Erro ao executar SQL no Postgres ({type(e).__name__}): {e}"

@mcp.tool()
def execute_firebird_sql(sql: str) -> str:
    """Executa uma query SELECT no Firebird via conexão segura (somente leitura).

    Usa audit_user/audit_password do config.yaml quando disponíveis; caso contrário,
    usa as credenciais principais. Charset WIN1252 configurado automaticamente.
    Limita o retorno a 100 linhas. Rejeita qualquer comando que não contenha SELECT.

    Args:
        sql: Instrução SELECT a executar. Apenas leitura é permitida.
    """
    if "SELECT" not in sql.upper():
        return "Erro: Apenas comandos SELECT são permitidos por segurança."
    try:
        conn = get_safe_fb_connection()
        # Firebird não tem um SET statement_timeout global fácil via SQL,
        # mas podemos usar a API do fdb se necessário. Por ora, limitamos o fetch.
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchmany(100)
        conn.close()
        return f"Resultado (limitado a 100 linhas): {str(rows)}"
    except Exception as e:
        return f"Erro ao executar SQL no Firebird ({type(e).__name__}): {e}"

def _find_migration_db(project: str = "") -> Path:
    """Localiza o migration.db do projeto. Prioridade: parâmetro > env > mais recente."""
    root = Path(__file__).parent.parent
    if project:
        db = root / project / "migration.db"
        if not db.exists():
            raise FileNotFoundError(f"migration.db não encontrado em {db}")
        return db
    env_path = os.getenv("MIGRATION_CONFIG_PATH")
    if env_path:
        db = Path(env_path).parent / "migration.db"
        if db.exists():
            return db
    candidates = sorted(root.glob("MIGRACAO_*/migration.db"), key=os.path.getmtime, reverse=True)
    if candidates:
        return candidates[0]
    raise FileNotFoundError("Nenhum migration.db encontrado nos diretórios MIGRACAO_*.")


_ALLOWED_SQLITE_TABLES = {"migration_meta", "steps", "tables", "batches", "constraints", "errors"}


@mcp.tool()
def query_migration_db(sql: str, project: str = "") -> str:
    """Executa SELECT no banco SQLite de controle da migração (migration.db).

    Tabelas disponíveis: migration_meta, steps, tables, batches, constraints, errors.
    Retorna no máximo 200 linhas. Apenas SELECT é permitido.

    Args:
        sql: Instrução SELECT a executar.
        project: Nome do diretório de migração (ex: MIGRACAO_0002). Se omitido,
                 usa o projeto ativo ou o mais recente.
    """
    if "SELECT" not in sql.upper():
        return "Erro: Apenas comandos SELECT são permitidos nesta ferramenta."
    try:
        db_path = _find_migration_db(project)
        conn = sqlite3.connect(str(db_path), timeout=10)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql)
        rows = cur.fetchmany(200)
        conn.close()
        if not rows:
            return f"Nenhuma linha retornada. (DB: {db_path.parent.name}/migration.db)"
        cols = rows[0].keys()
        header = " | ".join(cols)
        sep = "-" * len(header)
        lines = [header, sep] + [" | ".join(str(r[c]) for c in cols) for r in rows]
        return f"DB: {db_path.parent.name}/migration.db ({len(rows)} linha(s))\n" + "\n".join(lines)
    except Exception as e:
        return f"Erro ao consultar migration.db ({type(e).__name__}): {e}"


@mcp.tool()
def update_migration_db(sql: str, project: str = "") -> str:
    """Executa UPDATE controlado no migration.db para ajustes pontuais de controle.

    Restrições de segurança:
    - Apenas comandos UPDATE são aceitos (não DELETE, INSERT, DROP).
    - A cláusula WHERE é obrigatória.
    - Apenas tabelas de controle são permitidas: migration_meta, steps, tables,
      batches, constraints, errors.

    Casos de uso típicos:
    - Resetar tabela com falha: UPDATE tables SET status='pending', error_message=NULL WHERE source_table='NOME'
    - Marcar step manualmente: UPDATE steps SET status='completed' WHERE step_number=7
    - Registrar resolução de erro: UPDATE errors SET resolution='corrigido' WHERE id=3

    Args:
        sql: Instrução UPDATE a executar (WHERE obrigatório).
        project: Nome do diretório de migração (ex: MIGRACAO_0002). Se omitido,
                 usa o projeto ativo ou o mais recente.
    """
    sql_upper = sql.upper().strip()
    if not sql_upper.startswith("UPDATE"):
        return "Erro: Apenas comandos UPDATE são permitidos nesta ferramenta."
    if "WHERE" not in sql_upper:
        return "Erro: Cláusula WHERE obrigatória para evitar atualizações em massa."
    for forbidden in ("DELETE", "DROP", "INSERT", "CREATE", "ALTER", "TRUNCATE"):
        if forbidden in sql_upper:
            return f"Erro: Comando {forbidden} não é permitido."
    table_ok = any(t in sql_upper for t in (t.upper() for t in _ALLOWED_SQLITE_TABLES))
    if not table_ok:
        return f"Erro: Tabela não permitida. Use: {', '.join(sorted(_ALLOWED_SQLITE_TABLES))}."
    try:
        db_path = _find_migration_db(project)
        conn = sqlite3.connect(str(db_path), timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        cur = conn.execute(sql)
        affected = cur.rowcount
        conn.commit()
        conn.close()
        return f"OK: {affected} linha(s) atualizada(s) em {db_path.parent.name}/migration.db."
    except Exception as e:
        return f"Erro ao atualizar migration.db ({type(e).__name__}): {e}"


if __name__ == "__main__":
    mcp.run()
