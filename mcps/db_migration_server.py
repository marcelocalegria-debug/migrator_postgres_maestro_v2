import yaml
import fdb
import psycopg2
import os
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
mcp = FastMCP("Migracao_DB_MCP")

def load_config():
    """Lê o arquivo de configuração YAML dinamicamente."""
    # 1. Tenta via variável de ambiente (Obrigatório se estiver rodando via Agente ADK)
    env_path = os.getenv("MIGRATION_CONFIG_PATH")
    if env_path:
        p = Path(env_path)
        if p.exists():
            with open(p, "r", encoding="utf-8") as file:
                return yaml.safe_load(file)
        else:
            raise FileNotFoundError(f"Variavel MIGRATION_CONFIG_PATH aponta para arquivo inexistente: {env_path}")

    # 2. Tenta no diretório atual (Fallback para uso manual)
    paths_to_try = [
        Path("config.yaml"),
        Path(__file__).parent.parent / "config.yaml"
    ]
    
    for p in paths_to_try:
        if p.exists():
            with open(p, "r", encoding="utf-8") as file:
                return yaml.safe_load(file)
                
    raise FileNotFoundError("Arquivo config.yaml não encontrado. Defina MIGRATION_CONFIG_PATH para a pasta correta.")

# ─── Charset Helpers ──────────────────────────────────────────────────────────
_CONFIG_CHARSET_TO_FB = {
    'iso-8859-1': 'ISO8859_1', 'iso8859-1': 'ISO8859_1',
    'iso_8859-1': 'ISO8859_1', 'latin1':    'ISO8859_1',
    'latin-1':    'ISO8859_1', 'win1252':   'WIN1252',
    'windows-1252': 'WIN1252', 'cp1252':    'WIN1252',
    'utf-8':      'UTF8',      'utf8':      'UTF8',
}

def _fb_charset_for_connect(raw: str) -> str:
    if not raw: return 'WIN1252'
    return _CONFIG_CHARSET_TO_FB.get(raw.lower(), raw.upper())

def get_firebird_connection():
    """Retorna uma conexão ativa com o Firebird."""
    config = load_config()['firebird']
    charset = _fb_charset_for_connect(config.get('charset', 'WIN1252'))
    return fdb.connect(
        host=config['host'],
        port=config.get('port', 3050),
        database=config['database'],
        user=config['user'],
        password=config['password'],
        charset=charset
    )

def get_postgres_connection():
    """Retorna uma conexão ativa com o PostgreSQL."""
    cfg_data = load_config()
    config = cfg_data.get('postgresql') or cfg_data.get('postgres')
    if not config:
        raise ValueError("Seção 'postgresql' ou 'postgres' não encontrada no config.")
        
    return psycopg2.connect(
        host=config['host'],
        port=config.get('port', 5432),
        dbname=config['database'],
        user=config['user'],
        password=config['password'],
        options=f"-c search_path={config.get('schema', 'public')}"
    )

# =====================================================================
# AS FERRAMENTAS (TOOLS) QUE O AGENTE IA VAI ENXERGAR
# =====================================================================

@mcp.tool()
def test_connections() -> str:
    """Testa se as conexões com Firebird e PostgreSQL estão ativas."""
    results = []
    
    # Teste Firebird
    try:
        fb_conn = get_firebird_connection()
        results.append("Firebird: Conexão OK.")
        fb_conn.close()
    except Exception as e:
        results.append(f"Firebird: Falha - {str(e)}")

    # Teste Postgres
    try:
        pg_conn = get_postgres_connection()
        results.append("PostgreSQL: Conexão OK.")
        pg_conn.close()
    except Exception as e:
        results.append(f"PostgreSQL: Falha - {str(e)}")

    return "\n".join(results)

@mcp.tool()
def get_firebird_table_schema(table_name: str) -> str:
    """
    Busca os metadados (colunas e tipos) de uma tabela no Firebird.
    Use esta ferramenta para entender a origem antes de migrar.
    """
    query = f"""
        SELECT 
            r.RDB$FIELD_NAME AS field_name,
            f.RDB$FIELD_TYPE AS field_type,
            f.RDB$FIELD_LENGTH AS field_length
        FROM RDB$RELATION_FIELDS r
        LEFT JOIN RDB$FIELDS f ON r.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
        WHERE r.RDB$RELATION_NAME = '{table_name.upper()}'
    """
    try:
        conn = get_firebird_connection()
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()
        
        if not rows:
            return f"Tabela '{table_name}' não encontrada no Firebird."
            
        # Formata o retorno para o LLM ler facilmente
        schema_text = f"Schema Firebird da tabela {table_name}:\n"
        for row in rows:
            schema_text += f"- Coluna: {row[0].strip()}, Tipo Interno: {row[1]}, Tamanho: {row[2]}\n"
        return schema_text
    except Exception as e:
        return f"Erro ao ler schema do Firebird: {str(e)}"

@mcp.tool()
def count_firebird_tables() -> str:
    """Conta o número total de tabelas de usuário no Firebird."""
    query = "SELECT count(*) FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG = 0 AND RDB$VIEW_BLR IS NULL"
    try:
        conn = get_firebird_connection()
        cur = conn.cursor()
        cur.execute(query)
        count = cur.fetchone()[0]
        conn.close()
        return f"Total de tabelas de usuário no Firebird: {count}"
    except Exception as e:
        return f"Erro ao contar tabelas no Firebird: {str(e)}"

@mcp.tool()
def get_firebird_table_count(table_name: str) -> str:
    """Retorna o número de registros (count) de uma tabela no Firebird."""
    query = f"SELECT count(*) FROM {table_name.upper()}"
    try:
        conn = get_firebird_connection()
        cur = conn.cursor()
        cur.execute(query)
        count = cur.fetchone()[0]
        conn.close()
        return f"Tabela {table_name.upper()} no Firebird possui {count:,} registros."
    except Exception as e:
        return f"Erro ao contar registros na tabela {table_name} do Firebird: {str(e)}"

@mcp.tool()
def execute_firebird_sql(sql: str) -> str:
    """
    Executa um comando SQL (apenas DDL ou SELECT) no Firebird.
    Use para investigar dados ou metadados na origem.
    """
    sql_upper = sql.upper().strip()
    # Guardrail básico de segurança
    forbidden = ["DROP DATABASE", "DELETE FROM", "TRUNCATE", "UPDATE ", "DROP TABLE"]
    for word in forbidden:
        if word in sql_upper:
            return f"ERRO DE SEGURANÇA: O comando '{word}' não é permitido via MCP no Firebird."
            
    try:
        conn = get_firebird_connection()
        cur = conn.cursor()
        cur.execute(sql)
        
        if sql_upper.startswith("SELECT"):
            # Limita a 50 linhas para não explodir o contexto
            rows = cur.fetchmany(50)
            desc = [d[0] for d in cur.description]
            cur.close()
            conn.close()
            return f"Resultado (limitado a 50 linhas):\nColunas: {desc}\nDados: {str(rows)}"
            
        conn.commit()
        cur.close()
        conn.close()
        return "Comando SQL executado com sucesso no Firebird."
    except Exception as e:
        return f"Erro ao executar SQL no Firebird: {str(e)}"

@mcp.tool()
def get_postgres_table_schema(table_name: str) -> str:
    """
    Busca os metadados (colunas e tipos) de uma tabela no PostgreSQL.
    Use esta ferramenta para comparar com o Firebird.
    """
    query = """
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute(query, (table_name.lower(),))
        rows = cur.fetchall()
        conn.close()
        
        if not rows:
            return f"Tabela '{table_name}' não encontrada no PostgreSQL."
            
        schema_text = f"Schema PostgreSQL da tabela {table_name}:\n"
        for row in rows:
            schema_text += f"- Coluna: {row[0]}, Tipo: {row[1]}, Tamanho: {row[2]}, Nullable: {row[3]}\n"
        return schema_text
    except Exception as e:
        return f"Erro ao ler schema do PostgreSQL: {str(e)}"

@mcp.tool()
def execute_postgres_sql(sql: str) -> str:
    """
    Executa um comando SQL (apenas DDL ou SELECT) no PostgreSQL.
    Use para aplicar correções de schema (ALTER TABLE) sugeridas.
    """
    sql_upper = sql.upper().strip()
    # Guardrail básico de segurança
    forbidden = ["DROP DATABASE", "DELETE FROM", "TRUNCATE", "UPDATE ", "DROP TABLE"]
    for word in forbidden:
        if word in sql_upper:
            return f"ERRO DE SEGURANÇA: O comando '{word}' não é permitido via MCP."
            
    try:
        conn = get_postgres_connection()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql)
        
        if sql_upper.startswith("SELECT"):
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return f"Resultado do SELECT:\n{str(rows)}"
            
        cur.close()
        conn.close()
        return "Comando SQL executado com sucesso."
    except Exception as e:
        return f"Erro ao executar SQL no PostgreSQL: {str(e)}"

# Ponto de entrada exigido para MCP via stdio
if __name__ == "__main__":
    mcp.run()
