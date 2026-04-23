import os
import sys
import logging

# 1. Configurações de ambiente e silenciamento de logs (deve ser o PRIMEIRO de tudo)
os.environ["PYTHONWARNINGS"] = "ignore"
os.environ["LITELLM_VERBOSE"] = "False"
os.environ["SUPPRESS_LITELLM_LOGGING"] = "True"

import warnings
warnings.filterwarnings("ignore")
# Filtro específico para o chato do Authlib/joserfc
warnings.filterwarnings("ignore", message=".*authlib.jose module is deprecated.*")

# Redireciona warnings do Python para o logging e silencia o logger de warnings
logging.captureWarnings(True)
logging.getLogger("py.warnings").setLevel(logging.CRITICAL)

import asyncio
import contextlib

import litellm
# Configurações programáticas do LiteLLM para silenciar o "Provider List"
litellm.suppress_debug_info = True
litellm.set_verbose = False
litellm._turn_off_debug_setup = True

# Silencia loggers de bibliotecas conhecidas por serem ruidosas
for logger_name in ["litellm", "google.adk", "authlib", "fdb", "psycopg2", "py.warnings"]:
    logging.getLogger(logger_name).setLevel(logging.CRITICAL)

from typing import Optional
from pathlib import Path
from dotenv import load_dotenv

# Importações do Google ADK (agora com ambiente limpo)
from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm
from google.adk.runners import Runner
from google.genai import types

# Gerenciamento de Sessão para Memória Persistente (Auditoria)
from google.adk.sessions import DatabaseSessionService

# Integração MCP nativa do ADK
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StdioConnectionParams
from mcp import StdioServerParameters

# Carrega variáveis do .env
load_dotenv()

class MigrationAIAgent:
    """
    Agente Autônomo construído com Google ADK para Migração Firebird -> PostgreSQL.
    Acessa ferramentas via MCP, usa OpenRouter e mantém memória persistente por sessão.
    """

    def __init__(self, db_audit_path: str = "sqlite+aiosqlite:///migration_audit.db"):
        self.db_audit_path = db_audit_path
        
        # 1. Configurar o Modelo via OpenRouter usando LiteLLM no ADK
        raw_model_name = os.getenv("MODEL", "moonshotai/kimi-k2.5")
        self.model_id = f"openrouter/{raw_model_name}" if not raw_model_name.startswith("openrouter/") else raw_model_name
        
        # Redireciona stdout para devnull durante o init do LiteLlm para evitar o "Provider List"
        with open(os.devnull, 'w') as fnull:
            with contextlib.redirect_stdout(fnull):
                self.model = LiteLlm(
                    model=self.model_id,
                    api_key=os.getenv("OPENROUTER_API_KEY"),
                    api_base=os.getenv("OPENROUTER_URL", "https://openrouter.ai/api/v1")
                )

        # 2. Configurar o Serviço de Sessão (Memória/Auditoria)
        self.session_service = DatabaseSessionService(db_url=self.db_audit_path)

        # 3. Carregar Ferramentas (MCPs e Skills)
        self.tools = self._load_mcps_and_skills()

        # 4. Construir o Agente ADK
        self.agent = self._build_agent()

        # 5. O Runner orquestra o Agente, as Ferramentas e a Sessão
        self.runner = Runner(
            app_name="MigrationAgent",
            agent=self.agent,
            session_service=self.session_service,
            auto_create_session=True
        )

    def _load_mcps_and_skills(self) -> list:
        """
        Carrega os servidores MCP locais (via stdio) e as Skills.
        """
        tools = []
        
        # [FIX] Usa o executável atual do Python e caminho absoluto para o MCP
        # Localiza a raiz do projeto
        root_dir = Path(__file__).parent.parent.parent
        mcp_script = root_dir / "mcps" / "db_migration_server.py"
        
        firebird_postgres_mcp = McpToolset(
            connection_params=StdioConnectionParams(
                server_params=StdioServerParameters(
                    command=sys.executable,
                    args=[str(mcp_script.absolute())],
                    # Define explicitamente o env e cwd para o processo do MCP
                    env={**os.environ, "PYTHONPATH": str(root_dir.absolute())}
                )
            )
        )
        tools.append(firebird_postgres_mcp)
        
        return tools

    def _build_agent(self) -> Agent:
        """Define o comportamento, as ferramentas e a identidade do agente."""
        instruction = """Você é um Engenheiro de Dados e DBA Especialista atuando como Agente de Migração.
Seu objetivo principal é auxiliar na migração de estruturas e dados do Firebird para o PostgreSQL 18.

Você tem acesso a servidores MCP para consultar esquemas, ler logs de erros da aplicação Python 
e usar skills de conversão e regras de negócio.

Diretrizes:
1. Sempre verifique as regras de conversão antes de sugerir um script.
2. Ao receber um erro, utilize as ferramentas para ler o log antes de dar um diagnóstico.
3. Retorne soluções focadas, preferencialmente com código SQL/Python estruturado.
"""
        return Agent(
            name="FirebirdToPostgresAgent",
            model=self.model,
            instruction=instruction,
            tools=self.tools
        )

    async def execute_task(self, session_id: str, user_input: str) -> str:
        """
        Interage com o agente usando a API assíncrona do ADK Runner.
        """
        try:
            full_response = ""
            user_id = "default_user"
            
            new_message = types.UserContent(parts=[types.Part(text=user_input)])

            async for event in self.runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message=new_message
            ):
                if event.content and event.content.parts:
                    for part in event.content.parts:
                        if part.text:
                            full_response += part.text
            
            return full_response.strip()
        except Exception as e:
            return f"-- Falha na execução do Agente ADK: {str(e)} --"

if __name__ == "__main__":
    async def main():
        print("--- Iniciando Teste de Conexão (IA + MCP) ---")
        agent = MigrationAIAgent()
        print("Agente ADK e MCP vinculados com sucesso!")
        
        session_id = "teste_unitario_conexao"
        pergunta = "Olá! Por favor, teste as conexões com os bancos e depois me diga quantas tabelas de usuário existem no Firebird."
        
        print(f"\nUsuário: {pergunta}")
        print("Aguardando resposta da IA (processando ferramentas MCP)...")
        
        response = await agent.execute_task(session_id, pergunta)
        
        print(f"\nIA: {response}")
        print("\n--- Teste Finalizado ---")

    asyncio.run(main())
