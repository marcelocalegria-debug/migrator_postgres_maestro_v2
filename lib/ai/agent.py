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

    def __init__(self, project_path: Optional[str] = None):
        # 0. Define o diretório do projeto e banco de auditoria
        self.root_dir = Path(__file__).parent.parent.parent
        if project_path:
            self.project_path = Path(project_path)
        else:
            # Tenta pegar da variável de ambiente ou usa a mais recente
            env_proj = os.getenv("MIGRATION_PROJECT_PATH")
            if env_proj:
                self.project_path = Path(env_proj)
            else:
                migration_dirs = sorted(list(self.root_dir.glob("MIGRACAO_*")), reverse=True)
                self.project_path = migration_dirs[0] if migration_dirs else self.root_dir

        self.db_audit_path = f"sqlite+aiosqlite:///{self.project_path / 'migration_audit.db'}"
        
        # Configura variável de ambiente para o MCP ler o config correto
        os.environ["MIGRATION_CONFIG_PATH"] = str(self.project_path / "config.yaml")

        # 1. Configurar o Modelo via OpenRouter usando LiteLLM no ADK
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key or api_key.startswith("#"):
            self.model = None
            return

        raw_model_name = os.getenv("MODEL", "moonshotai/kimi-k2.5")
        self.model_id = f"openrouter/{raw_model_name}" if not raw_model_name.startswith("openrouter/") else raw_model_name
        
        # Redireciona stdout para devnull durante o init do LiteLlm para evitar o "Provider List"
        with open(os.devnull, 'w') as fnull:
            with contextlib.redirect_stdout(fnull):
                self.model = LiteLlm(
                    model=self.model_id,
                    api_key=api_key,
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
        
        # Localiza o script do MCP
        mcp_script = self.root_dir / "mcps" / "db_migration_server.py"
        
        firebird_postgres_mcp = McpToolset(
            connection_params=StdioConnectionParams(
                server_params=StdioServerParameters(
                    command=sys.executable,
                    args=[str(mcp_script.absolute())],
                    # Passa o caminho do projeto para o MCP
                    env={
                        **os.environ, 
                        "PYTHONPATH": str(self.root_dir.absolute()),
                        "MIGRATION_CONFIG_PATH": str(self.project_path / "config.yaml")
                    }
                ),
                timeout=60 # Aumentado de 5s (padrão) para 60s
            )
        )
        tools.append(firebird_postgres_mcp)
        
        return tools

    def _get_skills_instructions(self) -> str:
        """Lê arquivos .md da pasta skills/ para adicionar às instruções do agente."""
        skills_dir = self.root_dir / "skills"
        skills_text = ""
        if skills_dir.exists():
            for skill_file in skills_dir.glob("*.md"):
                with open(skill_file, "r", encoding="utf-8") as f:
                    skills_text += f"\n\n--- SKILL: {skill_file.name} ---\n"
                    skills_text += f.read()
        return skills_text

    def _build_agent(self) -> Agent:
        """Define o comportamento, as ferramentas e a identidade do agente."""
        instruction = f"""Você é um Engenheiro de Dados especialista em migração Firebird -> PostgreSQL.
PROJETO: {self.project_path.name}

DIRETRIZES DE RESPOSTA:
1. Seja EXTREMAMENTE CONCISO. Use tabelas para dados e blocos de código para SQL.
2. Foco exclusivo em migração (schema, dados, performance, constraints). 
3. Não responda sobre assuntos fora do escopo de migração de banco de dados.
4. Se uma ferramenta falhar ou der timeout, informe o erro técnico brevemente e sugira o próximo passo manual.

CONTEXTO TÉCNICO:
- Firebird 3.0 (WIN1252) -> PostgreSQL 18+ (UTF8/LATIN1).
- O processo utiliza COPY para alta performance.
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
        if not self.model:
            return "⚠️ Funcionalidade de Agente IA desativada: OPENROUTER_API_KEY não configurada ou inválida no .env."

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
            if "tool_call_id" in str(e):
                return "Ocorreu um erro de sincronização nas ferramentas. Por favor, repita a pergunta."
            return f"-- Agente Indisponível: {str(e)} --"

if __name__ == "__main__":
    async def chat_loop():
        from rich.console import Console
        from rich.prompt import Prompt
        from rich.panel import Panel
        
        console = Console()
        console.print(Panel("[bold cyan]Migration AI Agent - Chatbot Interativo[/bold cyan]"))
        
        # Listar projetos de migração
        root_dir = Path(__file__).parent.parent.parent
        migration_dirs = sorted([d for d in root_dir.glob("MIGRACAO_*") if d.is_dir()], reverse=True)
        
        if not migration_dirs:
            console.print("[red]Nenhum diretório MIGRACAO_???? encontrado![/red]")
            return

        choices = [d.name for d in migration_dirs]
        console.print(f"Projetos encontrados: {', '.join(choices)}")
        selected_migration = Prompt.ask("Selecione o projeto de migração", choices=choices, default=choices[0])
        
        project_path = root_dir / selected_migration
        agent = MigrationAIAgent(project_path=str(project_path))
        
        console.print(f"\n[green]Agente carregado para {selected_migration}[/green]")
        console.print("[dim]Digite 'sair' ou 'exit' para encerrar.[/dim]\n")
        
        session_id = f"chat_{selected_migration}"
        
        while True:
            user_input = Prompt.ask("[bold blue]Você[/bold blue]")
            if user_input.lower() in ["sair", "exit", "quit"]:
                break
                
            console.print("[bold yellow]IA pensando...[/bold yellow]")
            response = await agent.execute_task(session_id, user_input)
            console.print(f"\n[bold green]IA:[/bold green] {response}\n")

    asyncio.run(chat_loop())
