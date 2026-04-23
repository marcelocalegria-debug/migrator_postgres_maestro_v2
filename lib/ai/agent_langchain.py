import os
import json
import warnings
from typing import Optional, List, Any
from dotenv import load_dotenv

# Silencia warnings de bibliotecas
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# LangChain components
from langchain_openai import ChatOpenAI
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_community.chat_message_histories import SQLChatMessageHistory
from langchain_core.runnables.history import RunnableWithMessageHistory

# Importação hipotética dos seus MCPs e Skills configurados como @tool do LangChain
# from mcps.firebird_mcp import get_firebird_schema, run_firebird_query
# from mcps.postgres_mcp import get_postgres_schema
# from mcps.file_mcp import read_migration_logs
# from skills.migration_rules import check_datatype_conversion_rules

load_dotenv()

class MigrationAIAgent:
    """
    Agente Autônomo para Migração de Banco de Dados.
    Integra-se via OpenRouter, gerencia memória por sessão e consome MCPs/Skills.
    """

    def __init__(self, session_id: str, db_audit_path: str = "sqlite+aiosqlite:///migration_audit.db"):
        self.session_id = session_id
        self.db_audit_path = db_audit_path
        
        # Configurações do OpenRouter
        self.model_name = os.getenv("MODEL", "moonshotai/kimi-k2.6")
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        self.base_url = os.getenv("OPENROUTER_URL", "https://openrouter.ai/api/v1")

        if not self.api_key:
            raise ValueError("OPENROUTER_API_KEY não encontrada nas variáveis de ambiente.")

        # 1. Configurar o LLM Agnóstico
        # O OpenRouter é 100% compatível com a interface de cliente da OpenAI
        self.llm = ChatOpenAI(
            model=self.model_name,
            api_key=self.api_key,
            base_url=self.base_url,
            temperature=0.2, # Baixa temperatura para respostas mais determinísticas em SQL
            default_headers={
                "HTTP-Referer": "https://alegriadb.com", # Ajuda na identificação do OpenRouter
                "X-Title": "Agente de Migracao"
            }
        )

        # 2. Carregar Tools (MCPs e Skills do diretório local)
        self.tools = self._load_tools()

        # 3. Inicializar o Agente
        self.agent_executor = self._build_agent()

    def _load_tools(self) -> List[Any]:
        """
        Carrega as ferramentas e skills disponíveis nos diretórios ./mcps e ./skills.
        O LLM usará as docstrings e tipagens destas funções para saber quando e como chamá-las.
        """
        return [
            # get_firebird_schema,
            # get_postgres_schema,
            # read_migration_logs,
            # check_datatype_conversion_rules
        ]

    def _build_agent(self) -> AgentExecutor:
        """Constrói o prompt do sistema e amarra o LLM com as ferramentas."""
        
        system_prompt = """Você é um Engenheiro de Dados e DBA Especialista atuando como Agente de Migração.
Seu objetivo principal é auxiliar na migração de estruturas e dados do Firebird para o PostgreSQL 18.

Você tem acesso a ferramentas (MCPs) para consultar esquemas, ler arquivos de log no projeto Python, 
e Skills que definem o que é permitido ou não durante a conversão.

Diretrizes:
1. Sempre verifique as regras nas Skills antes de propor scripts de conversão de tipos de dados.
2. Se um erro ocorrer, use a ferramenta de leitura de logs para entender o contexto antes de responder.
3. Seja direto, técnico e forneça código SQL ou Python limpo e comentado.
"""

        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            MessagesPlaceholder(variable_name="history"), # Memória persistente injetada aqui
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad"), # Espaço para o agente "pensar" com as tools
        ])

        # Cria um agente capaz de acionar múltiplas ferramentas
        agent = create_tool_calling_agent(self.llm, self.tools, prompt)
        
        return AgentExecutor(
            agent=agent, 
            tools=self.tools, 
            verbose=True, 
            handle_parsing_errors=True
        )

    def _get_session_history(self, session_id: str):
        """Retorna ou cria o histórico de conversa no banco de dados de auditoria."""
        return SQLChatMessageHistory(
            session_id=session_id, 
            connection_string=self.db_audit_path
        )

    def execute_task(self, user_input: str) -> str:
        """
        Ponto de entrada para conversar com o agente ou enviar tarefas.
        Toda a interação é salva na memória vinculada ao ID_SESSAO.
        """
        agent_with_history = RunnableWithMessageHistory(
            self.agent_executor,
            self._get_session_history,
            input_messages_key="input",
            history_messages_key="history",
        )

        try:
            response = agent_with_history.invoke(
                {"input": user_input},
                config={"configurable": {"session_id": self.session_id}}
            )
            return response["output"]
        except Exception as e:
            # Fallback seguro para logs
            return f"-- Falha na execução do Agente: {str(e)} --"