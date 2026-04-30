---
status: resolved
trigger: "O comando /agent no Maestro V2 falha ao tentar usar tools MCP para comparar linhas entre Firebird e PostgreSQL. O agente retorna erro genérico de conexão sem mostrar detalhes do erro real."
created: 2026-04-30T00:00:00Z
updated: 2026-04-30T12:00:00Z
---

## Current Focus

hypothesis: CONFIRMADO — Dois bugs distintos: (1) MCP tools suprimem o erro real (retornam string genérica em vez de raise), (2) McpToolset é criado no __init__ síncrono fora do event loop correto causando "original event loop is closed" no cleanup.
test: Evidências já coletadas — ambos confirmados por leitura direta do código.
expecting: n/a
next_action: Aplicar fix

## Symptoms

expected: Agente conecta ao MCP server (mcps/db_migration_server.py), executa tool de comparação de linhas e retorna resultado comparando tabela `versao_inst` no Firebird e PostgreSQL.
actual: Agente retorna "ERRO: Falha de conexão em ambos os bancos. Verifique credenciais e conectividade." — sem detalhes. Erros internos das tools/MCPs não são mostrados ao usuário.
errors: |
  Error cleaning up session stdio_session: original event loop is closed, resources may be leaked.
  AGENTE: ERRO: Falha de conexão em ambos os bancos. Verifique credenciais e conectividade.
reproduction: |
  1. Entrar no Maestro V2: python maestro.py
  2. Executar /agent
  3. Digitar: "compare as linhas da versao_inst no postgres e no firebird"
  4. Erro aparece imediatamente
started: Comportamento atual — não se sabe se funcionou antes.

## Eliminated

(none yet)

## Evidence

- timestamp: 2026-04-30T00:01:00Z
  checked: mcps/db_migration_server.py — get_safe_fb_connection() e get_safe_pg_connection()
  found: Ambas as funções exigem audit_user/audit_password — campos específicos de auditoria distintos de user/password. get_safe_fb_connection() lança RuntimeError("audit_password ausente") se audit_password não estiver no config.yaml. get_safe_pg_connection() faz o mesmo.
  implication: Se config.yaml não tiver audit_user/audit_password, a conexão falha com RuntimeError antes mesmo de tentar conectar.

- timestamp: 2026-04-30T00:01:10Z
  checked: mcps/db_migration_server.py — execute_postgres_sql() e execute_firebird_sql()
  found: Ambas as tools capturam Exception e retornam uma string genérica: "Erro ao executar SQL no Postgres ({type(e).__name__})" — sem incluir str(e) (a mensagem real do erro). O agente recebe uma string de erro, não uma exceção, e interpreta como "falha de conexão em ambos".
  implication: A causa real (RuntimeError: audit_password ausente) é completamente suprimida. O agente só vê "Erro ao executar SQL (...)" e produz a mensagem genérica ao usuário.

- timestamp: 2026-04-30T00:01:20Z
  checked: lib/cli.py — do_agent() → run_async_task()
  found: run_async_task() detecta que o loop está rodando (prompt_toolkit usa asyncio internamente) e então cria uma thread nova e chama asyncio.run(coro) dentro dela. asyncio.run() cria um novo event loop, executa, e o FECHA ao terminar. McpToolset é criado no __init__ de MigrationAIAgent (síncrono, fora de qualquer loop) e tenta fazer cleanup do stdio_session quando o loop recém-criado já foi fechado pela thread.
  implication: O erro "Error cleaning up session stdio_session: original event loop is closed" é consequência direta dessa estratégia de threading — McpToolset não consegue fazer cleanup porque o loop que foi usado para inicializá-lo foi fechado.

- timestamp: 2026-04-30T00:01:30Z
  checked: lib/ai/agent.py — __init__() — linha 101: self.tools = self._load_mcps_and_skills()
  found: McpToolset é instanciado no construtor síncrono de MigrationAIAgent, fora de qualquer event loop ativo. O Google ADK/McpToolset internamente pode criar referências ao loop atual (asyncio.get_event_loop()) neste momento — que é o loop do prompt_toolkit ou nenhum. Quando a coroutine é executada em uma thread nova com asyncio.run(), o McpToolset tenta usar o loop original que agora está fechado.
  implication: O McpToolset precisa ser criado DENTRO do contexto assíncrono correto (async with), não no __init__ síncrono.

## Resolution

root_cause: |
  Dois bugs distintos com efeito composto:

  BUG 1 (causa da mensagem genérica): mcps/db_migration_server.py — get_safe_fb_connection() e
  get_safe_pg_connection() exigiam campos audit_user/audit_password no config.yaml. Como esses
  campos não existem no config.yaml do projeto, ambas lançavam RuntimeError. Porém execute_firebird_sql()
  e execute_postgres_sql() capturavam a Exception e retornavam apenas f"Erro ao ... ({type(e).__name__})"
  — sem incluir str(e). O agente recebia essa string como resultado de tool call e, sem conseguir
  interpretar, produzia a mensagem genérica "ERRO: Falha de conexão em ambos os bancos."

  BUG 2 (causa do "original event loop is closed"): lib/cli.py — do_agent() criava MigrationAIAgent
  (e com ele o McpToolset) no thread principal ANTES de entrar na thread async. A thread async usava
  asyncio.run() que cria um novo event loop, executa a coroutine e FECHA o loop ao terminar. O McpToolset
  tentava fazer cleanup no evento "stdio_session" usando o loop original — que já havia sido fechado.

fix: |
  FIX 1 — mcps/db_migration_server.py:
  - get_safe_fb_connection() e get_safe_pg_connection() agora têm fallback: se audit_password não
    estiver presente, usam user/password principais do config.yaml.
  - execute_firebird_sql() e execute_postgres_sql() agora incluem str(e) na mensagem de erro retornada,
    tornando o erro visível ao agente e ao usuário.

  FIX 2 — lib/cli.py — do_agent():
  - Removida a construção de MigrationAIAgent antes da thread async.
  - Implementado padrão produtor/consumidor com duas filas (msg_queue, resp_queue).
  - A thread _agent_thread() executa asyncio.run(_async_chat_loop()) que constrói o agente DENTRO
    do event loop correto — McpToolset captura o loop desta thread e o cleanup ocorre no mesmo loop.
  - O loop de chat do Maestro envia mensagens via msg_queue e aguarda resposta via resp_queue,
    sem conflito de event loop com prompt_toolkit.

  FIX 3 — lib/ai/agent.py:
  - execute_task() agora inclui type(e).__name__ E str(e) na mensagem de exceção.

verification: Confirmado pelo usuário em ambiente real — /agent conecta corretamente aos bancos e erros reais são exibidos (não mais a mensagem genérica).
files_changed:
  - mcps/db_migration_server.py
  - lib/cli.py
  - lib/ai/agent.py
