# GSD Debug Knowledge Base

Resolved debug sessions. Used by `gsd-debugger` to surface known-pattern hypotheses at the start of new investigations.

---

## agent-mcp-connection-failure — /agent retorna erro genérico de conexão MCP; "original event loop is closed" no cleanup
- **Date:** 2026-04-30
- **Error patterns:** original event loop is closed, falha de conexão, audit_password, McpToolset, stdio_session, execute_task, RuntimeError, asyncio.run
- **Root cause:** (1) get_safe_fb/pg_connection() exigia audit_password ausente no config.yaml, e execute_fb/pg_sql() suprimia o erro real retornando apenas type(e).__name__ sem str(e) — agente via string genérica. (2) MigrationAIAgent (e McpToolset) era construído no thread principal antes de asyncio.run() em thread nova; quando asyncio.run() encerrava e fechava o loop, o McpToolset não conseguia fazer cleanup do stdio_session.
- **Fix:** (1) Fallback para user/password quando audit_password ausente; str(e) incluído nas mensagens de erro das tools MCP. (2) Agente construído dentro de _async_chat_loop() na thread dedicada usando padrão produtor/consumidor com msg_queue/resp_queue — McpToolset captura o loop correto e cleanup ocorre no mesmo loop.
- **Files changed:** mcps/db_migration_server.py, lib/cli.py, lib/ai/agent.py
---

