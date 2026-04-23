-- ===========================================================
-- REABILITAR CONSTRAINTS/ÍNDICES: public.log_eventos
-- Gerado automaticamente pelo migrator
-- ===========================================================

BEGIN;

-- Restaurar configurações
SET synchronous_commit = on;
SET jit = on;

-- Atualizar estatísticas e reindexar
ANALYZE "public"."log_eventos";
REINDEX TABLE "public"."log_eventos";

COMMIT;

-- Total: 0 objetos recriados