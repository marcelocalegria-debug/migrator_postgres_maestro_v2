-- ===========================================================
-- REABILITAR CONSTRAINTS/ÍNDICES: public.documento_operacao
-- Gerado automaticamente pelo migrator
-- ===========================================================

BEGIN;

-- Restaurar configurações
SET synchronous_commit = on;
SET jit = on;

-- Atualizar estatísticas e reindexar
ANALYZE "public"."documento_operacao";
REINDEX TABLE "public"."documento_operacao";

COMMIT;

-- Total: 0 objetos recriados