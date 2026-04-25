-- ===========================================================
-- DESABILITAR CONSTRAINTS/ÍNDICES: public.documento_operacao
-- Gerado automaticamente pelo migrator
-- ===========================================================

BEGIN;

ALTER TABLE "public"."documento_operacao" DROP CONSTRAINT IF EXISTS "r_grupo_doc__doc_operacao";
ALTER TABLE "public"."documento_operacao" DROP CONSTRAINT IF EXISTS "r_operacao__doc_operacao";
ALTER TABLE "public"."documento_operacao" DROP CONSTRAINT IF EXISTS "r_usuario_parec__doc_operacao";
ALTER TABLE "public"."documento_operacao" DROP CONSTRAINT IF EXISTS "r_usuario_receb__doc_operacao";
ALTER TABLE "public"."documento_operacao" DROP CONSTRAINT IF EXISTS "xpkdocumento_operacao";
DROP INDEX IF EXISTS "public"."documento_operacaox01";
DROP INDEX IF EXISTS "public"."r_doc_grupo__doc_operacao";
DROP INDEX IF EXISTS "public"."r_grupo_doc__doc_operacao";
DROP INDEX IF EXISTS "public"."r_operacao__doc_operacao";
DROP INDEX IF EXISTS "public"."r_pessoa_pret__doc_operacao";
DROP INDEX IF EXISTS "public"."r_sistarq__doc_operacao";
DROP INDEX IF EXISTS "public"."r_usuario_parec__doc_operacao";
DROP INDEX IF EXISTS "public"."r_usuario_receb__doc_operacao";
DROP INDEX IF EXISTS "public"."xif374documento_operacao";
DROP INDEX IF EXISTS "public"."xif376documento_operacao";
DROP INDEX IF EXISTS "public"."xif377documento_operacao";
DROP INDEX IF EXISTS "public"."xif378documento_operacao";
DROP INDEX IF EXISTS "public"."xif379documento_operacao";
DROP INDEX IF EXISTS "public"."xif380documento_operacao";
DROP INDEX IF EXISTS "public"."xif381documento_operacao";

-- Otimizações de sessão para carga
SET synchronous_commit = off;
SET jit = off;

COMMIT;

-- Total: 20 objetos