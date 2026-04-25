-- ===========================================================
-- DESABILITAR CONSTRAINTS/ÍNDICES: public.log_eventos
-- Gerado automaticamente pelo migrator
-- ===========================================================

BEGIN;

DROP INDEX IF EXISTS "public"."idx_log_eventos_co_aplic_co_detalhe_dt_data_detalhe";
DROP INDEX IF EXISTS "public"."idx_log_eventos_co_aplic_dt_data_hora";
DROP INDEX IF EXISTS "public"."idx_log_eventos_co_aplic_dt_data_hora_no_usuario";
DROP INDEX IF EXISTS "public"."idx_log_eventos_dt_data_hora";
DROP INDEX IF EXISTS "public"."idx_log_eventos_dt_data_hora_no_base";
DROP INDEX IF EXISTS "public"."idx_log_eventos_dt_data_hora_no_tabela";
DROP INDEX IF EXISTS "public"."idx_log_eventos_dt_data_hora_nu_contrato";
DROP INDEX IF EXISTS "public"."idx_log_eventos_dt_data_hora_nu_pretendente";
DROP INDEX IF EXISTS "public"."idx_log_eventos_nu_contrato";
DROP INDEX IF EXISTS "public"."xif_log_eventos_contrato";
DROP INDEX IF EXISTS "public"."xiflog_eventos_aplic";
DROP INDEX IF EXISTS "public"."xiflog_eventos_basedata";
DROP INDEX IF EXISTS "public"."xiflog_eventos_datahora";
DROP INDEX IF EXISTS "public"."xiflog_eventos_detalhe";
DROP INDEX IF EXISTS "public"."xiflog_eventos_ixd";
DROP INDEX IF EXISTS "public"."xiflog_eventos_ixddb";
DROP INDEX IF EXISTS "public"."xiflog_eventos_pretdata";
DROP INDEX IF EXISTS "public"."xiflog_eventos_tabdata";
DROP INDEX IF EXISTS "public"."xiflog_eventos_usuario";

-- Otimizações de sessão para carga
SET synchronous_commit = off;
SET jit = off;

COMMIT;

-- Total: 19 objetos