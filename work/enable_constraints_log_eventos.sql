-- ===========================================================
-- REABILITAR CONSTRAINTS/ÍNDICES: public.log_eventos
-- Gerado automaticamente pelo migrator
-- ===========================================================

BEGIN;

-- Indexes explícitos
CREATE INDEX idx_log_eventos_co_aplic_co_detalhe_dt_data_detalhe ON public.log_eventos USING btree (co_aplic, co_detalhe, dt_data_detalhe);
CREATE INDEX idx_log_eventos_co_aplic_dt_data_hora ON public.log_eventos USING btree (co_aplic, dt_data_hora);
CREATE INDEX idx_log_eventos_co_aplic_dt_data_hora_no_usuario ON public.log_eventos USING btree (co_aplic, dt_data_hora, no_usuario);
CREATE INDEX idx_log_eventos_dt_data_hora ON public.log_eventos USING btree (dt_data_hora);
CREATE INDEX idx_log_eventos_dt_data_hora_no_base ON public.log_eventos USING btree (dt_data_hora, no_base);
CREATE INDEX idx_log_eventos_dt_data_hora_no_tabela ON public.log_eventos USING btree (dt_data_hora, no_tabela);
CREATE INDEX idx_log_eventos_dt_data_hora_nu_contrato ON public.log_eventos USING btree (dt_data_hora, nu_contrato);
CREATE INDEX idx_log_eventos_dt_data_hora_nu_pretendente ON public.log_eventos USING btree (dt_data_hora, nu_pretendente);
CREATE INDEX idx_log_eventos_nu_contrato ON public.log_eventos USING btree (nu_contrato);
CREATE INDEX xif_log_eventos_contrato ON public.log_eventos USING btree (nu_contrato);
CREATE INDEX xiflog_eventos_aplic ON public.log_eventos USING btree (co_aplic);
CREATE INDEX xiflog_eventos_basedata ON public.log_eventos USING btree (no_base, dt_data_hora);
CREATE INDEX xiflog_eventos_datahora ON public.log_eventos USING btree (dt_data_hora DESC);
CREATE INDEX xiflog_eventos_detalhe ON public.log_eventos USING btree (co_aplic, co_detalhe, dt_data_detalhe);
CREATE INDEX xiflog_eventos_ixd ON public.log_eventos USING btree (co_aplic, dt_data_hora);
CREATE INDEX xiflog_eventos_ixddb ON public.log_eventos USING btree (nu_contrato, dt_data_hora);
CREATE INDEX xiflog_eventos_pretdata ON public.log_eventos USING btree (nu_pretendente, dt_data_hora);
CREATE INDEX xiflog_eventos_tabdata ON public.log_eventos USING btree (no_tabela, dt_data_hora);
CREATE INDEX xiflog_eventos_usuario ON public.log_eventos USING btree (no_usuario, co_aplic, dt_data_hora);

-- Restaurar configurações
SET synchronous_commit = on;
SET jit = on;

-- Atualizar estatísticas e reindexar
ANALYZE "public"."log_eventos";
REINDEX TABLE "public"."log_eventos";

COMMIT;

-- Total: 19 objetos recriados