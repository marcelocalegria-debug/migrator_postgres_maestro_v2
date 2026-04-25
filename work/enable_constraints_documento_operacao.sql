-- ===========================================================
-- REABILITAR CONSTRAINTS/ÍNDICES: public.documento_operacao
-- Gerado automaticamente pelo migrator
-- ===========================================================

BEGIN;

-- Indexes explícitos
CREATE INDEX documento_operacaox01 ON public.documento_operacao USING btree (id_externo);
CREATE INDEX r_doc_grupo__doc_operacao ON public.documento_operacao USING btree (nu_grupo_documento, nu_documento_grupo_doc);
CREATE INDEX r_grupo_doc__doc_operacao ON public.documento_operacao USING btree (nu_grupo_documento);
CREATE INDEX r_operacao__doc_operacao ON public.documento_operacao USING btree (nu_operacao);
CREATE INDEX r_pessoa_pret__doc_operacao ON public.documento_operacao USING btree (nu_pessoa, nu_pretendente);
CREATE INDEX r_sistarq__doc_operacao ON public.documento_operacao USING btree (id);
CREATE INDEX r_usuario_parec__doc_operacao ON public.documento_operacao USING btree (co_usuario_parecer);
CREATE INDEX r_usuario_receb__doc_operacao ON public.documento_operacao USING btree (co_usuario_recebimento);
CREATE INDEX xif374documento_operacao ON public.documento_operacao USING btree (nu_operacao);
CREATE INDEX xif376documento_operacao ON public.documento_operacao USING btree (nu_grupo_documento, nu_documento_grupo_doc);
CREATE INDEX xif377documento_operacao ON public.documento_operacao USING btree (co_usuario_recebimento);
CREATE INDEX xif378documento_operacao ON public.documento_operacao USING btree (co_usuario_parecer);
CREATE INDEX xif379documento_operacao ON public.documento_operacao USING btree (nu_pretendente, nu_pessoa);
CREATE INDEX xif380documento_operacao ON public.documento_operacao USING btree (id);
CREATE INDEX xif381documento_operacao ON public.documento_operacao USING btree (nu_grupo_documento);

-- Primary Key
ALTER TABLE "public"."documento_operacao" ADD CONSTRAINT "xpkdocumento_operacao" PRIMARY KEY (nu_operacao, nu_documento);

-- Foreign Keys (próprias)
ALTER TABLE "public"."documento_operacao" ADD CONSTRAINT "r_grupo_doc__doc_operacao" FOREIGN KEY ("nu_grupo_documento") REFERENCES "public"."grupo_documento"("nu_grupo_documento") ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE "public"."documento_operacao" ADD CONSTRAINT "r_operacao__doc_operacao" FOREIGN KEY ("nu_operacao") REFERENCES "public"."operacao_credito"("nu_operacao") ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE "public"."documento_operacao" ADD CONSTRAINT "r_usuario_parec__doc_operacao" FOREIGN KEY ("co_usuario_parecer") REFERENCES "public"."usuario"("usuario") ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE "public"."documento_operacao" ADD CONSTRAINT "r_usuario_receb__doc_operacao" FOREIGN KEY ("co_usuario_recebimento") REFERENCES "public"."usuario"("usuario") ON UPDATE RESTRICT ON DELETE RESTRICT;

-- Restaurar configurações
SET synchronous_commit = on;
SET jit = on;

-- Atualizar estatísticas e reindexar
ANALYZE "public"."documento_operacao";
REINDEX TABLE "public"."documento_operacao";

COMMIT;

-- Total: 20 objetos recriados