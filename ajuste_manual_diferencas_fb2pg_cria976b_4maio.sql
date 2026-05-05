-- ============================================================
-- Schema Correction DDL — gerado em 2026-05-04 14:20:26
-- Migração: MIGRACAO_0001
-- FK-RULES: 35 | FK add: 0 | FK drop: 0
-- IDX add: 2 | IDX drop: 3
-- Tabelas FB-only (CREATE TABLE): 0
-- Itens manuais (comentados): 3
-- ============================================================

-- ==== CORREÇÕES AUTOMÁTICAS ====

-- [FK-RULES] acao_tarefa_padrao.r_acao__acao_tarefa_padrao: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "acao_tarefa_padrao" DROP CONSTRAINT IF EXISTS "r_acao__acao_tarefa_padrao";
ALTER TABLE "acao_tarefa_padrao" ADD CONSTRAINT "r_acao__acao_tarefa_padrao"
  FOREIGN KEY ("nome") REFERENCES "acao" ("nome")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] aprovacao_projeto.r_aprovacao_projeto_projeto: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "aprovacao_projeto" DROP CONSTRAINT IF EXISTS "r_aprovacao_projeto_projeto";
ALTER TABLE "aprovacao_projeto" ADD CONSTRAINT "r_aprovacao_projeto_projeto"
  FOREIGN KEY ("nu_projeto") REFERENCES "projeto" ("nu_projeto")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] aquisicao_imovel.r_imovel_terreno_aquisicao_imo: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "aquisicao_imovel" DROP CONSTRAINT IF EXISTS "r_imovel_terreno_aquisicao_imo";
ALTER TABLE "aquisicao_imovel" ADD CONSTRAINT "r_imovel_terreno_aquisicao_imo"
  FOREIGN KEY ("nu_imovel") REFERENCES "imovel_terreno" ("nu_imovel")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] configuracao_originacao.r_fase__config_originacao: ON DELETE NO ACTION→RESTRICT, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "configuracao_originacao" DROP CONSTRAINT IF EXISTS "r_fase__config_originacao";
ALTER TABLE "configuracao_originacao" ADD CONSTRAINT "r_fase__config_originacao"
  FOREIGN KEY ("nu_fase_cancelamento") REFERENCES "fase_operacao" ("nu_fase_operacao")
  ON DELETE RESTRICT ON UPDATE CASCADE;

-- [FK-RULES] confrontacao_rua.r_rua_projeto_confrontacao_rua: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "confrontacao_rua" DROP CONSTRAINT IF EXISTS "r_rua_projeto_confrontacao_rua";
ALTER TABLE "confrontacao_rua" ADD CONSTRAINT "r_rua_projeto_confrontacao_rua"
  FOREIGN KEY ("nu_projeto", "nu_rua") REFERENCES "rua_projeto" ("nu_projeto", "nu_rua")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] construcao_projeto.r_projeto_construcao_projeto: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "construcao_projeto" DROP CONSTRAINT IF EXISTS "r_projeto_construcao_projeto";
ALTER TABLE "construcao_projeto" ADD CONSTRAINT "r_projeto_construcao_projeto"
  FOREIGN KEY ("nu_projeto") REFERENCES "projeto" ("nu_projeto")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] contrato_obra.r_fornecedor_contrato_obra: ON DELETE SET NULL→SET NULL, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "contrato_obra" DROP CONSTRAINT IF EXISTS "r_fornecedor_contrato_obra";
ALTER TABLE "contrato_obra" ADD CONSTRAINT "r_fornecedor_contrato_obra"
  FOREIGN KEY ("nu_fornecedor") REFERENCES "fornecedor" ("nu_fornecedor")
  ON DELETE SET NULL ON UPDATE CASCADE;

-- [FK-RULES] contrato_obra.r_fonte_recurso_contrato_obra: ON DELETE NO ACTION→RESTRICT, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "contrato_obra" DROP CONSTRAINT IF EXISTS "r_fonte_recurso_contrato_obra";
ALTER TABLE "contrato_obra" ADD CONSTRAINT "r_fonte_recurso_contrato_obra"
  FOREIGN KEY ("co_recurso") REFERENCES "fonte_recurso" ("nu_recurso")
  ON DELETE RESTRICT ON UPDATE CASCADE;

-- [FK-RULES] contrato_obra.r_obra_contrato_obra: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "contrato_obra" DROP CONSTRAINT IF EXISTS "r_obra_contrato_obra";
ALTER TABLE "contrato_obra" ADD CONSTRAINT "r_obra_contrato_obra"
  FOREIGN KEY ("nu_obra") REFERENCES "obra" ("nu_obra")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] documento_cip.r_usuario_documento_cip: ON DELETE NO ACTION→NO ACTION, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "documento_cip" DROP CONSTRAINT IF EXISTS "r_usuario_documento_cip";
ALTER TABLE "documento_cip" ADD CONSTRAINT "r_usuario_documento_cip"
  FOREIGN KEY ("co_usuario_entregou") REFERENCES "usuario" ("usuario")
  ON DELETE NO ACTION ON UPDATE CASCADE;

-- [FK-RULES] documento_cip.r_projeto_documento_cip: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "documento_cip" DROP CONSTRAINT IF EXISTS "r_projeto_documento_cip";
ALTER TABLE "documento_cip" ADD CONSTRAINT "r_projeto_documento_cip"
  FOREIGN KEY ("nu_projeto") REFERENCES "projeto" ("nu_projeto")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] fatura.r_contrato_obra_fatura: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "fatura" DROP CONSTRAINT IF EXISTS "r_contrato_obra_fatura";
ALTER TABLE "fatura" ADD CONSTRAINT "r_contrato_obra_fatura"
  FOREIGN KEY ("nu_contrato") REFERENCES "contrato_obra" ("nu_contrato")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] fonte_recurso_pagamento.r_imovel_terreno_fonte_recurso: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "fonte_recurso_pagamento" DROP CONSTRAINT IF EXISTS "r_imovel_terreno_fonte_recurso";
ALTER TABLE "fonte_recurso_pagamento" ADD CONSTRAINT "r_imovel_terreno_fonte_recurso"
  FOREIGN KEY ("nu_imovel") REFERENCES "imovel_terreno" ("nu_imovel")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] gravame.r_imovel_terreno_gravame: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "gravame" DROP CONSTRAINT IF EXISTS "r_imovel_terreno_gravame";
ALTER TABLE "gravame" ADD CONSTRAINT "r_imovel_terreno_gravame"
  FOREIGN KEY ("nu_imovel") REFERENCES "imovel_terreno" ("nu_imovel")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] historico_imovel.r_imovel_terreno_historico_imo: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "historico_imovel" DROP CONSTRAINT IF EXISTS "r_imovel_terreno_historico_imo";
ALTER TABLE "historico_imovel" ADD CONSTRAINT "r_imovel_terreno_historico_imo"
  FOREIGN KEY ("nu_imovel") REFERENCES "imovel_terreno" ("nu_imovel")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] historico_operacao.r_fase__historico_operacao: ON DELETE NO ACTION→RESTRICT, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "historico_operacao" DROP CONSTRAINT IF EXISTS "r_fase__historico_operacao";
ALTER TABLE "historico_operacao" ADD CONSTRAINT "r_fase__historico_operacao"
  FOREIGN KEY ("nu_fase_operacao") REFERENCES "fase_operacao" ("nu_fase_operacao")
  ON DELETE RESTRICT ON UPDATE CASCADE;

-- [FK-RULES] historico_projeto.r_projeto_historico_projeto: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "historico_projeto" DROP CONSTRAINT IF EXISTS "r_projeto_historico_projeto";
ALTER TABLE "historico_projeto" ADD CONSTRAINT "r_projeto_historico_projeto"
  FOREIGN KEY ("nu_projeto") REFERENCES "projeto" ("nu_projeto")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] imagem_documento_rgi.r_registro_geral_imovel_imagem: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "imagem_documento_rgi" DROP CONSTRAINT IF EXISTS "r_registro_geral_imovel_imagem";
ALTER TABLE "imagem_documento_rgi" ADD CONSTRAINT "r_registro_geral_imovel_imagem"
  FOREIGN KEY ("nu_imovel") REFERENCES "registro_geral_imovel" ("nu_imovel")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] imovel_bloqueado.r_imovel_terreno_imovel_bloque: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "imovel_bloqueado" DROP CONSTRAINT IF EXISTS "r_imovel_terreno_imovel_bloque";
ALTER TABLE "imovel_bloqueado" ADD CONSTRAINT "r_imovel_terreno_imovel_bloque"
  FOREIGN KEY ("nu_imovel") REFERENCES "imovel_terreno" ("nu_imovel")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] inclusao_predial.r_lote_proj_inc_pred: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "inclusao_predial" DROP CONSTRAINT IF EXISTS "r_lote_proj_inc_pred";
ALTER TABLE "inclusao_predial" ADD CONSTRAINT "r_lote_proj_inc_pred"
  FOREIGN KEY ("nu_projeto", "nu_quadra", "nu_lote") REFERENCES "lote_projeto" ("nu_projeto", "nu_quadra", "nu_lote")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] item_fatura.r_fatura_item_fatura: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "item_fatura" DROP CONSTRAINT IF EXISTS "r_fatura_item_fatura";
ALTER TABLE "item_fatura" ADD CONSTRAINT "r_fatura_item_fatura"
  FOREIGN KEY ("nu_fatura") REFERENCES "fatura" ("nu_fatura")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] lote_projeto.r_projeto_lote_projeto: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "lote_projeto" DROP CONSTRAINT IF EXISTS "r_projeto_lote_projeto";
ALTER TABLE "lote_projeto" ADD CONSTRAINT "r_projeto_lote_projeto"
  FOREIGN KEY ("nu_projeto") REFERENCES "projeto" ("nu_projeto")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] obra.r_subprograma_obra: ON DELETE SET NULL→SET NULL, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "obra" DROP CONSTRAINT IF EXISTS "r_subprograma_obra";
ALTER TABLE "obra" ADD CONSTRAINT "r_subprograma_obra"
  FOREIGN KEY ("nu_programa", "co_subprograma") REFERENCES "subprograma" ("nu_programa", "co_subprograma")
  ON DELETE SET NULL ON UPDATE CASCADE;

-- [FK-RULES] obra.r_projeto_obra: ON DELETE SET NULL→SET NULL, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "obra" DROP CONSTRAINT IF EXISTS "r_projeto_obra";
ALTER TABLE "obra" ADD CONSTRAINT "r_projeto_obra"
  FOREIGN KEY ("nu_projeto") REFERENCES "projeto" ("nu_projeto")
  ON DELETE SET NULL ON UPDATE CASCADE;

-- [FK-RULES] operacao_credito.r_fase_operacao__operacao: ON DELETE NO ACTION→RESTRICT, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "operacao_credito" DROP CONSTRAINT IF EXISTS "r_fase_operacao__operacao";
ALTER TABLE "operacao_credito" ADD CONSTRAINT "r_fase_operacao__operacao"
  FOREIGN KEY ("nu_fase_atual") REFERENCES "fase_operacao" ("nu_fase_operacao")
  ON DELETE RESTRICT ON UPDATE CASCADE;

-- [FK-RULES] pagamento_fatura.r_fatura_pagamento_fatura: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "pagamento_fatura" DROP CONSTRAINT IF EXISTS "r_fatura_pagamento_fatura";
ALTER TABLE "pagamento_fatura" ADD CONSTRAINT "r_fatura_pagamento_fatura"
  FOREIGN KEY ("nu_fatura") REFERENCES "fatura" ("nu_fatura")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] pendencia_aprovacao.r_projeto_pendencia_aprovacao: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "pendencia_aprovacao" DROP CONSTRAINT IF EXISTS "r_projeto_pendencia_aprovacao";
ALTER TABLE "pendencia_aprovacao" ADD CONSTRAINT "r_projeto_pendencia_aprovacao"
  FOREIGN KEY ("nu_projeto") REFERENCES "projeto" ("nu_projeto")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] projeto.r_imovel_terreno_projeto: ON DELETE NO ACTION→RESTRICT, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "projeto" DROP CONSTRAINT IF EXISTS "r_imovel_terreno_projeto";
ALTER TABLE "projeto" ADD CONSTRAINT "r_imovel_terreno_projeto"
  FOREIGN KEY ("nu_imovel") REFERENCES "imovel_terreno" ("nu_imovel")
  ON DELETE RESTRICT ON UPDATE CASCADE;

-- [FK-RULES] projeto_bloqueado.r_projeto_projeto_bloqueado: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "projeto_bloqueado" DROP CONSTRAINT IF EXISTS "r_projeto_projeto_bloqueado";
ALTER TABLE "projeto_bloqueado" ADD CONSTRAINT "r_projeto_projeto_bloqueado"
  FOREIGN KEY ("nu_projeto") REFERENCES "projeto" ("nu_projeto")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] registro_geral_imovel.r_imovel_terreno_registro_gera: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "registro_geral_imovel" DROP CONSTRAINT IF EXISTS "r_imovel_terreno_registro_gera";
ALTER TABLE "registro_geral_imovel" ADD CONSTRAINT "r_imovel_terreno_registro_gera"
  FOREIGN KEY ("nu_imovel") REFERENCES "imovel_terreno" ("nu_imovel")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] requisitos_acao_tarefa_padrao.r_req_acao_acao_tar_padrao: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "requisitos_acao_tarefa_padrao" DROP CONSTRAINT IF EXISTS "r_req_acao_acao_tar_padrao";
ALTER TABLE "requisitos_acao_tarefa_padrao" ADD CONSTRAINT "r_req_acao_acao_tar_padrao"
  FOREIGN KEY ("nome", "co_tarefa_padrao") REFERENCES "acao_tarefa_padrao" ("nome", "co_tarefa_padrao")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [FK-RULES] rua_projeto.r_projeto_rua_projeto: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "rua_projeto" DROP CONSTRAINT IF EXISTS "r_projeto_rua_projeto";
ALTER TABLE "rua_projeto" ADD CONSTRAINT "r_projeto_rua_projeto"
  FOREIGN KEY ("nu_projeto") REFERENCES "projeto" ("nu_projeto")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [IDX só no FB] status_documento_operacao: criando status_documento_operacaoidx
CREATE INDEX IF NOT EXISTS "status_documento_operacaoidx" ON "status_documento_operacao" ("nu_documento", "nu_sequencial");

-- [FK-RULES] subprograma.r_programa_subprograma: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "subprograma" DROP CONSTRAINT IF EXISTS "r_programa_subprograma";
ALTER TABLE "subprograma" ADD CONSTRAINT "r_programa_subprograma"
  FOREIGN KEY ("nu_programa") REFERENCES "programa" ("nu_programa")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- [IDX só no FB] tab_cartorio: criando tab_cartoriox04
CREATE INDEX IF NOT EXISTS "tab_cartoriox04" ON "tab_cartorio" ("nu_municipio");

-- [FK-RULES] tributo_imovel.r_tipo_tributo_tributo_imovel: ON DELETE NO ACTION→RESTRICT, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "tributo_imovel" DROP CONSTRAINT IF EXISTS "r_tipo_tributo_tributo_imovel";
ALTER TABLE "tributo_imovel" ADD CONSTRAINT "r_tipo_tributo_tributo_imovel"
  FOREIGN KEY ("co_tributo") REFERENCES "tipo_tributo" ("co_tributo")
  ON DELETE RESTRICT ON UPDATE CASCADE;

-- [FK-RULES] tributo_imovel.r_imv_terreno_trib_imv: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "tributo_imovel" DROP CONSTRAINT IF EXISTS "r_imv_terreno_trib_imv";
ALTER TABLE "tributo_imovel" ADD CONSTRAINT "r_imv_terreno_trib_imv"
  FOREIGN KEY ("nu_imovel") REFERENCES "imovel_terreno" ("nu_imovel")
  ON DELETE CASCADE ON UPDATE CASCADE;

-- ==== ITENS PARA REVISÃO MANUAL ====

-- MANUAL: tabela 'arq_co_aplic' só existe no Firebird (DDL não encontrado em schema.sql — criar manualmente)
-- MANUAL: tabela 'ordem_pretendente' só existe no Firebird (DDL não encontrado em schema.sql — criar manualmente)
-- MANUAL: tabela 'tmp_exc_log' só existe no Firebird (DDL não encontrado em schema.sql — criar manualmente)



-- ==== ITENS PARA REVISÃO MANUAL ====

-- MANUAL: tabela 'arq_co_aplic' só existe no Firebird (ver schema.sql para DDL CREATE TABLE)
-- MANUAL: tabela 'ordem_pretendente' só existe no Firebird (ver schema.sql para DDL CREATE TABLE)
-- MANUAL: tabela 'tmp_exc_log' só existe no Firebird (ver schema.sql para DDL CREATE TABLE)

 -- tabela 'arq_co_aplic' só existe no Firebird
 -- tabela 'ordem_pretendente' só existe no Firebird
 -- tabela 'tmp_exc_log' só existe no Firebird


--
-- Name: arq_co_aplic; Type: TABLE; Schema: public; Owner: c6_producao_user
--

CREATE TABLE public.arq_co_aplic (
    co_aplic integer NOT NULL
);


ALTER TABLE public.arq_co_aplic OWNER TO c6_producao_user;

--
-- Name: arqag31; Type: TABLE; Schema: public; Owner: c6_producao_user
--

--
-- Name: arq_co_aplic arq_co_aplic_pkey; Type: CONSTRAINT; Schema: public; Owner: c6_producao_user
--

ALTER TABLE ONLY public.arq_co_aplic
    ADD CONSTRAINT arq_co_aplic_pkey PRIMARY KEY (co_aplic);



 --
-- Name: ordem_pretendente; Type: TABLE; Schema: public; Owner: c6_producao_user
--

CREATE TABLE public.ordem_pretendente (
    nu_pretendente character varying(9) DEFAULT ''::character varying NOT NULL,
    nu_ordem integer DEFAULT 0
);


ALTER TABLE public.ordem_pretendente OWNER TO c6_producao_user;

--
-- Name: ordem_pretendente ordem_pretendente_pkey; Type: CONSTRAINT; Schema: public; Owner: c6_producao_user
--

ALTER TABLE ONLY public.ordem_pretendente
    ADD CONSTRAINT ordem_pretendente_pkey PRIMARY KEY (nu_pretendente);


--
-- Name: tmp_exc_log; Type: TABLE; Schema: public; Owner: c6_producao_user
--

CREATE TABLE public.tmp_exc_log (
    co_log integer NOT NULL
);


ALTER TABLE public.tmp_exc_log OWNER TO c6_producao_user;

--
-- Name: token; Type: TABLE; Schema: public; Owner: c6_producao_user
--
--
-- Name: tmp_exc_log tmp_exc_log_pkey; Type: CONSTRAINT; Schema: public; Owner: c6_producao_user
--

ALTER TABLE ONLY public.tmp_exc_log
    ADD CONSTRAINT tmp_exc_log_pkey PRIMARY KEY (co_log);

ALTER TABLE IMOVEL_OPERACAO ADD COLUMN IF NOT EXISTS CO_SITUACAO_GARANTIA SMALLINT DEFAULT 0;
ALTER TABLE IMV_OPERACAO_EMPRESARIO ADD COLUMN IF NOT EXISTS CO_SITUACAO_GARANTIA SMALLINT DEFAULT 0;
ALTER TABLE CADIMV ADD COLUMN IF NOT EXISTS CO_SITUACAO_GARANTIA SMALLINT DEFAULT 0;
ALTER TABLE CADMUT ADD COLUMN IF NOT EXISTS CAD6_CO_SITUACAO_GARANTIA SMALLINT DEFAULT 0;


-- [FK-RULES] acao_tarefa_padrao.r_acao__acao_tarefa_padrao: ON DELETE CASCADE→CASCADE, ON UPDATE NO ACTION→CASCADE
ALTER TABLE "acao_tarefa_padrao" DROP CONSTRAINT IF EXISTS "r_acao__acao_tarefa_padrao";
ALTER TABLE "acao_tarefa_padrao" ADD CONSTRAINT "r_acao__acao_tarefa_padrao"
  FOREIGN KEY ("nome") REFERENCES "acao" ("nome")
  ON DELETE CASCADE ON UPDATE CASCADE;
