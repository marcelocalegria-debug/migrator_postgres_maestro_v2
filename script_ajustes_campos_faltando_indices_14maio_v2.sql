\c  c6_producao

-- ============================================================
-- Schema Correction DDL — gerado em 2026-05-14 21:44:15
-- FK-RULES: 0 | FK add: 0 | FK drop: 0
-- IDX add: 2 | IDX drop: 3
-- Tabelas FB-only (CREATE TABLE): 0
-- Itens manuais (comentados): 3
-- ============================================================


-- [IDX só no FB] status_documento_operacao: criando status_documento_operacaoidx
CREATE INDEX IF NOT EXISTS "status_documento_operacaoidx" ON "status_documento_operacao" ("nu_documento", "nu_sequencial");

-- [IDX só no FB] tab_cartorio: criando tab_cartoriox04
CREATE INDEX IF NOT EXISTS "tab_cartoriox04" ON "tab_cartorio" ("nu_municipio");

-- ==== ITENS PARA REVISÃO MANUAL ====

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


-- ajustes gerado por IA
-- Tipo Firebird 7 (SMALLINT sem subtype) → smallint
ALTER TABLE cadimv ADD COLUMN co_situacao_garantia smallint;
ALTER TABLE cadmut ADD COLUMN cad6_co_situacao_garantia smallint;
ALTER TABLE imovel_operacao ADD COLUMN co_situacao_garantia smallint;
ALTER TABLE imv_operacao_empresario ADD COLUMN co_situacao_garantia smallint;
