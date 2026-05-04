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
