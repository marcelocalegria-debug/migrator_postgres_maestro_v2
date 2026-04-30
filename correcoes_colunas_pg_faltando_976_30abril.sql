-- Correções de colunas geradas por compara_estrutura_fb2pg.py
-- Data: 2026-04-30 09:42:01

-- [TABELA: cadimv] Coluna 'co_situacao_garantia' faltando no PostgreSQL
ALTER TABLE public.cadimv ADD COLUMN co_situacao_garantia smallint;
-- [TABELA: cadimv] Coluna 'codarea' faltando no PostgreSQL
ALTER TABLE public.cadimv ADD COLUMN codarea integer;
-- [TABELA: cadimv] Coluna 'nu_pavimentos' faltando no PostgreSQL
ALTER TABLE public.cadimv ADD COLUMN nu_pavimentos smallint;
-- [TABELA: cadmut] Coluna 'cad6_co_situacao_garantia' faltando no PostgreSQL
ALTER TABLE public.cadmut ADD COLUMN cad6_co_situacao_garantia smallint;
-- [TABELA: cadmut] Coluna 'cad_dt3av' faltando no PostgreSQL
ALTER TABLE public.cadmut ADD COLUMN cad_dt3av timestamp without time zone;
-- [TABELA: cadmut] Coluna 'cad_dt3avbk' faltando no PostgreSQL
ALTER TABLE public.cadmut ADD COLUMN cad_dt3avbk timestamp without time zone;
-- [TABELA: cadmut] Coluna 'cad_dt4av' faltando no PostgreSQL
ALTER TABLE public.cadmut ADD COLUMN cad_dt4av timestamp without time zone;
-- [TABELA: cadmut] Coluna 'cad_dt4avbk' faltando no PostgreSQL
ALTER TABLE public.cadmut ADD COLUMN cad_dt4avbk timestamp without time zone;
-- [TABELA: cadmut] Coluna 'financ3_txjurosminima' faltando no PostgreSQL
ALTER TABLE public.cadmut ADD COLUMN financ3_txjurosminima double precision;
-- [TABELA: carteira] Coluna 'ndias3aviso' faltando no PostgreSQL
ALTER TABLE public.carteira ADD COLUMN ndias3aviso smallint;
-- [TABELA: carteira] Coluna 'ndias4aviso' faltando no PostgreSQL
ALTER TABLE public.carteira ADD COLUMN ndias4aviso smallint;
-- [TABELA: carteira] Coluna 'sitcobj3aviso' faltando no PostgreSQL
ALTER TABLE public.carteira ADD COLUMN sitcobj3aviso smallint;
-- [TABELA: carteira] Coluna 'sitcobj4aviso' faltando no PostgreSQL
ALTER TABLE public.carteira ADD COLUMN sitcobj4aviso smallint;
-- [TABELA: classificacao_renda_passivo] Coluna 'in_prog_classe_media' faltando no PostgreSQL
ALTER TABLE public.classificacao_renda_passivo ADD COLUMN in_prog_classe_media character(1);
-- [TABELA: documento_grupo_documento] Coluna 'te_expressao_exibicao' faltando no PostgreSQL
ALTER TABLE public.documento_grupo_documento ADD COLUMN te_expressao_exibicao bytea;
-- [TABELA: grupo_tipo_operacao] Coluna 'in_imovel_novo_usado' faltando no PostgreSQL
ALTER TABLE public.grupo_tipo_operacao ADD COLUMN in_imovel_novo_usado character(1);
-- [TABELA: imovel_operacao] Coluna 'co_condicao_imovel' faltando no PostgreSQL
ALTER TABLE public.imovel_operacao ADD COLUMN co_condicao_imovel integer;
-- [TABELA: imovel_operacao] Coluna 'co_situacao_garantia' faltando no PostgreSQL
ALTER TABLE public.imovel_operacao ADD COLUMN co_situacao_garantia smallint;
-- [TABELA: imv_operacao_empresario] Coluna 'co_situacao_garantia' faltando no PostgreSQL
ALTER TABLE public.imv_operacao_empresario ADD COLUMN co_situacao_garantia smallint;
-- [TABELA: interveniente_quitante] Coluna 'nu_cnpj' faltando no PostgreSQL
ALTER TABLE public.interveniente_quitante ADD COLUMN nu_cnpj character varying(14);
-- [TABELA: nrps] Coluna 'data_ent_est3' faltando no PostgreSQL
ALTER TABLE public.nrps ADD COLUMN data_ent_est3 timestamp without time zone;
-- [TABELA: nrps] Coluna 'valor_bruto_est3' faltando no PostgreSQL
ALTER TABLE public.nrps ADD COLUMN valor_bruto_est3 double precision;
-- [TABELA: nrps_dia] Coluna 'data_ent_est3' faltando no PostgreSQL
ALTER TABLE public.nrps_dia ADD COLUMN data_ent_est3 timestamp without time zone;
-- [TABELA: nrps_dia] Coluna 'valor_bruto_est3' faltando no PostgreSQL
ALTER TABLE public.nrps_dia ADD COLUMN valor_bruto_est3 double precision;
-- [TABELA: rpa] Coluna 'temp12_va_subsidio' faltando no PostgreSQL
ALTER TABLE public.rpa ADD COLUMN temp12_va_subsidio double precision;
-- [TABELA: titulos] Coluna 'va_saldopar' faltando no PostgreSQL
ALTER TABLE public.titulos ADD COLUMN va_saldopar double precision;
