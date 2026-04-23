-- ==========================================================
-- fix_blob_to_text.sql
-- Corrige colunas BLOB SUB_TYPE 0 que contêm texto
-- Gerado por fix_blob_text_columns.py
-- ==========================================================

-- IMPORTANTE: Executar com superuser ou owner das tabelas.
-- Cada ALTER roda em sua própria transação (autocommit).
-- Se uma falhar, as demais continuam.

-- Total BLOB sub_type 0: 372
-- Converter para text: 362
-- Manter como bytea: 10

-- Colunas mantidas como bytea (genuinamente binárias):
--   controleversao.dado
--   controleversao.te_imagem_reduzida
--   email_a_enviar.te_imagem_reduzida
--   grupo_tipo_operacao.im_grupo_tipo_operacao
--   imagem_documento_rgi.im_pagina_rgi
--   scci_session.refresh_token
--   scci_session.token
--   segura.im_seguradora
--   segura.im_seguradora_mini
--   simulacao_originacao.im_enquadramento

-- abertura_endividamento_oper (1 colunas)
ALTER TABLE "public"."abertura_endividamento_oper" ALTER COLUMN "no_garantia" TYPE text USING convert_from("no_garantia", 'LATIN1');

-- acao_juridico (1 colunas)
ALTER TABLE "public"."acao_juridico" ALTER COLUMN "te_obs_processo" TYPE text USING convert_from("te_obs_processo", 'LATIN1');

-- acao_ocorrencia_sisat (1 colunas)
ALTER TABLE "public"."acao_ocorrencia_sisat" ALTER COLUMN "te_acao" TYPE text USING convert_from("te_acao", 'LATIN1');

-- agend_pretendente (1 colunas)
ALTER TABLE "public"."agend_pretendente" ALTER COLUMN "te_descricao" TYPE text USING convert_from("te_descricao", 'LATIN1');

-- analise_empreendimento (9 colunas)
ALTER TABLE "public"."analise_empreendimento" ALTER COLUMN "te_aspectos_cadastrais" TYPE text USING convert_from("te_aspectos_cadastrais", 'LATIN1');
ALTER TABLE "public"."analise_empreendimento" ALTER COLUMN "te_criterio_comercializacao" TYPE text USING convert_from("te_criterio_comercializacao", 'LATIN1');
ALTER TABLE "public"."analise_empreendimento" ALTER COLUMN "te_demanda_efetiva" TYPE text USING convert_from("te_demanda_efetiva", 'LATIN1');
ALTER TABLE "public"."analise_empreendimento" ALTER COLUMN "te_fontes_consulta_restricoes" TYPE text USING convert_from("te_fontes_consulta_restricoes", 'LATIN1');
ALTER TABLE "public"."analise_empreendimento" ALTER COLUMN "te_garantias" TYPE text USING convert_from("te_garantias", 'LATIN1');
ALTER TABLE "public"."analise_empreendimento" ALTER COLUMN "te_localizacao_empreendimento" TYPE text USING convert_from("te_localizacao_empreendimento", 'LATIN1');
ALTER TABLE "public"."analise_empreendimento" ALTER COLUMN "te_manifestacao_operador" TYPE text USING convert_from("te_manifestacao_operador", 'LATIN1');
ALTER TABLE "public"."analise_empreendimento" ALTER COLUMN "te_manifestacao_promotor" TYPE text USING convert_from("te_manifestacao_promotor", 'LATIN1');
ALTER TABLE "public"."analise_empreendimento" ALTER COLUMN "te_objetivo_empreendimento" TYPE text USING convert_from("te_objetivo_empreendimento", 'LATIN1');

-- andamento_fase_producao (1 colunas)
ALTER TABLE "public"."andamento_fase_producao" ALTER COLUMN "no_historico" TYPE text USING convert_from("no_historico", 'LATIN1');

-- andamento_lote (1 colunas)
ALTER TABLE "public"."andamento_lote" ALTER COLUMN "erro" TYPE text USING convert_from("erro", 'LATIN1');

-- andamento_relatorio (2 colunas)
ALTER TABLE "public"."andamento_relatorio" ALTER COLUMN "detalhes" TYPE text USING convert_from("detalhes", 'LATIN1');
ALTER TABLE "public"."andamento_relatorio" ALTER COLUMN "erro" TYPE text USING convert_from("erro", 'LATIN1');

-- andamento_spc_serasa (4 colunas)
ALTER TABLE "public"."andamento_spc_serasa" ALTER COLUMN "arquivoentrada" TYPE text USING convert_from("arquivoentrada", 'LATIN1');
ALTER TABLE "public"."andamento_spc_serasa" ALTER COLUMN "arquivosaida" TYPE text USING convert_from("arquivosaida", 'LATIN1');
ALTER TABLE "public"."andamento_spc_serasa" ALTER COLUMN "erro" TYPE text USING convert_from("erro", 'LATIN1');
ALTER TABLE "public"."andamento_spc_serasa" ALTER COLUMN "selecionados" TYPE text USING convert_from("selecionados", 'LATIN1');

-- assinatura_documento (1 colunas)
ALTER TABLE "public"."assinatura_documento" ALTER COLUMN "te_motivo_recusa" TYPE text USING convert_from("te_motivo_recusa", 'LATIN1');

-- atendimento (2 colunas)
ALTER TABLE "public"."atendimento" ALTER COLUMN "te_aprovacao_quitacao_ata" TYPE text USING convert_from("te_aprovacao_quitacao_ata", 'LATIN1');
ALTER TABLE "public"."atendimento" ALTER COLUMN "te_observacao_asc" TYPE text USING convert_from("te_observacao_asc", 'LATIN1');

-- atendimento_dfi (1 colunas)
ALTER TABLE "public"."atendimento_dfi" ALTER COLUMN "te_descricao_ocorrido" TYPE text USING convert_from("te_descricao_ocorrido", 'LATIN1');

-- atendimento_fase (1 colunas)
ALTER TABLE "public"."atendimento_fase" ALTER COLUMN "te_observacao_fase" TYPE text USING convert_from("te_observacao_fase", 'LATIN1');

-- atendimento_mip (1 colunas)
ALTER TABLE "public"."atendimento_mip" ALTER COLUMN "te_outros_documentos" TYPE text USING convert_from("te_outros_documentos", 'LATIN1');

-- cadimv_ib (3 colunas)
ALTER TABLE "public"."cadimv_ib" ALTER COLUMN "no_desc_livre_imv" TYPE text USING convert_from("no_desc_livre_imv", 'LATIN1');
ALTER TABLE "public"."cadimv_ib" ALTER COLUMN "te_restricoes_ao_imovel" TYPE text USING convert_from("te_restricoes_ao_imovel", 'LATIN1');
ALTER TABLE "public"."cadimv_ib" ALTER COLUMN "te_situacao_imovel" TYPE text USING convert_from("te_situacao_imovel", 'LATIN1');

-- calcoperfgts (1 colunas)
ALTER TABLE "public"."calcoperfgts" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- calculo_acordo (1 colunas)
ALTER TABLE "public"."calculo_acordo" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- calculo_diferencas (1 colunas)
ALTER TABLE "public"."calculo_diferencas" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- campos_laser (1 colunas)
ALTER TABLE "public"."campos_laser" ALTER COLUMN "extra" TYPE text USING convert_from("extra", 'LATIN1');

-- campos_rem (1 colunas)
ALTER TABLE "public"."campos_rem" ALTER COLUMN "extra" TYPE text USING convert_from("extra", 'LATIN1');

-- campos_rem_spc (1 colunas)
ALTER TABLE "public"."campos_rem_spc" ALTER COLUMN "extra" TYPE text USING convert_from("extra", 'LATIN1');

-- campo_expressao_sig (2 colunas)
ALTER TABLE "public"."campo_expressao_sig" ALTER COLUMN "no_descricao" TYPE text USING convert_from("no_descricao", 'LATIN1');
ALTER TABLE "public"."campo_expressao_sig" ALTER COLUMN "no_expressao" TYPE text USING convert_from("no_expressao", 'LATIN1');

-- campo_perda_analise_cred (2 colunas)
ALTER TABLE "public"."campo_perda_analise_cred" ALTER COLUMN "te_expressao_condicao" TYPE text USING convert_from("te_expressao_condicao", 'LATIN1');
ALTER TABLE "public"."campo_perda_analise_cred" ALTER COLUMN "te_mensagem" TYPE text USING convert_from("te_mensagem", 'LATIN1');

-- cessao_contrato (2 colunas)
ALTER TABLE "public"."cessao_contrato" ALTER COLUMN "te_dados_cessao" TYPE text USING convert_from("te_dados_cessao", 'LATIN1');
ALTER TABLE "public"."cessao_contrato" ALTER COLUMN "te_dados_recebimento" TYPE text USING convert_from("te_dados_recebimento", 'LATIN1');

-- cessao_implantada (1 colunas)
ALTER TABLE "public"."cessao_implantada" ALTER COLUMN "te_dados_parcelas" TYPE text USING convert_from("te_dados_parcelas", 'LATIN1');

-- codmovev (1 colunas)
ALTER TABLE "public"."codmovev" ALTER COLUMN "te_comentario" TYPE text USING convert_from("te_comentario", 'LATIN1');

-- comercializacao (1 colunas)
ALTER TABLE "public"."comercializacao" ALTER COLUMN "te_inf_adicionais" TYPE text USING convert_from("te_inf_adicionais", 'LATIN1');

-- comprometimento_operacao (1 colunas)
ALTER TABLE "public"."comprometimento_operacao" ALTER COLUMN "te_resultado_consulta" TYPE text USING convert_from("te_resultado_consulta", 'LATIN1');

-- comp_val_cobrado (1 colunas)
ALTER TABLE "public"."comp_val_cobrado" ALTER COLUMN "no_texto" TYPE text USING convert_from("no_texto", 'LATIN1');

-- condicao_perda_analise (3 colunas)
ALTER TABLE "public"."condicao_perda_analise" ALTER COLUMN "te_fase_sem_perda" TYPE text USING convert_from("te_fase_sem_perda", 'LATIN1');
ALTER TABLE "public"."condicao_perda_analise" ALTER COLUMN "te_tarefa_nao_cancela" TYPE text USING convert_from("te_tarefa_nao_cancela", 'LATIN1');
ALTER TABLE "public"."condicao_perda_analise" ALTER COLUMN "te_tarefa_resultado_analise" TYPE text USING convert_from("te_tarefa_resultado_analise", 'LATIN1');

-- cond_aquis_pretendente (1 colunas)
ALTER TABLE "public"."cond_aquis_pretendente" ALTER COLUMN "no_adaptacao_imovel" TYPE text USING convert_from("no_adaptacao_imovel", 'LATIN1');

-- configuracao_originacao (1 colunas)
ALTER TABLE "public"."configuracao_originacao" ALTER COLUMN "te_formulas_desconto" TYPE text USING convert_from("te_formulas_desconto", 'LATIN1');

-- configuracao_usuario (1 colunas)
ALTER TABLE "public"."configuracao_usuario" ALTER COLUMN "te_configuracao" TYPE text USING convert_from("te_configuracao", 'LATIN1');

-- config_crivo (5 colunas)
ALTER TABLE "public"."config_crivo" ALTER COLUMN "te_explain" TYPE text USING convert_from("te_explain", 'LATIN1');
ALTER TABLE "public"."config_crivo" ALTER COLUMN "te_loadlog" TYPE text USING convert_from("te_loadlog", 'LATIN1');
ALTER TABLE "public"."config_crivo" ALTER COLUMN "te_loadpdf" TYPE text USING convert_from("te_loadpdf", 'LATIN1');
ALTER TABLE "public"."config_crivo" ALTER COLUMN "te_setpolicyevalvalues" TYPE text USING convert_from("te_setpolicyevalvalues", 'LATIN1');
ALTER TABLE "public"."config_crivo" ALTER COLUMN "te_values" TYPE text USING convert_from("te_values", 'LATIN1');

-- config_modelos_contrato (2 colunas)
ALTER TABLE "public"."config_modelos_contrato" ALTER COLUMN "te_grupo_tipo_operacao" TYPE text USING convert_from("te_grupo_tipo_operacao", 'LATIN1');
ALTER TABLE "public"."config_modelos_contrato" ALTER COLUMN "te_tipo_operacao" TYPE text USING convert_from("te_tipo_operacao", 'LATIN1');

-- confrontacao_rua (1 colunas)
ALTER TABLE "public"."confrontacao_rua" ALTER COLUMN "te_descricao_confrontacao" TYPE text USING convert_from("te_descricao_confrontacao", 'LATIN1');

-- consulta_sig (1 colunas)
ALTER TABLE "public"."consulta_sig" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- consulta_sig_pret (1 colunas)
ALTER TABLE "public"."consulta_sig_pret" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- controleversao (1 colunas)
ALTER TABLE "public"."controleversao" ALTER COLUMN "te_observacao_versao" TYPE text USING convert_from("te_observacao_versao", 'LATIN1');

-- controle_escritura (2 colunas)
ALTER TABLE "public"."controle_escritura" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');
ALTER TABLE "public"."controle_escritura" ALTER COLUMN "te_qualificacao" TYPE text USING convert_from("te_qualificacao", 'LATIN1');

-- controle_escritura_documentos (1 colunas)
ALTER TABLE "public"."controle_escritura_documentos" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- cotacao (1 colunas)
ALTER TABLE "public"."cotacao" ALTER COLUMN "te_observacao_cotacao" TYPE text USING convert_from("te_observacao_cotacao", 'LATIN1');

-- cpfcnpj_bloqueado_originacao (1 colunas)
ALTER TABLE "public"."cpfcnpj_bloqueado_originacao" ALTER COLUMN "no_observacao" TYPE text USING convert_from("no_observacao", 'LATIN1');

-- cq (1 colunas)
ALTER TABLE "public"."cq" ALTER COLUMN "observacoes" TYPE text USING convert_from("observacoes", 'LATIN1');

-- cri (1 colunas)
ALTER TABLE "public"."cri" ALTER COLUMN "observacoes" TYPE text USING convert_from("observacoes", 'LATIN1');

-- criterio_prioridade (1 colunas)
ALTER TABLE "public"."criterio_prioridade" ALTER COLUMN "te_regra" TYPE text USING convert_from("te_regra", 'LATIN1');

-- criticas_sge (1 colunas)
ALTER TABLE "public"."criticas_sge" ALTER COLUMN "no_mensagem" TYPE text USING convert_from("no_mensagem", 'LATIN1');

-- critica_ocorr (1 colunas)
ALTER TABLE "public"."critica_ocorr" ALTER COLUMN "te_ocorrencia" TYPE text USING convert_from("te_ocorrencia", 'LATIN1');

-- custa_mutuario (1 colunas)
ALTER TABLE "public"."custa_mutuario" ALTER COLUMN "no_observacao_livre" TYPE text USING convert_from("no_observacao_livre", 'LATIN1');

-- dados_adicionais_sisat (1 colunas)
ALTER TABLE "public"."dados_adicionais_sisat" ALTER COLUMN "te_dados_adicionais" TYPE text USING convert_from("te_dados_adicionais", 'LATIN1');

-- dados_aux_renda_pretendente (1 colunas)
ALTER TABLE "public"."dados_aux_renda_pretendente" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- dados_crivo (1 colunas)
ALTER TABLE "public"."dados_crivo" ALTER COLUMN "te_resposta" TYPE text USING convert_from("te_resposta", 'LATIN1');

-- dados_historico_cobranca (1 colunas)
ALTER TABLE "public"."dados_historico_cobranca" ALTER COLUMN "te_historico" TYPE text USING convert_from("te_historico", 'LATIN1');

-- dados_openbanking (1 colunas)
ALTER TABLE "public"."dados_openbanking" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- demonstrativo_lan (1 colunas)
ALTER TABLE "public"."demonstrativo_lan" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- dependencia_tarefa_padrao (1 colunas)
ALTER TABLE "public"."dependencia_tarefa_padrao" ALTER COLUMN "te_status_finalizacao" TYPE text USING convert_from("te_status_finalizacao", 'LATIN1');

-- dependente_pretendente (1 colunas)
ALTER TABLE "public"."dependente_pretendente" ALTER COLUMN "no_descr_deficiencia" TYPE text USING convert_from("no_descr_deficiencia", 'LATIN1');

-- despesa_originacao (3 colunas)
ALTER TABLE "public"."despesa_originacao" ALTER COLUMN "te_expressao_pode_incorporar" TYPE text USING convert_from("te_expressao_pode_incorporar", 'LATIN1');
ALTER TABLE "public"."despesa_originacao" ALTER COLUMN "te_municipio" TYPE text USING convert_from("te_municipio", 'LATIN1');
ALTER TABLE "public"."despesa_originacao" ALTER COLUMN "te_observacao_despesa" TYPE text USING convert_from("te_observacao_despesa", 'LATIN1');

-- docs_pendentes (1 colunas)
ALTER TABLE "public"."docs_pendentes" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- documento_externo (1 colunas)
ALTER TABLE "public"."documento_externo" ALTER COLUMN "te_payload_ext" TYPE text USING convert_from("te_payload_ext", 'LATIN1');

-- documento_grupo_documento (14 colunas)
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_atividade_profissional" TYPE text USING convert_from("te_atividade_profissional", 'LATIN1');
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_co_enquadramento" TYPE text USING convert_from("te_co_enquadramento", 'LATIN1');
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_co_perfil_doc_desabilitado" TYPE text USING convert_from("te_co_perfil_doc_desabilitado", 'LATIN1');
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_co_perfil_doc_especifico" TYPE text USING convert_from("te_co_perfil_doc_especifico", 'LATIN1');
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_documento" TYPE text USING convert_from("te_documento", 'LATIN1');
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_expressao_exibicao" TYPE text USING convert_from("te_expressao_exibicao", 'LATIN1');
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_fase_operacao" TYPE text USING convert_from("te_fase_operacao", 'LATIN1');
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_grupo_tipo_operacao" TYPE text USING convert_from("te_grupo_tipo_operacao", 'LATIN1');
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_municipio" TYPE text USING convert_from("te_municipio", 'LATIN1');
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_munic_cartorio_registro" TYPE text USING convert_from("te_munic_cartorio_registro", 'LATIN1');
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_tipo_operacao" TYPE text USING convert_from("te_tipo_operacao", 'LATIN1');
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_tipo_op_certid_obrigatoria" TYPE text USING convert_from("te_tipo_op_certid_obrigatoria", 'LATIN1');
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_uf" TYPE text USING convert_from("te_uf", 'LATIN1');
ALTER TABLE "public"."documento_grupo_documento" ALTER COLUMN "te_uf_cartorio_registro" TYPE text USING convert_from("te_uf_cartorio_registro", 'LATIN1');

-- documento_ocorrencia_sisat (1 colunas)
ALTER TABLE "public"."documento_ocorrencia_sisat" ALTER COLUMN "te_obs_analista" TYPE text USING convert_from("te_obs_analista", 'LATIN1');

-- documento_operacao (4 colunas)
ALTER TABLE "public"."documento_operacao" ALTER COLUMN "te_dados_ocr" TYPE text USING convert_from("te_dados_ocr", 'LATIN1');
ALTER TABLE "public"."documento_operacao" ALTER COLUMN "te_exigencia" TYPE text USING convert_from("te_exigencia", 'LATIN1');
ALTER TABLE "public"."documento_operacao" ALTER COLUMN "te_obs_analista" TYPE text USING convert_from("te_obs_analista", 'LATIN1');
ALTER TABLE "public"."documento_operacao" ALTER COLUMN "te_obs_consultor" TYPE text USING convert_from("te_obs_consultor", 'LATIN1');

-- documento_tipo_atendimento (1 colunas)
ALTER TABLE "public"."documento_tipo_atendimento" ALTER COLUMN "te_tipo_documento" TYPE text USING convert_from("te_tipo_documento", 'LATIN1');

-- email_anexo (1 colunas)
ALTER TABLE "public"."email_anexo" ALTER COLUMN "te_anexo" TYPE text USING convert_from("te_anexo", 'LATIN1');

-- email_a_enviar (6 colunas)
ALTER TABLE "public"."email_a_enviar" ALTER COLUMN "te_corpo_mensagem" TYPE text USING convert_from("te_corpo_mensagem", 'LATIN1');
ALTER TABLE "public"."email_a_enviar" ALTER COLUMN "te_destinatario" TYPE text USING convert_from("te_destinatario", 'LATIN1');
ALTER TABLE "public"."email_a_enviar" ALTER COLUMN "te_destinatario_cc" TYPE text USING convert_from("te_destinatario_cc", 'LATIN1');
ALTER TABLE "public"."email_a_enviar" ALTER COLUMN "te_destinatario_cco" TYPE text USING convert_from("te_destinatario_cco", 'LATIN1');
ALTER TABLE "public"."email_a_enviar" ALTER COLUMN "te_documento_anexo" TYPE text USING convert_from("te_documento_anexo", 'LATIN1');
ALTER TABLE "public"."email_a_enviar" ALTER COLUMN "te_erro" TYPE text USING convert_from("te_erro", 'LATIN1');

-- email_esteira (1 colunas)
ALTER TABLE "public"."email_esteira" ALTER COLUMN "te_mensagem" TYPE text USING convert_from("te_mensagem", 'LATIN1');

-- email_sistema (1 colunas)
ALTER TABLE "public"."email_sistema" ALTER COLUMN "te_assunto_email" TYPE text USING convert_from("te_assunto_email", 'LATIN1');

-- entidades (3 colunas)
ALTER TABLE "public"."entidades" ALTER COLUMN "desc_contratos" TYPE text USING convert_from("desc_contratos", 'LATIN1');
ALTER TABLE "public"."entidades" ALTER COLUMN "qual_repres" TYPE text USING convert_from("qual_repres", 'LATIN1');
ALTER TABLE "public"."entidades" ALTER COLUMN "te_observacao_cliente" TYPE text USING convert_from("te_observacao_cliente", 'LATIN1');

-- entidade_scci (1 colunas)
ALTER TABLE "public"."entidade_scci" ALTER COLUMN "te_tabelionato_pacto" TYPE text USING convert_from("te_tabelionato_pacto", 'LATIN1');

-- entrevista (1 colunas)
ALTER TABLE "public"."entrevista" ALTER COLUMN "descricao" TYPE text USING convert_from("descricao", 'LATIN1');

-- eventos (1 colunas)
ALTER TABLE "public"."eventos" ALTER COLUMN "te_parametros" TYPE text USING convert_from("te_parametros", 'LATIN1');

-- evento_chatbot (2 colunas)
ALTER TABLE "public"."evento_chatbot" ALTER COLUMN "te_mensagem" TYPE text USING convert_from("te_mensagem", 'LATIN1');
ALTER TABLE "public"."evento_chatbot" ALTER COLUMN "te_retorno_acao" TYPE text USING convert_from("te_retorno_acao", 'LATIN1');

-- exigencia_cartoraria (2 colunas)
ALTER TABLE "public"."exigencia_cartoraria" ALTER COLUMN "te_descricao" TYPE text USING convert_from("te_descricao", 'LATIN1');
ALTER TABLE "public"."exigencia_cartoraria" ALTER COLUMN "te_exigencia" TYPE text USING convert_from("te_exigencia", 'LATIN1');

-- extrato_atr_aviso (1 colunas)
ALTER TABLE "public"."extrato_atr_aviso" ALTER COLUMN "quadro_atraso" TYPE text USING convert_from("quadro_atraso", 'LATIN1');

-- fase_operacao (1 colunas)
ALTER TABLE "public"."fase_operacao" ALTER COLUMN "te_desc_fase" TYPE text USING convert_from("te_desc_fase", 'LATIN1');

-- fatura (1 colunas)
ALTER TABLE "public"."fatura" ALTER COLUMN "te_observacao_fatura" TYPE text USING convert_from("te_observacao_fatura", 'LATIN1');

-- faturamento_anual_por_operacao (2 colunas)
ALTER TABLE "public"."faturamento_anual_por_operacao" ALTER COLUMN "inf_comentario" TYPE text USING convert_from("inf_comentario", 'LATIN1');
ALTER TABLE "public"."faturamento_anual_por_operacao" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- fcvs_controle (1 colunas)
ALTER TABLE "public"."fcvs_controle" ALTER COLUMN "te_campos" TYPE text USING convert_from("te_campos", 'LATIN1');

-- fcvs_oficios (7 colunas)
ALTER TABLE "public"."fcvs_oficios" ALTER COLUMN "no_exigen1" TYPE text USING convert_from("no_exigen1", 'LATIN1');
ALTER TABLE "public"."fcvs_oficios" ALTER COLUMN "no_exigen2" TYPE text USING convert_from("no_exigen2", 'LATIN1');
ALTER TABLE "public"."fcvs_oficios" ALTER COLUMN "no_exigen3" TYPE text USING convert_from("no_exigen3", 'LATIN1');
ALTER TABLE "public"."fcvs_oficios" ALTER COLUMN "no_exigen4" TYPE text USING convert_from("no_exigen4", 'LATIN1');
ALTER TABLE "public"."fcvs_oficios" ALTER COLUMN "no_exigen5" TYPE text USING convert_from("no_exigen5", 'LATIN1');
ALTER TABLE "public"."fcvs_oficios" ALTER COLUMN "te_motivo_negativa" TYPE text USING convert_from("te_motivo_negativa", 'LATIN1');
ALTER TABLE "public"."fcvs_oficios" ALTER COLUMN "te_observacoes" TYPE text USING convert_from("te_observacoes", 'LATIN1');

-- fgts_atendimento (1 colunas)
ALTER TABLE "public"."fgts_atendimento" ALTER COLUMN "te_filhos_segurado" TYPE text USING convert_from("te_filhos_segurado", 'LATIN1');

-- fornecedor (1 colunas)
ALTER TABLE "public"."fornecedor" ALTER COLUMN "te_observacao_fornecedor" TYPE text USING convert_from("te_observacao_fornecedor", 'LATIN1');

-- fx_renda_fam_por_operacao (1 colunas)
ALTER TABLE "public"."fx_renda_fam_por_operacao" ALTER COLUMN "te_uf" TYPE text USING convert_from("te_uf", 'LATIN1');

-- gravame (1 colunas)
ALTER TABLE "public"."gravame" ALTER COLUMN "no_identificacao_gravame" TYPE text USING convert_from("no_identificacao_gravame", 'LATIN1');

-- grupo_pontuacao_risco (1 colunas)
ALTER TABLE "public"."grupo_pontuacao_risco" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- grupo_tipo_operacao (1 colunas)
ALTER TABLE "public"."grupo_tipo_operacao" ALTER COLUMN "te_descricao_grupo" TYPE text USING convert_from("te_descricao_grupo", 'LATIN1');

-- histlog (1 colunas)
ALTER TABLE "public"."histlog" ALTER COLUMN "te_comentario" TYPE text USING convert_from("te_comentario", 'LATIN1');

-- historico_imovel (1 colunas)
ALTER TABLE "public"."historico_imovel" ALTER COLUMN "te_motivo_alteracao" TYPE text USING convert_from("te_motivo_alteracao", 'LATIN1');

-- historico_parecer (1 colunas)
ALTER TABLE "public"."historico_parecer" ALTER COLUMN "te_parecer" TYPE text USING convert_from("te_parecer", 'LATIN1');

-- historico_projeto (1 colunas)
ALTER TABLE "public"."historico_projeto" ALTER COLUMN "te_motivo_alteracao" TYPE text USING convert_from("te_motivo_alteracao", 'LATIN1');

-- historico_registro_cartorio (2 colunas)
ALTER TABLE "public"."historico_registro_cartorio" ALTER COLUMN "te_notificacao" TYPE text USING convert_from("te_notificacao", 'LATIN1');
ALTER TABLE "public"."historico_registro_cartorio" ALTER COLUMN "url_documentos" TYPE text USING convert_from("url_documentos", 'LATIN1');

-- historico_texto_ocorrencia (1 colunas)
ALTER TABLE "public"."historico_texto_ocorrencia" ALTER COLUMN "te_texto" TYPE text USING convert_from("te_texto", 'LATIN1');

-- hist_infadicional (1 colunas)
ALTER TABLE "public"."hist_infadicional" ALTER COLUMN "te_infadicional" TYPE text USING convert_from("te_infadicional", 'LATIN1');

-- imovel_bloqueado (1 colunas)
ALTER TABLE "public"."imovel_bloqueado" ALTER COLUMN "te_motivo_bloqueio" TYPE text USING convert_from("te_motivo_bloqueio", 'LATIN1');

-- imovel_operacao (7 colunas)
ALTER TABLE "public"."imovel_operacao" ALTER COLUMN "te_avaliacao_imovel" TYPE text USING convert_from("te_avaliacao_imovel", 'LATIN1');
ALTER TABLE "public"."imovel_operacao" ALTER COLUMN "te_descricao_rgi" TYPE text USING convert_from("te_descricao_rgi", 'LATIN1');
ALTER TABLE "public"."imovel_operacao" ALTER COLUMN "te_instrumento_comp_anterior" TYPE text USING convert_from("te_instrumento_comp_anterior", 'LATIN1');
ALTER TABLE "public"."imovel_operacao" ALTER COLUMN "te_observacao_avaliacao" TYPE text USING convert_from("te_observacao_avaliacao", 'LATIN1');
ALTER TABLE "public"."imovel_operacao" ALTER COLUMN "te_referencia_localizacao" TYPE text USING convert_from("te_referencia_localizacao", 'LATIN1');
ALTER TABLE "public"."imovel_operacao" ALTER COLUMN "te_rgi_comp_anterior" TYPE text USING convert_from("te_rgi_comp_anterior", 'LATIN1');
ALTER TABLE "public"."imovel_operacao" ALTER COLUMN "te_tabelionato_comp_anterior" TYPE text USING convert_from("te_tabelionato_comp_anterior", 'LATIN1');

-- imovel_terreno (1 colunas)
ALTER TABLE "public"."imovel_terreno" ALTER COLUMN "no_endereco_imovel" TYPE text USING convert_from("no_endereco_imovel", 'LATIN1');

-- implantacao_fgts (1 colunas)
ALTER TABLE "public"."implantacao_fgts" ALTER COLUMN "te_atrasofgts" TYPE text USING convert_from("te_atrasofgts", 'LATIN1');

-- imv_operacao_empresario (4 colunas)
ALTER TABLE "public"."imv_operacao_empresario" ALTER COLUMN "te_avaliacao_imovel" TYPE text USING convert_from("te_avaliacao_imovel", 'LATIN1');
ALTER TABLE "public"."imv_operacao_empresario" ALTER COLUMN "te_descricao_rgi" TYPE text USING convert_from("te_descricao_rgi", 'LATIN1');
ALTER TABLE "public"."imv_operacao_empresario" ALTER COLUMN "te_observacao_avaliacao" TYPE text USING convert_from("te_observacao_avaliacao", 'LATIN1');
ALTER TABLE "public"."imv_operacao_empresario" ALTER COLUMN "te_referencia_localizacao" TYPE text USING convert_from("te_referencia_localizacao", 'LATIN1');

-- incorporacao (1 colunas)
ALTER TABLE "public"."incorporacao" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- inf_adicional_gestao (1 colunas)
ALTER TABLE "public"."inf_adicional_gestao" ALTER COLUMN "te_inf_adicional" TYPE text USING convert_from("te_inf_adicional", 'LATIN1');

-- inf_adicional_parecer (1 colunas)
ALTER TABLE "public"."inf_adicional_parecer" ALTER COLUMN "te_inf_adicional" TYPE text USING convert_from("te_inf_adicional", 'LATIN1');

-- integracao_arisp (5 colunas)
ALTER TABLE "public"."integracao_arisp" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');
ALTER TABLE "public"."integracao_arisp" ALTER COLUMN "te_url_boleto" TYPE text USING convert_from("te_url_boleto", 'LATIN1');
ALTER TABLE "public"."integracao_arisp" ALTER COLUMN "te_url_doc_enviado" TYPE text USING convert_from("te_url_doc_enviado", 'LATIN1');
ALTER TABLE "public"."integracao_arisp" ALTER COLUMN "te_url_doc_recebido" TYPE text USING convert_from("te_url_doc_recebido", 'LATIN1');
ALTER TABLE "public"."integracao_arisp" ALTER COLUMN "te_url_exigencia" TYPE text USING convert_from("te_url_exigencia", 'LATIN1');

-- integra_sgl (1 colunas)
ALTER TABLE "public"."integra_sgl" ALTER COLUMN "te_garantia_1" TYPE text USING convert_from("te_garantia_1", 'LATIN1');

-- item_pontuacao_risco (1 colunas)
ALTER TABLE "public"."item_pontuacao_risco" ALTER COLUMN "no_regra" TYPE text USING convert_from("no_regra", 'LATIN1');

-- item_reestruturacao (1 colunas)
ALTER TABLE "public"."item_reestruturacao" ALTER COLUMN "te_criterio" TYPE text USING convert_from("te_criterio", 'LATIN1');

-- leiautes_impexp (1 colunas)
ALTER TABLE "public"."leiautes_impexp" ALTER COLUMN "te_leiaute" TYPE text USING convert_from("te_leiaute", 'LATIN1');

-- liberacao_fgtsconst (1 colunas)
ALTER TABLE "public"."liberacao_fgtsconst" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- liberacao_vendedor (1 colunas)
ALTER TABLE "public"."liberacao_vendedor" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- libera_hipoteca (3 colunas)
ALTER TABLE "public"."libera_hipoteca" ALTER COLUMN "te_doc_apresentados" TYPE text USING convert_from("te_doc_apresentados", 'LATIN1');
ALTER TABLE "public"."libera_hipoteca" ALTER COLUMN "te_observacao_espelho" TYPE text USING convert_from("te_observacao_espelho", 'LATIN1');
ALTER TABLE "public"."libera_hipoteca" ALTER COLUMN "te_observacao_oficio" TYPE text USING convert_from("te_observacao_oficio", 'LATIN1');

-- logpg (1 colunas)
ALTER TABLE "public"."logpg" ALTER COLUMN "te_relato" TYPE text USING convert_from("te_relato", 'LATIN1');

-- log_campanha (1 colunas)
ALTER TABLE "public"."log_campanha" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- log_eventos (1 colunas)
ALTER TABLE "public"."log_eventos" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- log_eventos_exc (1 colunas)
ALTER TABLE "public"."log_eventos_exc" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- lote_danificado (4 colunas)
ALTER TABLE "public"."lote_danificado" ALTER COLUMN "te_aprovacao_quitacao_ata" TYPE text USING convert_from("te_aprovacao_quitacao_ata", 'LATIN1');
ALTER TABLE "public"."lote_danificado" ALTER COLUMN "te_descricao_avarias" TYPE text USING convert_from("te_descricao_avarias", 'LATIN1');
ALTER TABLE "public"."lote_danificado" ALTER COLUMN "te_descricao_negativa" TYPE text USING convert_from("te_descricao_negativa", 'LATIN1');
ALTER TABLE "public"."lote_danificado" ALTER COLUMN "te_descricao_prestacoes" TYPE text USING convert_from("te_descricao_prestacoes", 'LATIN1');

-- lote_projeto (2 colunas)
ALTER TABLE "public"."lote_projeto" ALTER COLUMN "te_modelo_livre" TYPE text USING convert_from("te_modelo_livre", 'LATIN1');
ALTER TABLE "public"."lote_projeto" ALTER COLUMN "te_observacao_lote" TYPE text USING convert_from("te_observacao_lote", 'LATIN1');

-- matriz_obrigatoriedade (2 colunas)
ALTER TABLE "public"."matriz_obrigatoriedade" ALTER COLUMN "te_expressao_condicao" TYPE text USING convert_from("te_expressao_condicao", 'LATIN1');
ALTER TABLE "public"."matriz_obrigatoriedade" ALTER COLUMN "te_mensagem" TYPE text USING convert_from("te_mensagem", 'LATIN1');

-- matriz_pontuacao_risco (4 colunas)
ALTER TABLE "public"."matriz_pontuacao_risco" ALTER COLUMN "te_carteira" TYPE text USING convert_from("te_carteira", 'LATIN1');
ALTER TABLE "public"."matriz_pontuacao_risco" ALTER COLUMN "te_grupo_tipo_operacao" TYPE text USING convert_from("te_grupo_tipo_operacao", 'LATIN1');
ALTER TABLE "public"."matriz_pontuacao_risco" ALTER COLUMN "te_modalidade" TYPE text USING convert_from("te_modalidade", 'LATIN1');
ALTER TABLE "public"."matriz_pontuacao_risco" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- matriz_travas_ori (3 colunas)
ALTER TABLE "public"."matriz_travas_ori" ALTER COLUMN "te_fase_operacao" TYPE text USING convert_from("te_fase_operacao", 'LATIN1');
ALTER TABLE "public"."matriz_travas_ori" ALTER COLUMN "te_grupo_tipo_operacao" TYPE text USING convert_from("te_grupo_tipo_operacao", 'LATIN1');
ALTER TABLE "public"."matriz_travas_ori" ALTER COLUMN "te_status_finalizacao" TYPE text USING convert_from("te_status_finalizacao", 'LATIN1');

-- memomsg (1 colunas)
ALTER TABLE "public"."memomsg" ALTER COLUMN "memo" TYPE text USING convert_from("memo", 'LATIN1');

-- modelo_email_campanha (1 colunas)
ALTER TABLE "public"."modelo_email_campanha" ALTER COLUMN "te_modelo" TYPE text USING convert_from("te_modelo", 'LATIN1');

-- motreimp (1 colunas)
ALTER TABLE "public"."motreimp" ALTER COLUMN "mot_depura" TYPE text USING convert_from("mot_depura", 'LATIN1');

-- movimentos_openbanking (1 colunas)
ALTER TABLE "public"."movimentos_openbanking" ALTER COLUMN "fato_gerador_tarifa" TYPE text USING convert_from("fato_gerador_tarifa", 'LATIN1');

-- movimentos_passivo (1 colunas)
ALTER TABLE "public"."movimentos_passivo" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- movs_garantia (1 colunas)
ALTER TABLE "public"."movs_garantia" ALTER COLUMN "detalhe_mov" TYPE text USING convert_from("detalhe_mov", 'LATIN1');

-- nf_material_construcao (1 colunas)
ALTER TABLE "public"."nf_material_construcao" ALTER COLUMN "te_ressalva" TYPE text USING convert_from("te_ressalva", 'LATIN1');

-- nmov (1 colunas)
ALTER TABLE "public"."nmov" ALTER COLUMN "te_campos" TYPE text USING convert_from("te_campos", 'LATIN1');

-- novacao (1 colunas)
ALTER TABLE "public"."novacao" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- ocorrencia (2 colunas)
ALTER TABLE "public"."ocorrencia" ALTER COLUMN "te_descricao_ocorrencia" TYPE text USING convert_from("te_descricao_ocorrencia", 'LATIN1');
ALTER TABLE "public"."ocorrencia" ALTER COLUMN "te_observacao_execucao" TYPE text USING convert_from("te_observacao_execucao", 'LATIN1');

-- ocorrencias_codmovev (1 colunas)
ALTER TABLE "public"."ocorrencias_codmovev" ALTER COLUMN "te_comentario" TYPE text USING convert_from("te_comentario", 'LATIN1');

-- ocorrencia_sisat (3 colunas)
ALTER TABLE "public"."ocorrencia_sisat" ALTER COLUMN "te_descricao_ocorrencia" TYPE text USING convert_from("te_descricao_ocorrencia", 'LATIN1');
ALTER TABLE "public"."ocorrencia_sisat" ALTER COLUMN "te_observacao_execucao" TYPE text USING convert_from("te_observacao_execucao", 'LATIN1');
ALTER TABLE "public"."ocorrencia_sisat" ALTER COLUMN "te_variavel_tarefa" TYPE text USING convert_from("te_variavel_tarefa", 'LATIN1');

-- operacao_credito (33 colunas)
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_aprova_emissao_ctr" TYPE text USING convert_from("te_aprova_emissao_ctr", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_cancelamento" TYPE text USING convert_from("te_cancelamento", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_exigencia_cartorio" TYPE text USING convert_from("te_exigencia_cartorio", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_motivo_rejeicao" TYPE text USING convert_from("te_motivo_rejeicao", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_observacao_ab_endividamento" TYPE text USING convert_from("te_observacao_ab_endividamento", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_observacoes" TYPE text USING convert_from("te_observacoes", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_observacoes_1" TYPE text USING convert_from("te_observacoes_1", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_observacoes_2" TYPE text USING convert_from("te_observacoes_2", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_observacoes_3" TYPE text USING convert_from("te_observacoes_3", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_observacoes_4" TYPE text USING convert_from("te_observacoes_4", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_obs_motivo_emprestimo" TYPE text USING convert_from("te_obs_motivo_emprestimo", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_parecer_credito" TYPE text USING convert_from("te_parecer_credito", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_parecer_credito_auditor" TYPE text USING convert_from("te_parecer_credito_auditor", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_parecer_dados_aux_renda" TYPE text USING convert_from("te_parecer_dados_aux_renda", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_parecer_enquadramento" TYPE text USING convert_from("te_parecer_enquadramento", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_parecer_informacoes" TYPE text USING convert_from("te_parecer_informacoes", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_parecer_juridico" TYPE text USING convert_from("te_parecer_juridico", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_parecer_registro" TYPE text USING convert_from("te_parecer_registro", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_parecer_registro_auditor" TYPE text USING convert_from("te_parecer_registro_auditor", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_parecer_renda_familiar" TYPE text USING convert_from("te_parecer_renda_familiar", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_parecer_tecnico" TYPE text USING convert_from("te_parecer_tecnico", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_pedido_avaliacao" TYPE text USING convert_from("te_pedido_avaliacao", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_pendencia_operacao" TYPE text USING convert_from("te_pendencia_operacao", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_prioridade" TYPE text USING convert_from("te_prioridade", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_qualificacao_iq" TYPE text USING convert_from("te_qualificacao_iq", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_qualific_compradores" TYPE text USING convert_from("te_qualific_compradores", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_qualific_construtor" TYPE text USING convert_from("te_qualific_construtor", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_qualific_empreendimento" TYPE text USING convert_from("te_qualific_empreendimento", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_qualific_fiadores" TYPE text USING convert_from("te_qualific_fiadores", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_qualific_interveniente" TYPE text USING convert_from("te_qualific_interveniente", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_qualific_regular_fiscal" TYPE text USING convert_from("te_qualific_regular_fiscal", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_qualific_vendedores" TYPE text USING convert_from("te_qualific_vendedores", 'LATIN1');
ALTER TABLE "public"."operacao_credito" ALTER COLUMN "te_series_operacao" TYPE text USING convert_from("te_series_operacao", 'LATIN1');

-- outra_liberacao_originacao (1 colunas)
ALTER TABLE "public"."outra_liberacao_originacao" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- pagamento_premio (1 colunas)
ALTER TABLE "public"."pagamento_premio" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- parcelasctb (1 colunas)
ALTER TABLE "public"."parcelasctb" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- parcela_lote (1 colunas)
ALTER TABLE "public"."parcela_lote" ALTER COLUMN "te_descricao" TYPE text USING convert_from("te_descricao", 'LATIN1');

-- parecer_imobiliario (6 colunas)
ALTER TABLE "public"."parecer_imobiliario" ALTER COLUMN "te_compl_munic_sit_soc_eco" TYPE text USING convert_from("te_compl_munic_sit_soc_eco", 'LATIN1');
ALTER TABLE "public"."parecer_imobiliario" ALTER COLUMN "te_compl_mut_disp_pag" TYPE text USING convert_from("te_compl_mut_disp_pag", 'LATIN1');
ALTER TABLE "public"."parecer_imobiliario" ALTER COLUMN "te_conclusao" TYPE text USING convert_from("te_conclusao", 'LATIN1');
ALTER TABLE "public"."parecer_imobiliario" ALTER COLUMN "te_lider_comunitario" TYPE text USING convert_from("te_lider_comunitario", 'LATIN1');
ALTER TABLE "public"."parecer_imobiliario" ALTER COLUMN "te_parecer_imobiliario" TYPE text USING convert_from("te_parecer_imobiliario", 'LATIN1');
ALTER TABLE "public"."parecer_imobiliario" ALTER COLUMN "te_parecer_social" TYPE text USING convert_from("te_parecer_social", 'LATIN1');

-- parecer_operacao (1 colunas)
ALTER TABLE "public"."parecer_operacao" ALTER COLUMN "te_parecer" TYPE text USING convert_from("te_parecer", 'LATIN1');

-- partes_acao_juridico (2 colunas)
ALTER TABLE "public"."partes_acao_juridico" ALTER COLUMN "te_endereco" TYPE text USING convert_from("te_endereco", 'LATIN1');
ALTER TABLE "public"."partes_acao_juridico" ALTER COLUMN "te_obs" TYPE text USING convert_from("te_obs", 'LATIN1');

-- pendencia_aprovacao (1 colunas)
ALTER TABLE "public"."pendencia_aprovacao" ALTER COLUMN "te_descricao_pendencia" TYPE text USING convert_from("te_descricao_pendencia", 'LATIN1');

-- pendencia_operacao (1 colunas)
ALTER TABLE "public"."pendencia_operacao" ALTER COLUMN "te_pendencia" TYPE text USING convert_from("te_pendencia", 'LATIN1');

-- perfil (1 colunas)
ALTER TABLE "public"."perfil" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- permissao_acesso (1 colunas)
ALTER TABLE "public"."permissao_acesso" ALTER COLUMN "te_permissao_acesso" TYPE text USING convert_from("te_permissao_acesso", 'LATIN1');

-- pessoa_pretendente (15 colunas)
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "no_descr_deficiencia" TYPE text USING convert_from("no_descr_deficiencia", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "no_motivo" TYPE text USING convert_from("no_motivo", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "no_procedencia" TYPE text USING convert_from("no_procedencia", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "te_contrato_social" TYPE text USING convert_from("te_contrato_social", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "te_declaracao_negocio" TYPE text USING convert_from("te_declaracao_negocio", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "te_declaracao_origem_recurso" TYPE text USING convert_from("te_declaracao_origem_recurso", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "te_envio_consulta_crivo" TYPE text USING convert_from("te_envio_consulta_crivo", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "te_filiais" TYPE text USING convert_from("te_filiais", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "te_inf_adicional" TYPE text USING convert_from("te_inf_adicional", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "te_observacao_prof" TYPE text USING convert_from("te_observacao_prof", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "te_resposta_crivo" TYPE text USING convert_from("te_resposta_crivo", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "te_restricao_securitaria" TYPE text USING convert_from("te_restricao_securitaria", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "te_rgi_pacto" TYPE text USING convert_from("te_rgi_pacto", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "te_sucessao" TYPE text USING convert_from("te_sucessao", 'LATIN1');
ALTER TABLE "public"."pessoa_pretendente" ALTER COLUMN "te_tabelionato_pacto" TYPE text USING convert_from("te_tabelionato_pacto", 'LATIN1');

-- pontuacao_risco_contrato (1 colunas)
ALTER TABLE "public"."pontuacao_risco_contrato" ALTER COLUMN "te_pontuacao" TYPE text USING convert_from("te_pontuacao", 'LATIN1');

-- pontuacao_risco_operacao (1 colunas)
ALTER TABLE "public"."pontuacao_risco_operacao" ALTER COLUMN "te_pontuacao" TYPE text USING convert_from("te_pontuacao", 'LATIN1');

-- portabilidade (2 colunas)
ALTER TABLE "public"."portabilidade" ALTER COLUMN "te_erro" TYPE text USING convert_from("te_erro", 'LATIN1');
ALTER TABLE "public"."portabilidade" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- posicao_contrato (1 colunas)
ALTER TABLE "public"."posicao_contrato" ALTER COLUMN "te_resumo_cobranca" TYPE text USING convert_from("te_resumo_cobranca", 'LATIN1');

-- posicao_mensal_gestor (3 colunas)
ALTER TABLE "public"."posicao_mensal_gestor" ALTER COLUMN "obs_ajuste" TYPE text USING convert_from("obs_ajuste", 'LATIN1');
ALTER TABLE "public"."posicao_mensal_gestor" ALTER COLUMN "obs_remuneracao_gestor" TYPE text USING convert_from("obs_remuneracao_gestor", 'LATIN1');
ALTER TABLE "public"."posicao_mensal_gestor" ALTER COLUMN "obs_remuneracao_saldo_medio" TYPE text USING convert_from("obs_remuneracao_saldo_medio", 'LATIN1');

-- posicao_suspensao (1 colunas)
ALTER TABLE "public"."posicao_suspensao" ALTER COLUMN "te_situacoes" TYPE text USING convert_from("te_situacoes", 'LATIN1');

-- prefeitura (2 colunas)
ALTER TABLE "public"."prefeitura" ALTER COLUMN "no_observacao" TYPE text USING convert_from("no_observacao", 'LATIN1');
ALTER TABLE "public"."prefeitura" ALTER COLUMN "te_checklist" TYPE text USING convert_from("te_checklist", 'LATIN1');

-- pretendente (3 colunas)
ALTER TABLE "public"."pretendente" ALTER COLUMN "no_analise" TYPE text USING convert_from("no_analise", 'LATIN1');
ALTER TABLE "public"."pretendente" ALTER COLUMN "no_memo" TYPE text USING convert_from("no_memo", 'LATIN1');
ALTER TABLE "public"."pretendente" ALTER COLUMN "no_obs" TYPE text USING convert_from("no_obs", 'LATIN1');

-- processo_fisico (1 colunas)
ALTER TABLE "public"."processo_fisico" ALTER COLUMN "te_obs_bloqueio" TYPE text USING convert_from("te_obs_bloqueio", 'LATIN1');

-- prog_rec_credito (2 colunas)
ALTER TABLE "public"."prog_rec_credito" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');
ALTER TABLE "public"."prog_rec_credito" ALTER COLUMN "te_motivo_canc_calc_prc" TYPE text USING convert_from("te_motivo_canc_calc_prc", 'LATIN1');

-- prog_rec_credito_implantado (1 colunas)
ALTER TABLE "public"."prog_rec_credito_implantado" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- projeto (6 colunas)
ALTER TABLE "public"."projeto" ALTER COLUMN "te_area_verde" TYPE text USING convert_from("te_area_verde", 'LATIN1');
ALTER TABLE "public"."projeto" ALTER COLUMN "te_avaliacao_tecnica" TYPE text USING convert_from("te_avaliacao_tecnica", 'LATIN1');
ALTER TABLE "public"."projeto" ALTER COLUMN "te_descricao_ruas" TYPE text USING convert_from("te_descricao_ruas", 'LATIN1');
ALTER TABLE "public"."projeto" ALTER COLUMN "te_padrao_construcao" TYPE text USING convert_from("te_padrao_construcao", 'LATIN1');
ALTER TABLE "public"."projeto" ALTER COLUMN "te_projeto_loteamento" TYPE text USING convert_from("te_projeto_loteamento", 'LATIN1');
ALTER TABLE "public"."projeto" ALTER COLUMN "te_reserva_tecnica" TYPE text USING convert_from("te_reserva_tecnica", 'LATIN1');

-- projeto_bloqueado (1 colunas)
ALTER TABLE "public"."projeto_bloqueado" ALTER COLUMN "te_motivo_bloqueio" TYPE text USING convert_from("te_motivo_bloqueio", 'LATIN1');

-- quadro_obras_operacao_cred (1 colunas)
ALTER TABLE "public"."quadro_obras_operacao_cred" ALTER COLUMN "te_observacao_ab_obras" TYPE text USING convert_from("te_observacao_ab_obras", 'LATIN1');

-- recorte_desconto_fgts (1 colunas)
ALTER TABLE "public"."recorte_desconto_fgts" ALTER COLUMN "te_recorte" TYPE text USING convert_from("te_recorte", 'LATIN1');

-- recurso_proprio_operacao (1 colunas)
ALTER TABLE "public"."recurso_proprio_operacao" ALTER COLUMN "te_descricao_fonte" TYPE text USING convert_from("te_descricao_fonte", 'LATIN1');

-- registro_geral_imovel (1 colunas)
ALTER TABLE "public"."registro_geral_imovel" ALTER COLUMN "te_observacao_rgi" TYPE text USING convert_from("te_observacao_rgi", 'LATIN1');

-- registro_voto (1 colunas)
ALTER TABLE "public"."registro_voto" ALTER COLUMN "no_comentario" TYPE text USING convert_from("no_comentario", 'LATIN1');

-- renegociacao (1 colunas)
ALTER TABLE "public"."renegociacao" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- renegociacao_com_novacao (1 colunas)
ALTER TABLE "public"."renegociacao_com_novacao" ALTER COLUMN "te_obs" TYPE text USING convert_from("te_obs", 'LATIN1');

-- renegociacao_implantada (1 colunas)
ALTER TABLE "public"."renegociacao_implantada" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- renegociacao_pendente (1 colunas)
ALTER TABLE "public"."renegociacao_pendente" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- representante_legal (1 colunas)
ALTER TABLE "public"."representante_legal" ALTER COLUMN "te_procuracao" TYPE text USING convert_from("te_procuracao", 'LATIN1');

-- rescisao_contratual (1 colunas)
ALTER TABLE "public"."rescisao_contratual" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- result_gerainterfaces (1 colunas)
ALTER TABLE "public"."result_gerainterfaces" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- rps (1 colunas)
ALTER TABLE "public"."rps" ALTER COLUMN "te_mov" TYPE text USING convert_from("te_mov", 'LATIN1');

-- seguradora_tipo_operacao (1 colunas)
ALTER TABLE "public"."seguradora_tipo_operacao" ALTER COLUMN "te_co_enquadramento" TYPE text USING convert_from("te_co_enquadramento", 'LATIN1');

-- sequencia_agenda_pagamento (2 colunas)
ALTER TABLE "public"."sequencia_agenda_pagamento" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');
ALTER TABLE "public"."sequencia_agenda_pagamento" ALTER COLUMN "te_obs" TYPE text USING convert_from("te_obs", 'LATIN1');

-- sistarq (1 colunas)
ALTER TABLE "public"."sistarq" ALTER COLUMN "te_observacao_arquivo" TYPE text USING convert_from("te_observacao_arquivo", 'LATIN1');

-- sitesp (1 colunas)
ALTER TABLE "public"."sitesp" ALTER COLUMN "te_observacao_sitesp" TYPE text USING convert_from("te_observacao_sitesp", 'LATIN1');

-- sitesp_excluido (1 colunas)
ALTER TABLE "public"."sitesp_excluido" ALTER COLUMN "te_observacao_sitesp" TYPE text USING convert_from("te_observacao_sitesp", 'LATIN1');

-- status (1 colunas)
ALTER TABLE "public"."status" ALTER COLUMN "te_descricao_status" TYPE text USING convert_from("te_descricao_status", 'LATIN1');

-- status_finaliza_tarefa (1 colunas)
ALTER TABLE "public"."status_finaliza_tarefa" ALTER COLUMN "te_condicao_finalizacao" TYPE text USING convert_from("te_condicao_finalizacao", 'LATIN1');

-- tabadv (1 colunas)
ALTER TABLE "public"."tabadv" ALTER COLUMN "te_pg_reneg" TYPE text USING convert_from("te_pg_reneg", 'LATIN1');

-- tablcobr (1 colunas)
ALTER TABLE "public"."tablcobr" ALTER COLUMN "email_mensagem" TYPE text USING convert_from("email_mensagem", 'LATIN1');

-- tabsit (1 colunas)
ALTER TABLE "public"."tabsit" ALTER COLUMN "detalhamentosituacaonaweb" TYPE text USING convert_from("detalhamentosituacaonaweb", 'LATIN1');

-- tabsitcj (1 colunas)
ALTER TABLE "public"."tabsitcj" ALTER COLUMN "detalhamentosituacaonaweb" TYPE text USING convert_from("detalhamentosituacaonaweb", 'LATIN1');

-- tab_avisos_e_notific_gerados (2 colunas)
ALTER TABLE "public"."tab_avisos_e_notific_gerados" ALTER COLUMN "te_atraso" TYPE text USING convert_from("te_atraso", 'LATIN1');
ALTER TABLE "public"."tab_avisos_e_notific_gerados" ALTER COLUMN "te_dados" TYPE text USING convert_from("te_dados", 'LATIN1');

-- tab_cartorio (3 colunas)
ALTER TABLE "public"."tab_cartorio" ALTER COLUMN "te_doc_cartorio" TYPE text USING convert_from("te_doc_cartorio", 'LATIN1');
ALTER TABLE "public"."tab_cartorio" ALTER COLUMN "te_guia_itbi" TYPE text USING convert_from("te_guia_itbi", 'LATIN1');
ALTER TABLE "public"."tab_cartorio" ALTER COLUMN "te_titulo_aquisitivo" TYPE text USING convert_from("te_titulo_aquisitivo", 'LATIN1');

-- tarefa_padrao (2 colunas)
ALTER TABLE "public"."tarefa_padrao" ALTER COLUMN "no_comando_executar_inicializa" TYPE text USING convert_from("no_comando_executar_inicializa", 'LATIN1');
ALTER TABLE "public"."tarefa_padrao" ALTER COLUMN "te_descricao_tarefa_padrao" TYPE text USING convert_from("te_descricao_tarefa_padrao", 'LATIN1');

-- tarefa_periodica (1 colunas)
ALTER TABLE "public"."tarefa_periodica" ALTER COLUMN "te_resumo_geracao" TYPE text USING convert_from("te_resumo_geracao", 'LATIN1');

-- taxas (1 colunas)
ALTER TABLE "public"."taxas" ALTER COLUMN "te_msg" TYPE text USING convert_from("te_msg", 'LATIN1');

-- taxas_instrucoes (1 colunas)
ALTER TABLE "public"."taxas_instrucoes" ALTER COLUMN "desc_instrucoes" TYPE text USING convert_from("desc_instrucoes", 'LATIN1');

-- taxas_pretendente (1 colunas)
ALTER TABLE "public"."taxas_pretendente" ALTER COLUMN "te_observacao" TYPE text USING convert_from("te_observacao", 'LATIN1');

-- testemunha (2 colunas)
ALTER TABLE "public"."testemunha" ALTER COLUMN "descricao" TYPE text USING convert_from("descricao", 'LATIN1');
ALTER TABLE "public"."testemunha" ALTER COLUMN "te_procuracao" TYPE text USING convert_from("te_procuracao", 'LATIN1');

-- tipos_inf_adicionais_parecer (1 colunas)
ALTER TABLE "public"."tipos_inf_adicionais_parecer" ALTER COLUMN "te_inf_adicional" TYPE text USING convert_from("te_inf_adicional", 'LATIN1');

-- tipotaxa (2 colunas)
ALTER TABLE "public"."tipotaxa" ALTER COLUMN "te_fato_gerador" TYPE text USING convert_from("te_fato_gerador", 'LATIN1');
ALTER TABLE "public"."tipotaxa" ALTER COLUMN "te_msg" TYPE text USING convert_from("te_msg", 'LATIN1');

-- tipo_atendimento (1 colunas)
ALTER TABLE "public"."tipo_atendimento" ALTER COLUMN "te_tipo_atendimento" TYPE text USING convert_from("te_tipo_atendimento", 'LATIN1');

-- tipo_enquadramento (1 colunas)
ALTER TABLE "public"."tipo_enquadramento" ALTER COLUMN "te_detalhamento" TYPE text USING convert_from("te_detalhamento", 'LATIN1');

-- tipo_unidade_operacao (1 colunas)
ALTER TABLE "public"."tipo_unidade_operacao" ALTER COLUMN "te_tipo_unidade" TYPE text USING convert_from("te_tipo_unidade", 'LATIN1');

-- token (1 colunas)
ALTER TABLE "public"."token" ALTER COLUMN "te_token" TYPE text USING convert_from("te_token", 'LATIN1');

-- tramitacao_cetip (1 colunas)
ALTER TABLE "public"."tramitacao_cetip" ALTER COLUMN "te_detalhe" TYPE text USING convert_from("te_detalhe", 'LATIN1');

-- tramitacao_fcvs (1 colunas)
ALTER TABLE "public"."tramitacao_fcvs" ALTER COLUMN "te_detalhe" TYPE text USING convert_from("te_detalhe", 'LATIN1');

-- tramitacao_juridico (1 colunas)
ALTER TABLE "public"."tramitacao_juridico" ALTER COLUMN "te_detalhe" TYPE text USING convert_from("te_detalhe", 'LATIN1');

-- tramitacao_processo_fisico (1 colunas)
ALTER TABLE "public"."tramitacao_processo_fisico" ALTER COLUMN "te_justificativa_solicitacao" TYPE text USING convert_from("te_justificativa_solicitacao", 'LATIN1');

-- tramite_rnv (1 colunas)
ALTER TABLE "public"."tramite_rnv" ALTER COLUMN "obs" TYPE text USING convert_from("obs", 'LATIN1');

-- variavel_originacao (1 colunas)
ALTER TABLE "public"."variavel_originacao" ALTER COLUMN "te_variavel" TYPE text USING convert_from("te_variavel", 'LATIN1');

-- variavel_tarefa (2 colunas)
ALTER TABLE "public"."variavel_tarefa" ALTER COLUMN "te_valorvariaveispossiveis" TYPE text USING convert_from("te_valorvariaveispossiveis", 'LATIN1');
ALTER TABLE "public"."variavel_tarefa" ALTER COLUMN "te_variavel" TYPE text USING convert_from("te_variavel", 'LATIN1');

-- Fim do script
-- 362 colunas convertidas de bytea para text