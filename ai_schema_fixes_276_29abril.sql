```sql
/* ========================================
   SCRIPT CORRETIVO: AJUSTE DE ESTRUTURA FIREBIRD → POSTGRES
   Projeto: MIGRACAO_0002
   ======================================== */

-- ============================================================
-- BLOCO 1: ADIÇÃO DE COLUNAS FALTANTES
-- ============================================================

/* Adiciona colunas ausentes identificadas na comparação.
   Tipo inferido como VARCHAR(50) genérico - AJUSTAR conforme metadados FB. */

ALTER TABLE imovel_terreno ADD COLUMN IF NOT EXISTS co_situacao_garantia VARCHAR(50);
ALTER TABLE imovel_terreno ADD COLUMN IF NOT EXISTS codarea VARCHAR(50);
ALTER TABLE imovel_terreno ADD COLUMN IF NOT EXISTS nu_pavimentos INTEGER;
ALTER TABLE imovel_terreno ADD COLUMN IF NOT EXISTS in_imovel_novo_usado CHAR(1);
ALTER TABLE imovel_terreno ADD COLUMN IF NOT EXISTS co_condicao_imovel VARCHAR(50);

ALTER TABLE cadastro ADD COLUMN IF NOT EXISTS cad6_co_situacao_garantia VARCHAR(50);
ALTER TABLE cadastro ADD COLUMN IF NOT EXISTS cad_dt3av DATE;
ALTER TABLE cadastro ADD COLUMN IF NOT EXISTS cad_dt3avbk DATE;
ALTER TABLE cadastro ADD COLUMN IF NOT EXISTS cad_dt4av DATE;
ALTER TABLE cadastro ADD COLUMN IF NOT EXISTS cad_dt4avbk DATE;
ALTER TABLE cadastro ADD COLUMN IF NOT EXISTS financ3_txjurosminima NUMERIC(15,4);
ALTER TABLE cadastro ADD COLUMN IF NOT EXISTS ndias3aviso INTEGER;
ALTER TABLE cadastro ADD COLUMN IF NOT EXISTS ndias4aviso INTEGER;
ALTER TABLE cadastro ADD COLUMN IF NOT EXISTS sitcobj3aviso VARCHAR(50);
ALTER TABLE cadastro ADD COLUMN IF NOT EXISTS sitcobj4aviso VARCHAR(50);

ALTER TABLE programa ADD COLUMN IF NOT EXISTS in_prog_classe_media CHAR(1);
ALTER TABLE campo_formulario ADD COLUMN IF NOT EXISTS te_expressao_exibicao TEXT;
ALTER TABLE fornecedor ADD COLUMN IF NOT EXISTS nu_cnpj VARCHAR(18);
ALTER TABLE fatura_item ADD COLUMN IF NOT EXISTS data_ent_est3 DATE;
ALTER TABLE fatura_item ADD COLUMN IF NOT EXISTS valor_bruto_est3 NUMERIC(15,2);
ALTER TABLE documento_operacao ADD COLUMN IF NOT EXISTS temp12_va_subsidio NUMERIC(15,2);
ALTER TABLE parcela ADD COLUMN IF NOT EXISTS va_saldopar NUMERIC(15,2);

-- ============================================================
-- BLOCO 2: AJUSTE DE FOREIGN KEYS - ON UPDATE CASCADE
-- ============================================================

/* Firebird usa ON UPDATE CASCADE por padrão; Postgres criou como NO ACTION.
   Recriar FKs com regra correta. */

-- FK: acao_tarefa_padrao (nome → acao)
ALTER TABLE acao_tarefa_padrao DROP CONSTRAINT IF EXISTS fk_acao_tarefa_padrao_nome;
ALTER TABLE acao_tarefa_padrao ADD CONSTRAINT fk_acao_tarefa_padrao_nome 
  FOREIGN KEY (nome) REFERENCES acao(nome) ON DELETE CASCADE ON UPDATE CASCADE;

-- FK: acao_tarefa_padrao (co_tarefa_padrao → tarefa_padrao)
ALTER TABLE acao_tarefa_padrao DROP CONSTRAINT IF EXISTS fk_acao_tarefa_padrao_tarefa;
ALTER TABLE acao_tarefa_padrao ADD CONSTRAINT fk_acao_tarefa_padrao_tarefa 
  FOREIGN KEY (co_tarefa_padrao) REFERENCES tarefa_padrao(co_tarefa_padrao) ON DELETE CASCADE ON UPDATE CASCADE;

-- FK: fase_operacao (nu_fase_cancelamento)
ALTER TABLE fase_operacao DROP CONSTRAINT IF EXISTS fk_fase_operacao_cancelamento;
ALTER TABLE fase_operacao ADD CONSTRAINT fk_fase_operacao_cancelamento 
  FOREIGN KEY (nu_fase_cancelamento) REFERENCES fase_operacao(nu_fase_operacao) ON DELETE RESTRICT ON UPDATE CASCADE;

-- FK: rua_projeto (nu_projeto, nu_rua)
ALTER TABLE rua_projeto DROP CONSTRAINT IF EXISTS fk_rua_projeto_projeto;
ALTER TABLE rua_projeto ADD CONSTRAINT fk_rua_projeto_projeto 
  FOREIGN KEY (nu_projeto) REFERENCES projeto(nu_projeto) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE rua_projeto DROP CONSTRAINT IF EXISTS fk_rua_projeto_rua;
ALTER TABLE rua_projeto ADD CONSTRAINT fk_rua_projeto_rua 
  FOREIGN KEY (nu_rua) REFERENCES rua(nu_rua) ON DELETE CASCADE ON UPDATE CASCADE;

-- FK: fonte_recurso (co_recurso)
ALTER TABLE fonte_recurso DROP CONSTRAINT IF EXISTS fk_fonte_recurso_recurso;
ALTER TABLE fonte_recurso ADD CONSTRAINT fk_fonte_recurso_recurso 
  FOREIGN KEY (co_recurso) REFERENCES recurso(nu_recurso) ON DELETE RESTRICT ON UPDATE CASCADE;

-- FK: fornecedor (nu_fornecedor)
ALTER TABLE obra DROP CONSTRAINT IF EXISTS fk_obra_fornecedor;
ALTER TABLE obra ADD CONSTRAINT fk_obra_fornecedor 
  FOREIGN KEY (nu_fornecedor) REFERENCES fornecedor(nu_fornecedor) ON DELETE SET NULL ON UPDATE CASCADE;

-- FK: obra (nu_obra)
ALTER TABLE contrato_obra DROP CONSTRAINT IF EXISTS fk_contrato_obra_obra;
ALTER TABLE contrato_obra ADD CONSTRAINT fk_contrato_obra_obra 
  FOREIGN KEY (nu_obra) REFERENCES obra(nu_obra) ON DELETE CASCADE ON UPDATE CASCADE;

-- FK: contrato_obra (nu_contrato)
ALTER TABLE contrato_obra DROP CONSTRAINT IF EXISTS fk_contrato_obra_contrato;
ALTER TABLE contrato_obra ADD CONSTRAINT fk_contrato_obra_contrato 
  FOREIGN KEY (nu_contrato) REFERENCES contrato(nu_contrato) ON DELETE CASCADE ON UPDATE CASCADE;

-- FK: imovel_terreno (nu_imovel) - múltiplas tabelas
ALTER TABLE registro_geral_imovel DROP CONSTRAINT IF EXISTS fk_registro_geral_imovel;
ALTER TABLE registro_geral_imovel ADD CONSTRAINT fk_registro_geral_imovel 
  FOREIGN KEY (nu_imovel) REFERENCES imovel_terreno(nu_imovel) ON DELETE CASCADE ON UPDATE CASCADE;

-- FK: lote_projeto (nu_projeto, nu_lote, nu_quadra)
ALTER TABLE lote_projeto DROP CONSTRAINT IF EXISTS fk_lote_projeto_projeto;
ALTER TABLE lote_projeto ADD CONSTRAINT fk_lote_projeto_projeto 
  FOREIGN KEY (nu_projeto) REFERENCES projeto(nu_projeto) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE lote_projeto DROP CONSTRAINT IF EXISTS fk_lote_projeto_lote;
ALTER TABLE lote_projeto ADD CONSTRAINT fk_lote_projeto_lote 
  FOREIGN KEY (nu_lote) REFERENCES lote(nu_lote) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE lote_projeto DROP CONSTRAINT IF EXISTS fk_lote_projeto_quadra;
ALTER TABLE lote_projeto ADD CONSTRAINT fk_lote_projeto_quadra 
  FOREIGN KEY (nu_quadra) REFERENCES quadra(nu_quadra) ON DELETE CASCADE ON UPDATE CASCADE;

-- FK: fatura (nu_fatura)
ALTER TABLE fatura_item DROP CONSTRAINT IF EXISTS fk_fatura_item_fatura;
ALTER TABLE fatura_item ADD CONSTRAINT fk_fatura_item_fatura 
  FOREIGN KEY (nu_fatura) REFERENCES fatura(nu_fatura) ON DELETE CASCADE ON UPDATE CASCADE;

-- FK: subprograma (nu_programa, co_subprograma)
ALTER TABLE documento_operacao DROP CONSTRAINT IF EXISTS fk_documento_operacao_programa;
ALTER TABLE documento_operacao ADD CONSTRAINT fk_documento_operacao_programa 
  FOREIGN KEY (nu_programa) REFERENCES subprograma(nu_programa) ON DELETE SET NULL ON UPDATE CASCADE;

ALTER TABLE documento_operacao DROP CONSTRAINT IF EXISTS fk_documento_operacao_subprograma;
ALTER TABLE documento_operacao ADD CONSTRAINT fk_documento_operacao_subprograma 
  FOREIGN KEY (co_subprograma) REFERENCES subprograma(co_subprograma) ON DELETE SET NULL ON UPDATE CASCADE;

-- FK: documento_operacao (nu_fase_atual)
ALTER TABLE documento_operacao DROP CONSTRAINT IF EXISTS fk_documento_operacao_fase_atual;
ALTER TABLE documento_operacao ADD CONSTRAINT fk_documento_operacao_fase_atual 
  FOREIGN KEY (nu_fase_atual) REFERENCES fase_operacao(nu_fase_operacao) ON DELETE RESTRICT ON UPDATE CASCADE;

-- FK: programa (nu_programa)
ALTER TABLE programa DROP CONSTRAINT IF EXISTS fk_programa_programa;
ALTER TABLE programa ADD CONSTRAINT fk_programa_programa 
  FOREIGN KEY (nu_programa) REFERENCES programa(nu_programa) ON DELETE CASCADE ON UPDATE CASCADE;

-- FK: tipo_tributo (co_tributo)
ALTER TABLE tipo_tributo DROP CONSTRAINT IF EXISTS fk_tipo_tributo_tributo;
ALTER TABLE tipo_tributo ADD CONSTRAINT fk_tipo_tributo_tributo 
  FOREIGN KEY (co_tributo) REFERENCES tributo(co_tributo) ON DELETE RESTRICT ON UPDATE CASCADE;

-- FK: usuario (co_usuario_entregou)
ALTER TABLE entrega_documento DROP CONSTRAINT IF EXISTS fk_entrega_documento_usuario;
ALTER TABLE entrega_documento ADD CONSTRAINT fk_entrega_documento_usuario 
  FOREIGN KEY (co_usuario_entregou) REFERENCES usuario(usuario) ON DELETE NO ACTION ON UPDATE CASCADE;

/* ========================================
   FIM DO SCRIPT CORRETIVO
   ======================================== */
```