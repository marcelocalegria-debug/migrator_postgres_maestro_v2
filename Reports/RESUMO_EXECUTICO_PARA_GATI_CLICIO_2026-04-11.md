# Análise Técnica — 2026-04-11

## Contexto

Sessão de análise cobrindo dois temas principais:
1. Review de código dos 4 migrators (bugs identificados pelo Claude Opus / Gatti) → patches v2
2. Problema de mapeamento BLOB sub_type 0 → bytea e comparação entre os dois DDLs de schema

---

## Parte 1 — Review do Claude Opus: 6 Achados

O review identificou 6 problemas nos migrators. Todos foram validados contra código real e bancos locais.

### Quadro Final de Severidade

| # | Achado | Severidade | Status |
|---|--------|-----------|--------|
| 1 | Sub-batch perde segunda metade | **CRÍTICO** | Patched — todos os 4 migrators (8 métodos) |
| 2 | BLOB sub_type 1 vs bytea | ~~ALTO~~ → **RESSUSCITADO** | Ver Parte 2 abaixo |
| 3 | Restart sem PK é O(n) skip | ~~ALTO~~ → BAIXO | Mitigado — migrator_log_eventos já trata; CO_LOG é único |
| 4 | Encoding WIN1252 bytes 0x80–0x9F | ~~MÉDIO~~ | Descartado — zero ocorrências nas tabelas críticas |
| 5 | `_setup_logging` duplica handlers | BAIXO | Patched |
| 6 | `_restore_pg` silencia autovacuum | BAIXO | Patched |

### Fix #1 — Sub-batch perde segunda metade [CRÍTICO]

**Padrão do bug (antes):**
```python
mid = len(rows) // 2
if mid > 0:
    self._insert_copy(pg_conn, rows[:mid], batch_num)
    self._insert_copy(pg_conn, rows[mid:], batch_num)  # nunca executa se anterior raise
    return
```

**Padrão corrigido (depois):**
```python
mid = len(rows) // 2
if mid > 0:
    errors = []
    try:
        self._insert_copy(pg_conn, rows[:mid], batch_num)
    except Exception as e1:
        errors.append(('first_half', len(rows[:mid]), e1))
    try:
        self._insert_copy(pg_conn, rows[mid:], batch_num)
    except Exception as e2:
        errors.append(('second_half', len(rows[mid:]), e2))
    if errors:
        total_lost = sum(x[1] for x in errors)
        self.log.error(
            f'Batch {batch_num}: {len(errors)} sub-batch(es) falharam, '
            f'{total_lost} linhas afetadas: {errors}')
    return
```

Aplicado em `_insert_copy` e `_insert_values` nos 4 migrators.

### Fix #3 — Checkpoint por RDB$DB_KEY em migrator.py [BAIXO]

```python
# Antes:
elif self.progress.use_db_key:
    pass  # restart sem PK faz skip por contagem

# Depois:
elif self.progress.use_db_key:
    last = batch_rows[-1]
    if len(last) > len(self.columns):
        self.progress.last_db_key = last[-1]
```

### Fix #5 — `_setup_logging` duplica handlers [BAIXO]

```python
root = logging.getLogger()
for h in list(root.handlers):  # linha adicionada
    root.removeHandler(h)      # linha adicionada
root.setLevel(level)
```

### Fix #6 — `_restore_pg` silencia erros [BAIXO]

```python
# Antes:
except Exception:
    pass

# Depois:
except Exception as e:
    self.log.warning(f'Falha ao reabilitar autovacuum: {e}. Verificar manualmente.')
```

### Arquivos v2 Gerados

| Original | v2 | Fixes Aplicados |
|----------|-----|-----------------|
| `migrator.py` | `migrator_v2.py` | #1, #3, #5, #6 |
| `migrator_smalltables.py` | `migrator_smalltables_v2.py` | #1, #5, #6 |
| `migrator_parallel_doc_oper.py` | `migrator_parallel_doc_oper_v2.py` | #1, #6 |
| `migrator_log_eventos.py` | `migrator_log_eventos_v2.py` | #1, #6 |

**Nota:** `migrator_log_eventos_v2.py` não recebeu Fix #3 porque o arquivo original já salva `last_db_key` corretamente na linha 742.

---

## Parte 2 — Problema BLOB sub_type 0 → bytea

### Causa Raiz

O Firebird aceita silenciosamente texto em colunas declaradas como BLOB sub_type 0 (binário). A aplicação SCCI grava XML/texto puro nessas colunas há anos. O PostgreSQL é rigoroso: rejeita gravação de strings em colunas `bytea`, gerando 200+ erros na aplicação.

**Exemplo confirmado:** `nmov.te_campos` contém XML em texto puro, mas foi declarada como BLOB sub_type 0 no Firebird e mapeada como `bytea` no PostgreSQL.

### Varredura: 372 colunas BLOB sub_type 0 identificadas

- **362 para converter** para `text` (contêm texto: XML, texto livre, expressões, etc.)
- **10 para manter** como `bytea` (genuinamente binárias: imagens, tokens, dados brutos)

**Colunas mantidas como bytea:**

| Tabela | Coluna | Motivo |
|--------|--------|--------|
| controleversao | dado | Dado binário bruto de versão |
| controleversao | te_imagem_reduzida | Imagem |
| email_a_enviar | te_imagem_reduzida | Imagem |
| grupo_tipo_operacao | im_grupo_tipo_operacao | Imagem |
| imagem_documento_rgi | im_pagina_rgi | Imagem |
| scci_session | token | Token de autenticação |
| scci_session | refresh_token | Token de autenticação |
| segura | im_seguradora | Imagem |
| segura | im_seguradora_mini | Imagem |
| simulacao_originacao | im_enquadramento | Imagem |

### Script de Correção Gerado

**`fix_blob_to_text.sql`** — 362 ALTER TABLE statements

Padrão de cada ALTER:
```sql
ALTER TABLE "public"."nmov" ALTER COLUMN "te_campos" TYPE text
USING convert_from("te_campos", 'LATIN1');
```

O `convert_from(..., 'LATIN1')` é necessário porque os dados já estão no PostgreSQL como `bytea` com encoding WIN1252/ISO-8859-1 (charset do Firebird). `LATIN1` é compatível para decodificação.

**Script gerador:** `fix_blob_text_columns.py` — conecta ao Firebird, lista todas as colunas BLOB sub_type 0, exclui as binárias, gera o SQL.

> **IMPORTANTE:** O script ainda não foi aplicado ao banco de destino. Executar apenas quando validado:
> ```bash
> psql -h host -p 5435 -U postgres -d c6_alegria -f fix_blob_to_text.sql
> ```

---

## Parte 3 — Comparação dos Dois DDLs

### Os dois schemas comparados

| Arquivo | Origem | Colunas bytea |
|---------|--------|--------------|
| `c6_producao_cria_sql_ec2.sql` | Script de criação zerada do próprio SCCI | **371** |
| `c6_producao_pg_converter_equinix.sql` | Postgres Converter (ferramenta automática) | **372** |

### Resultado: Ambas as abordagens têm o mesmo problema

**371 colunas bytea são idênticas nos dois arquivos.** A única diferença é:

- `documento_grupo_documento.te_expressao_exibicao` — presente **só no PG Converter** (ausente no script SCCI)

Essa coluna já está coberta no `fix_blob_to_text.sql`.

### Conclusão

Nenhuma das duas ferramentas errou: ambas seguiram corretamente o metadata do Firebird (sub_type 0 = binário → bytea). O problema é estrutural — o Firebird é permissivo e aceita texto em campos binários, mas o PostgreSQL não.

**O `fix_blob_to_text.sql` deve ser aplicado independentemente de qual schema foi usado.**

| Schema utilizado | Colunas a converter |
|-----------------|-------------------|
| SCCI (ec2) | 362 (das 371 bytea) |
| PG Converter (equinix) | 362 (das 372 bytea — inclui te_expressao_exibicao) |

---

## Arquivos Gerados nesta Sessão

| Arquivo | Descrição |
|---------|-----------|
| `migrator_v2.py` | Migrator principal com fixes #1, #3, #5, #6 |
| `migrator_smalltables_v2.py` | Small tables com fixes #1, #5, #6 |
| `migrator_parallel_doc_oper_v2.py` | DOCUMENTO_OPERACAO com fixes #1, #6 |
| `migrator_log_eventos_v2.py` | LOG_EVENTOS com fixes #1, #6 |
| `fix_blob_to_text.sql` | 362 ALTERs bytea → text (não aplicado ainda) |
| `fix_blob_text_columns.py` | Gerador do script SQL acima |
| `CLAUDE.md` | Documentação do projeto para Claude Code |
