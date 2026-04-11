#!/bin/bash
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
# 1. Configurações Iniciais
#DIRETORIO_BASE="/migracao_firebird"
DIRETORIO_BASE="/migracao_firebird"
TABELA=$1

# 2. Definição do arquivo de Log (NomeTabela_DataHora.log)
DATA_HORA=$(date +"%d%m%y_%H%M%S")

# Cria a pasta de logs se não existir
mkdir -p "${DIRETORIO_BASE}/logs"

# 3. Execução dos Comandos
{
    echo "Iniciando migração da tabela: DOCUMENTO_OPERACAO"
    echo "Data: $(date)"
    echo "------------------------------------------"

    cd "$DIRETORIO_BASE" || exit
    
    # Ativa o ambiente virtual
    . .venv/Scripts/activate
    
    # Força codificação UTF-8
    export PYTHONIOENCODING=utf-8

    echo "[PASSO 1] Rodando Migrator..."
    python migrator_parallel_doc_oper_v2.py --threads 4

    echo -e "\n[PASSO 2] Rodando Comparação de Checksum..."
    python PosMigracao_comparaChecksum_bytea.py --table "DOCUMENTO_OPERACAO"

    echo "------------------------------------------"
    echo "Finalizado em: $(date)"

} 
