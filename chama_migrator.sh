#!/bin/bash
export LANG=C.UTF-8
export LC_ALL=C.UTF-8

# 1. Configurações Iniciais
#DIRETORIO_BASE="/migracao_firebird"
DIRETORIO_BASE="/migracao_firebird"
TABELA=$1

# Verifica se o parâmetro da tabela foi enviado
if [ -z "$TABELA" ]; then
    echo "Erro: Informe o nome da tabela. Ex: ./rodar_migracao.sh CONTROLEVERSAO"
    exit 1
fi

# 2. Definição do arquivo de Log (NomeTabela_DataHora.log)
DATA_HORA=$(date +"%d%m%y_%H%M%S")
ARQUIVO_LOG="${DIRETORIO_BASE}/logs/${TABELA}_${DATA_HORA}.log"

# Cria a pasta de logs se não existir
mkdir -p "${DIRETORIO_BASE}/logs"

# 3. Execução dos Comandos
{
    echo "Iniciando migração da tabela: ${TABELA}"
    echo "Data: $(date)"
    echo "------------------------------------------"

    cd "$DIRETORIO_BASE" || exit
    
    # Ativa o ambiente virtual
    . .venv/Scripts/activate
    
    # Força codificação UTF-8
    export PYTHONIOENCODING=utf-8

    echo "[PASSO 1] Rodando Migrator..."
    python migrator_v2.py --table "$TABELA"

    echo -e "\n[PASSO 2] Rodando Comparação de Checksum..."
    python PosMigracao_comparaChecksum_bytea.py --table "$TABELA"

    echo "------------------------------------------"
    echo "Finalizado em: $(date)"

} > "$ARQUIVO_LOG" 2>&1
