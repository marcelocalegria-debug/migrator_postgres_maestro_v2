#!/bin/bash
# Script Backup FISICO ONLINE com pg_backup_star/stop Postgres 2025
# Marcelo Alegria - 12/10/2025
# Log alteracoes:

alias gzip='gzip -9'

export varDataHora="`date +%F\_%R`"
export varDiaMes="`date +%d%m`"
export varDataHoraInicio="`date +%F' '%T`"
START_TIME=$SECONDS

# nome diretorio bkp
export varDATALABEL=`date '+%Y_%m_%d_%H_%M'`
export varPORTA=5432
export varDIR_BKP_LOG=/backup/Log
export varArquivoLogBackup=Backup_full_online_${varDATALABEL}.log


# origem bckup
export varDIR_PGDATA=/var/lib/postgresql/18/main
export varDIR_TABLESPACES=/database/tablespaces/18/main
export varDIR_ARCHIVE=/backup/archive/18/main
export varCONFDIR=/etc/postgresql/18/main
## destino destino backup
export varDESTBKP=/backup/backup_fisico_online
export varBACKUPDIR=$varDESTBKP/BKP_FISICO_ONLINE_PITR_$varDATALABEL


# Apago o arquivo de log do job de backup
rm -f $varDIR_BKP_LOG/Backup_fisico_online_pitr.log     
rm -f $varBACKUPDIR/Backup_fisico_online_pitr.log  

# LIMPA O BACKUP ANTERIOR    
rm -rf $varBACKUPDIR/*.tar.gz                                                                                 2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log

# ATENCAO - EXPURGO CONFIGURAR OS DIAS DE EXPURGO DOS BACKUPS ANTIGOS
find ${varDESTBKP} -name "BKP_FISICO_ONLINE_PITR*" -ctime 30 -print0 | xargs -0 -I {} rm -rf {}               2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log

# Cria o diretorio de backup
mkdir -p $varBACKUPDIR                                                                                        2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
mkdir -p $varDIR_BKP_LOG                                                                                      2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log

echo "INICIANDO BACKUP ONLINE_PITR DA INTANCIA POSTGRES!!! "  `date +%F\ %r`                                  2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log


echo "Fazendo a troca de logarchive.."                                      2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log

# Forçar a troca e archivamento de REDOLOG
psql -p ${varPORTA} -c "select pg_switch_wal();"                            2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
psql -p ${varPORTA} -c "select pg_switch_wal();"                            2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
psql -p ${varPORTA} -c "select pg_switch_wal();"                            2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log

echo "Colocando o banco em Modo Backup (EXCLUSIVE).."
echo "Colocando o banco em Modo Backup (EXCLUSIVE).."                       2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log

# Coloca o banco em modo de Backup exclusivo
psql -p ${varPORTA} -a <<EOF                                                2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
select pg_backup_start('BackupFullOnline');     
\! echo "Gerando o tar.gz do banco no diretorio $BACKUPDIR"                                             2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log                            
\! echo "DIRETORIO LOCAL DE DETINO DO BAKCUP: ${varBACKUPDIR} "                                         2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
\! tar -czvf ${varBACKUPDIR}/BackupPGDATA.tar.gz     --verbose ${varDIR_PGDATA}/*        2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
\! tar -czvf ${varBACKUPDIR}/BackupDATABASE.tar.gz   --verbose ${varDIR_TABLESPACES}/*   2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
-- \! #tar -czvf ${varBACKUPDIR}/BackupArchive.tar.gz    --verbose ${varDIR_ARCHIVE}/*       2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
\! tar -czvf ${varBACKUPDIR}/BackupConfig.tar.gz     --verbose ${varCONFDIR}/*           2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
\! echo "Tirando o banco do MODO BACKUP"
\! 
SELECT * FROM pg_backup_stop();                                                                         
EOF


# Forçar a troca e archivamento de REDOLOG
psql -p ${varPORTA} -c "select pg_switch_wal();"                            2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log

# LIMPA OS ARCHIVES EXCETO O ULTIMO (LAST_LOG), POIS JÁ FORAM LEVADOS NO BACKUP FULL - 
# Esta variavel foi carregada antes do backup tar dos archives, pois caso houvesse 2 trocas ou mais de archives durante o backup, o penúltimo archive gerado
# seria apagado e não levado no backupeado (correção em 17/01/2019)
LAST_LOG=$(cd ${varDIR_ARCHIVE} ; ls -rt *.backup | tail -1)

echo "Fazendo o TAR dos arquivos do banco para o backup"                   2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log

# Fazer o backup dos archives
tar --ignore-failed-read -cvzf  ${varBACKUPDIR}/BackupArchive.tar.gz  ${varBACKUPDIR}  2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log

if test $? -le 1
then
  echo "Backup Archives efetuado $date com sucesso"                  2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
else
  echo "Erro no processo de backup de Archive $? "                   2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
  tail $varDIR_BKP_LOG/Backup_fisico_online_pitr.log                 2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
  rm -f $FILE_BACKUP_EM_ANDAMENTO                                    
  exit 1
fi

# LIMPA OS ARCHIVES EXCETO O ULTIMO (LAST_LOG), POIS JÁ FORAM LEVADOS NO BACKUP FULL 
echo "pg_archivecleanup ${varDIR_ARCHIVE} ${LAST_LOG}  "
pg_archivecleanup ${varDIR_ARCHIVE} ${LAST_LOG}                       2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
erro=$?
if test $erro -eq 0
then
  echo "BackupBase efetuado $date com sucesso"                       2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
else
  echo "Erro no processo de ArchiveCleanup. "                        2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
  tail $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
  rm -f $FILE_BACKUP_EM_ANDAMENTO
  exit 1
fi

# Apago o arquivo que diz que o backup está em excucao
rm -f $FILE_BACKUP_EM_ANDAMENTO                                      2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log

grep -n ERROR $varDIR_BKP_LOG/Backup_fisico_online_pitr.log          2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log

ELAPSED_TIME=$(($SECONDS - $START_TIME))

echo "#### Estatisticas do Job de Backup:"                                                      2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
echo "**************************************************"                                       2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
echo "HORA INICIO               : " $varDataHoraInicio                                          2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
echo "TEMPO DE EXECUCAO (min)   : " $(( $ELAPSED_TIME/60))                                      2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
echo "TAMANHO DO BACKUP EM DISCO: " $(du -sh $BACKUPDIR | awk '{print $1}')                     2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
echo "**************************************************"                                       2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log
echo ""                                                                                         2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pitr.log

cat $varDIR_BKP_LOG/Backup_fisico_online_pitr.log | tee -a $varDIR_BKP_LOG/Backup_fisico_online_pit_historico.log
cp $varDIR_BKP_LOG/Backup_fisico_online_pitr.log ${varBACKUPDIR}

exit 0
