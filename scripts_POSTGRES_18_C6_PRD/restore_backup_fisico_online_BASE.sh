#!/bin/bash
# Script Dinamico Restore backup fisico ONLINE BASE
# Marcelo Alegria - 12/10/2025
# Ajustado por:
# Log alteracoes
# restore_backup_fisico_online_BASE.sh


alias gzip='gzip -9'

varDataHora="`date +%F\_%R`"
varDiaMes="`date +%d%m`"
varDataHoraInicio="`date +%F' '%T`"
START_TIME=$SECONDS

# nome diretorio bkp
varDATALABEL=`date '+%Y_%m_%d_%H_%M'`
varPORTA=5432
varDIR_BKP_LOG=/backup/Log
varArquivoLogRestore=Restore_full_online_${varDATALABEL}.log


# destino do restore
varDIR_PGDATA=/var/lib/postgresql/18/main
varDIR_TABLESPACES=/database/tablespaces/18/main
varDIR_ARCHIVE=/backup/archive/18/main
varCONFDIR=/etc/postgresql/18/main


## origem do restore
varDESTBKP=/backup/backup_fisico_online
## PAREI AQUI - FALTA PARAMETRIZAR
varRESTOREDIR=/backup/backup_fisico_online/BKP_FISICO_ONLINE_BASE_2025_10_12_15_34


# Apago o arquivo de log do job de backup
rm -f $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log     


echo "COLOCANDO O BANCO OFFLINE DA INTANCIA POSTGRES!! "  `date +%F\ %r`                      2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log

### TIRA O BANCO DO AR
##sudo systemctl stop postgresql@18-main.service                                              2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
##sudo systemctl status postgresql@18-main.service                                            2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
# Alterado em 12/10 para não precisar usar o sudo para root                                   
#/usr/lib/postgresql/18/bin/pg_ctl stop   -D  ${varDIR_PGDATA}                                2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
#/usr/lib/postgresql/18/bin/pg_ctl status -D  ${varDIR_PGDATA}                                2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
/usr/bin/pg_ctlcluster 18 main stop                                                           2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
/usr/bin/pg_ctlcluster 18 main status                                                         2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log

echo "ATENCAO - INSTANCIA SHUTDOWN !!! "  `date +%F\ %r`                                      2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log

echo "APAGANDO OS DADOS DA INSTANCIA ATUAL "                                                  2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
## PAREI AQUI - FALTA COLOCAR UM PROMPT CONFIRMANDO
rm -rf /var/lib/postgresql/18/main/*                                                                    2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
rm -rf /database/tablespaces/18/main/*                                                        2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log

# restaurando backup ONLINE BASE nos diretorios do cluster
mkdir -p /database/tablespaces/18/main/bbc      
mkdir -p /database/tablespaces/18/main/bbc_idx	 

tar -zxvf ${varRESTOREDIR}/base.tar.gz   -C ${varDIR_PGDATA}                                  2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
tar -zxvf ${varRESTOREDIR}/pg_wal.tar.gz -C ${varDIR_PGDATA}/pg_wal                           2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
tar -zxvf ${varRESTOREDIR}/18450.tar.gz  -C ${varDIR_TABLESPACES}/bbc                         2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
tar -zxvf ${varRESTOREDIR}/18451.tar.gz  -C ${varDIR_TABLESPACES}/bbc_idx                     2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log


# COLOCA O BANCO NO AR                                                                        
#sudo systemctl start postgresql@18-main.service                                              2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
#sudo systemctl status postgresql@18-main.service                                             2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
# Alterado em 12/10 para não precisar usar o sudo para root
#/usr/lib/postgresql/18/bin/pg_ctl start  -D  ${varDIR_PGDATA}                                2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
#/usr/lib/postgresql/18/bin/pg_ctl status -D  ${varDIR_PGDATA}                                2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
/usr/bin/pg_ctlcluster 18 main start                                                          2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
/usr/bin/pg_ctlcluster 18 main status                                                         2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log

echo "ATENCAO - BANCO JA ESTA NO AR !!! " `date +%F\ %r`                                      2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log

cp $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log ${varRESTOREDIR}

ELAPSED_TIME=$(($SECONDS - $START_TIME))
echo "#### Estatisticas do Job de Restore:"                                                   2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
echo "**************************************************"                                     2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
echo "HORA INICIO               : " $varDataHoraInicio                                        2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
echo "TEMPO DE EXECUCAO (min)   : " $(($ELAPSED_TIME/60))                                     2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
echo "TAMANHO DO RESTORE EM DISCO: " $(du -sh $varRESTOREDIR | awk '{print $1}')              2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
echo "**************************************************"                                     2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log
echo ""
echo "#################### Fim do Restore Fisico Offline no servidor $HOSTNAME :  `date` "    2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log

cat $varDIR_BKP_LOG/Restore_backup_fisico_online_BASE.log >> $varDIR_BKP_LOG/Restore_fisico_offline_historico.log


exit 0


