#!/bin/bash
# Script Dinamico Restore backup fisico OFFLINE 
# Marcelo Alegria - 12/10/2025
# Ajustado por:
# Log alteracoes
# restore_backup_fisico_offline.sh


alias gzip='gzip -9'

varDataHora="`date +%F\_%R`"
varDiaMes="`date +%d%m`"
varDataHoraInicio="`date +%F' '%T`"
START_TIME=$SECONDS

# nome diretorio bkp
varDATALABEL=`date '+%Y_%m_%d_%H_%M'`
varPORTA=5432
varDIR_BKP_LOG=/backup/Log
varArquivoLogRestore=Restore_full_offline_${varDATALABEL}.log


# destino do restore
varDIR_PGDATA=/var/lib/postgresql/18/main
varDIR_TABLESPACES=/database/tablespaces/18/main
varDIR_ARCHIVE=/backup/archive/18/main
varCONFDIR=/etc/postgresql/18/main


## origem do restore
varDESTBKP=/backup/backup_fisico_offline



## PAREI AQUI - FALTA PARAMETRIZAR
varRESTOREDIR=/backup/backup_fisico_offline/BKP_FISICO_OFFLINE_2025_10_12_14_45


# Apago o arquivo de log do job de backup
rm -f $varDIR_BKP_LOG/Restore_backup_fisico_offline.log     


echo "COLOCANDO O BANCO OFFLINE DA INTANCIA POSTGRES!! "  `date +%F\ %r`                                  2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log

### TIRA O BANCO DO AR
##sudo systemctl stop postgresql@18-main.service                                                          2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
##sudo systemctl status postgresql@18-main.service                                                        2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
# Alterado em 12/10 para não precisar usar o sudo para root                                               
#/usr/lib/postgresql/18/bin/pg_ctl stop   -D  ${varDIR_PGDATA}                                            2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
#/usr/lib/postgresql/18/bin/pg_ctl status -D  ${varDIR_PGDATA}                                            2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
/usr/bin/pg_ctlcluster 18 main stop                                                                       2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
/usr/bin/pg_ctlcluster 18 main  status                                                                     2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log

echo "ATENCAO - INSTANCIA SHUTDOWN !!! "  `date +%F\ %r`                                                  2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log

echo "APAGANDO OS DADOS DA INSTANCIA ATUAL "                                                              2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
## PAREI AQUI - FALTA COLOCAR UM PROMPT CONFIRMANDO
rm -rf /var/lib/postgresql/18/main/*                                                                                2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
rm -rf /database/tablespaces/18/main/*                                                                    2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log



# restaurando backup OFFLINE BASE nos diretorios do cluster

echo "Restaurando dados e tablespaces.. (nao inclui archiveWAL nem conf!) "  `date +%F\ %r`               2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log

tar -zxvf ${varRESTOREDIR}/BackupPGDATA.tar.gz     -C /                                                   2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
tar -zxvf ${varRESTOREDIR}/BackupDATABASE.tar.gz   -C /                                                   2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log


echo "COLOCANDO O BANCO ONLINE POSTGRES!! "  `date +%F\ %r`                                               2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
																				      
# COLOCA O BANCO NO AR                                                                                    
#sudo systemctl start postgresql@18-main.service                                                          2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
#sudo systemctl status postgresql@18-main.service                                                         2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
# Alterado em 12/10 para não precisar usar o sudo para root
#/usr/lib/postgresql/18/bin/pg_ctl start  -D  ${varDIR_PGDATA}                                            2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
#/usr/lib/postgresql/18/bin/pg_ctl status -D  ${varDIR_PGDATA}                                            2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
/usr/bin/pg_ctlcluster 18 main start                                                                      2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
/usr/bin/pg_ctlcluster 18 main status                                                                     2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log

echo "ATENCAO - BANCO JA ESTA NO AR !!! " `date +%F\ %r`                                                  2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log

cp $varDIR_BKP_LOG/Restore_backup_fisico_offline.log ${varRESTOREDIR}

ELAPSED_TIME=$(($SECONDS - $START_TIME))
echo "#### Estatisticas do Job de Restore:"                                                               2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
echo "**************************************************"                                                 2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
echo "HORA INICIO               : " $varDataHoraInicio                                                    2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
echo "TEMPO DE EXECUCAO (min)   : " $(($ELAPSED_TIME/60))                                                 2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
echo "TAMANHO DO RESTORE EM DISCO: " $(du -sh $varRESTOREDIR | awk '{print $1}')                          2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
echo "**************************************************"                                                 2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log
echo ""
echo "#################### Fim do Restore Fisico Offline no servidor $HOSTNAME :  `date` "                2>&1 | tee -a $varDIR_BKP_LOG/Restore_backup_fisico_offline.log

cat $varDIR_BKP_LOG/Restore_backup_fisico_offline.log >> $varDIR_BKP_LOG/Restore_fisico_offline_historico.log

exit 0


