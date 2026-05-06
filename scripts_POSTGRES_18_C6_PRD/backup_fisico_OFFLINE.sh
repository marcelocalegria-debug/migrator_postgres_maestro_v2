#!/bin/bash
# Script Dinamico Backup COLD - POSTGRES
# Marcelo Alegria - 04/12/2018
# Ajustado por Marcelo Alegria em 23/12/2023 para a versão 18
# Log alteraçs
# Backup_fisico_OFFLINE.sh


alias gzip='gzip -9'

varDataHora="`date +%F\_%R`"
varDiaMes="`date +%d%m`"
varDataHoraInicio="`date +%F' '%T`"
START_TIME=$SECONDS

# nome diretorio bkp
varDATALABEL=`date '+%Y_%m_%d_%H_%M'`
varPORTA=5432
varDIR_BKP_LOG=/backup/Log
varArquivoLogBackup=Backup_full_offline_${varDATALABEL}.log


# origem bckup
varDIR_PGDATA=/var/lib/postgresql/18/main
varDIR_TABLESPACES=/database/tablespaces/18/main
varDIR_ARCHIVE=/backup/archive/18/main
varCONFDIR=/etc/postgresql/18/main
## destino destino backup
varDESTBKP=/backup/backup_fisico_offline
varBACKUPDIR=$varDESTBKP/BKP_FISICO_OFFLINE_$varDATALABEL


# Apago o arquivo de log do job de backup
rm -f $varDIR_BKP_LOG/Backup_fisico_offline.log     
rm -f $varBACKUPDIR/Backup_fisico_offline.log  

# LIMPA O BACKUP ANTERIOR    
rm -rf $varBACKUPDIR/*.tar.gz                                                                             2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log

# ATENCAO - EXPURGO CONFIGURAR OS DIAS DE EXPURGO DOS BACKUPS ANTIGOS
find ${varDESTBKP} -name "BKP_FISICO_OFFLINE*" -ctime 30 -print0 | xargs -0 -I {} rm -rf {}               2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log

# Cria o diretorio de backup
mkdir -p $varBACKUPDIR
mkdir -p $varDIR_BKP_LOG

echo "INICIANDO BACKUP OFFLINE DA INTANCIA POSTGRES!!! "  `date +%F\ %r`                                  2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log

### TIRA O BANCO DO AR
##sudo systemctl stop postgresql@18-main.service                                                          2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
##sudo systemctl status postgresql@18-main.service                                                        2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
# Alterado em 12/10 para não precisar usar o sudo para root                                               
#/usr/lib/postgresql/18/bin/pg_ctl stop   -D  ${varDIR_PGDATA}                                            2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
#/usr/lib/postgresql/18/bin/pg_ctl status -D  ${varDIR_PGDATA}                                            2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
/usr/bin/pg_ctlcluster 18 main stop                                                                       2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
/usr/bin/pg_ctlcluster 18 main status                                                                     2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log

echo "ATENCAO - INSTANCIA SHUTDOWN !!! "  `date +%F\ %r`                                                  2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log

# NOVO METODO COM GZIP                                     
echo "DIRETORIO LOCAL DE DETINO DO BAKCUP: ${varBACKUPDIR} "                                              2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
tar -czvf ${varBACKUPDIR}/BackupPGDATA.tar.gz     --verbose ${varDIR_PGDATA}/*                            2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
tar -czvf ${varBACKUPDIR}/BackupDATABASE.tar.gz   --verbose ${varDIR_TABLESPACES}/*                       2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
tar -czvf ${varBACKUPDIR}/BackupArchive.tar.gz    --verbose ${varDIR_ARCHIVE}/*                           2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
tar -czvf ${varBACKUPDIR}/BackupConfig.tar.gz     --verbose ${varCONFDIR}/*                               2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
																									      
																					      
# COLOCA O BANCO NO AR                                                                                    
#sudo systemctl start postgresql@18-main.service                                                          2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
#sudo systemctl status postgresql@18-main.service                                                         2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
# Alterado em 12/10 para não precisar usar o sudo para root
#/usr/lib/postgresql/18/bin/pg_ctl start  -D  ${varDIR_PGDATA}                                            2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
#/usr/lib/postgresql/18/bin/pg_ctl status -D  ${varDIR_PGDATA}                                            2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
/usr/bin/pg_ctlcluster 18 main start                                                                      2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
/usr/bin/pg_ctlcluster 18 main status                                                                     2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log

echo "ATENCAO - BANCO JA ESTA NO AR !!! " `date +%F\ %r`                                                  2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log

cp $varDIR_BKP_LOG/Backup_fisico_offline.log ${varBACKUPDIR}

ELAPSED_TIME=$(($SECONDS - $START_TIME))
echo "#### Estatisticas do Job de Backup:"                                                                2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
echo "**************************************************"                                                 2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
echo "HORA INICIO               : " $varDataHoraInicio                                                    2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
echo "TEMPO DE EXECUCAO (min)   : " $(($ELAPSED_TIME/60))                                                 2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
echo "TAMANHO DO BACKUP EM DISCO: " $(du -sh $varBACKUPDIR | awk '{print $1}')                            2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
echo "**************************************************"                                                 2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log
echo ""
echo "#################### Fim do Backup Fisico Offline no servidor $HOSTNAME :  `date` "                 2>&1 | tee -a $varDIR_BKP_LOG/Backup_fisico_offline.log

cat $varDIR_BKP_LOG/Backup_fisico_offline.log >> $varDIR_BKP_LOG/Backup_fisico_offline_historico.log


exit 0


