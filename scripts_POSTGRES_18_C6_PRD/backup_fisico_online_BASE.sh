#!/bin/bash
# Script Backup FISICO ONLINE com pg_basebackup Postgres 2025
# Marcelo Alegria - 10/10/2025
# Log alteracoes:

# DICAS
# Crie o usuario de backup no banco origem
# CREATE USER backup_user WITH REPLICATION ENCRYPTED PASSWORD 'ae8KFEalsdf';
#
# Ajuste o pg_hba.conf e faça o reload da configuracao

#   # Allow replication connections from localhost, by a user with the
#   # replication privilege.
#   #local   replication     all                                     peer
#   #host    replication     all             127.0.0.1/32            ident
#   #host    replication     all             ::1/128                 ident
#   host     replication     backup_user     ::1/128                 trust
##

# TIPS por RECOVERY - https://stormatics.tech/blogs/postgresql-physical-backups-using-pg_basebackup-a-comprehensive-guide
#restore_command = 'cp /mnt/server/archivedir/%f %p'
#recovery_target_time = '2023-06-14 12:59:59.319298'
# touch /var/lib/pgsql/12/data/recovery.signal



varDiaMes="`date +%d%m`"
varDataHoraInicio="`date +%F' '%T`"
START_TIME=$SECONDS
varDATALABEL=`date '+%Y_%m_%d_%H_%M'`
varPORTA=5432
varDIR_BKP_LOG=/backup/Log
varArquivoLogBackup=Backup_full_online_${varDATALABEL}.log

varPGUSER=backup_user
varPGDATA=/var/lib/postgresql/18/main

varDESTBKP=/backup/backup_fisico_online
varBACKUPDIR=$varDESTBKP/BKP_FISICO_ONLINE_BASE_$varDATALABEL
varARCHIVEDIR=/backup/archive/18/main
varCONFDIR=/etc/postgresql/18/main/
varBKPARCHIVE=$varDESTBKP/BKP_FISICO_ONLINE_BASE_${varDATALABEL}${varARCHIVEDIR}

#  NAO FUNCIONOU POIS PRECISA MAPEAR TODAS AS TABLESPACES NO MAPPING, UMA A UMA 
#varTABLESPACES=/database/tablespaces/18/main
#varBKPTABLESPACES=$varDESTBKP/BKP_FISICO_$varDATALABEL/$varTABLESPACES


## MENOR TAXA COMPRESSAO - MELHOR CONFIG

echo "---------------------------------------------------"  			  > $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "Iniciando Backup físico POSTGRES online (pg_basebackup) " `date`    2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  
echo "---------------------------------------------------"                2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  
echo " "                                                    2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1  
echo "Variaveis do script:"                                 2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "---------------------------------------------------"  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "varDATALABEL          =  $varDATALABEL             "  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "varPORTA              =  $varPORTA                 "  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "varDIR_BKP_LOG        =  $varDIR_BKP_LOG           "  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "varArquivoLogBackup   =  $varArquivoLogBackup      "  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "varPGUSER             =  $varPGUSER                "  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "varPGDATA             =  $varPGDATA                "  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "varDESTBKP            =  $varDESTBKP               "  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "varBACKUPDIR          =  $varBACKUPDIR             "  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "varARCHIVEDIR         =  $varARCHIVEDIR            "  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "varCONFDIR            =  $varCONFDIR               "  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "varBKPARCHIVE         =  $varBKPARCHIVE            "  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
#echo "varTABLESPACES        =  $varBACKUPDIR             "  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
#echo "varBKPTABLESPACES     =  $varBKPTABLESPACES        "  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "---------------------------------------------------"  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo " "                                                    2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1  
echo " "                                                    2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1  
GZIP=-1                                                     2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "Limpando os logs e tar"                               2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1  
rm -f ${varBKPARCHIVE}/000*                                 2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
rm -f ${varBACKUPDIR}/*.log                                 2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
rm -f ${varBACKUPDIR}/*.tar*                                2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
rm -f ${varBACKUPDIR}/*.conf                                2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
rmdir ${varBKPARCHIVE}                                      2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
rm -f ${varBACKUPDIR}/backup_manifest	                    2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
echo "mkdir -p ${varBACKUPDIR} "                            2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
mkdir -p ${varBACKUPDIR}                                    2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1



#  NAO FUNCIONOU POIS PRECISA MAPEAR TODAS AS TABLESPACES NO MAPPING, UMA A UMA 
#echo "mkdir -p ${varBKPTABLESPACES} "                       2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
#mkdir -p ${varBKPTABLESPACES}                               2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1

echo "/usr/bin/pg_basebackup -h localhost -p $varPORTA -U backup_user -D ${varBACKUPDIR} --format=tar --compress=none  --wal-method=s --verbose  --label=BackupONLINE_FULL_$varDATALABEL  --progress "
/usr/bin/pg_basebackup -h localhost -p $varPORTA -U backup_user -D ${varBACKUPDIR} --format=tar --gzip --compress=3  --wal-method=s --verbose  --label=BackupONLINE_FULL_$varDATALABEL  --progress  >  $varDIR_BKP_LOG/Log_pg_base_backup.log  


#  NAO FUNCIONOU POIS PRECISA MAPEAR TODAS AS TABLESPACES NO MAPPING, UMA A UMA 
# /usr/bin/pg_basebackup -h localhost -p $varPORTA -U backup_user -D ${varBACKUPDIR} --format=plain --tablespace-mapping=$varTABLESPACES=$varBKPTABLESPACES --compress=none  --wal-method=s --verbose  --label=BackupONLINE_FULL_$varDATALABEL  --progress  >  $varDIR_BKP_LOG/Log_pg_base_backup.log  

## gera um backup do log $varDIR_BKP_LOG/Log_Rman${varSCRIPT_RMAN}.log
cat  $varDIR_BKP_LOG/Log_pg_base_backup.log >> $varDIR_BKP_LOG/${varArquivoLogBackup}

## copia os arqauivos de configuração
cp $varCONFDIR/*.conf $varBACKUPDIR


## copia os archives
# varBKPARCHIVE
echo "mkdir -p ${varBKPARCHIVE} "                            2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
mkdir -p ${varBKPARCHIVE}                                    2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1
cp -r $varARCHIVEDIR/*  ${varBKPARCHIVE} ${varBKPARCHIVE}    2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  2>&1

ELAPSED_TIME=$(($SECONDS - $START_TIME))

echo "#### Estatisticas do Job de Backup:"                                                                    2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  
echo "**************************************************"                                                     2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  
echo "HORA INICIO               : " $varDataHoraInicio                                                        2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  
echo "TEMPO DE EXECUCAO (min)   : " $(( $ELAPSED_TIME/60))                                                    2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  
echo "TAMANHO DO BACKUP EM DISCO: (" $varBACKUPDIR ") "  $(du -sh ${varDIR_FRA_BD} | awk '{print $1}')        2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  
echo "**************************************************"                                                     2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  
echo ""       
echo "##### Fim Backup ${varSCRIPT_RMAN} - $ORACLE_SID no $HOSTNAME :  `date` em $(( $ELAPSED_TIME/60)) min"  2>&1 | tee -a $varDIR_BKP_LOG/${varArquivoLogBackup}  

# copia o log para a pasta do backup
cp   $varDIR_BKP_LOG/${varArquivoLogBackup}  $varBACKUPDIR

exit 0
