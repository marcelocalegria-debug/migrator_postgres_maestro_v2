#!/bin/bash
# Script Backup Complementar (scripts, configuracao, etc)
# Ajuste para a 18 em 19/03/2026 por Marcelo Alegria

####################### INICIO - Tratar parâmetro de entrada do Script e setar as variáveis de ambiente

varDIR_SCRIPTS="/backup/scripts"
varDIR_BACKUP="/backup/backup_fisico_offline/18/ConfiguracaoHost"
varDIR_LOG="/backup/Log"
varDiaMes="`date +%d%m`"
varDataHoraInicio="`date +%F\ %r`"
varDataArquivo="`date +%Y_%m_%d`"
PGDATA=/var/lib/postgresql/18/main

####################### Seta variáveis para uso da rotina de Backup

mkdir -p $varDIR_BACKUP
mkdir -p $varDIR_LOG

echo '########## BACKUP COMPLEMENTAR SERVIDOR' ${HOSTNAME}' em '${varDataHoraInicio}' #############'     > $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1

echo '########## BACKUP ARQUIVOS CONF POSTGRESQL.CONF PG_HBA e PG_IDENT ###############'                 >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
cp -vp /etc/postgresql/18/main/postgresql.conf  $varDIR_BACKUP/postgresql_${varDataArquivo}.conf                         >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
cp -vp /etc/postgresql/18/main/pg_hba.conf      $varDIR_BACKUP/pg_hba_${varDataArquivo}.conf                             >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
cp -vp /etc/postgresql/18/main/pg_ident.conf    $varDIR_BACKUP/pg_ident_${varDataArquivo}.conf                           >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
echo '########## BACKUP CRONTAB postgres #################'                                              >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
crontab -l >  $varDIR_BACKUP/crontab_${USER}_${HOSTNAME}_${varDataArquivo}.bkp        
echo ''                                                                                                  >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
echo '########## BACKUP SCRIPTS NA PASTA '${varDIR_SCRIPTS} '#################'                          >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
ls -lart  $varDIR_SCRIPTS/*                                                                              >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
tar -zcvf $varDIR_BACKUP/backup_scripts_bkp.tar.gz $varDIR_SCRIPTS/*
echo ''                                                                                                  >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
echo '########## CONFIG FS #################'                                                            >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
df -kh                                                                                                   >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1
echo ''                                                                                                  >> $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log 2>&1

cat $varDIR_LOG/backup_config_postgres_${varDataArquivo}.log
exit 0
