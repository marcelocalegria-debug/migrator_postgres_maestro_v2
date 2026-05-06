###!/bin/bash
### Ajustado por Marcelo Alegria em 23/12/2025 para a versao 18
##
##LOGDIR=/backup/Log
##SCRIPTS=/backup/scripts/
##varDATA=`date +%d%m`
##erro=0
##datai=`date +%d/%m/%Y" as "%H:%M`
##NEW_expration_DATE=$(date -d "-1 days")
##echo $NEW_expration_DATE
##varDATALOG=`date -d "-1 days" +%d%m `
##PGDATA=/var/lib/postgresql/18/main
##ARCHIVE=$PGDATA/pg_xlog
##BACKUPARCHIVE=/backup/archive/18/main
##OLD="$BACKUPARCHIVE/*$varDATALOG*"
##BACKUP_FISICO_ONLINE=/backup/backup_fisico_online
##
##mkdir -p $LOGDIR
##mkdir -p $BACKUP_FISICO_ONLINE
##mkdir -p $BACKUP_FISICO_ONLINE
##
### Verifica se o sgbd esta disponivel. (Andre - 25/08/2014)
##/usr/lib/postgresql/18/bin/pg_ctl -p 5432 status -D $PGDATA  > /dev/null
##if test $? -ne 0
##then
##    echo "ERRO: O SERVICO DE BANCO DE DADOS ESTA INDISPONIVEL! $datai "  >> $LOGDIR/backup$varDATALOG.log
##    erro=1
##fi
##
##backPITR="${BACKUP_FISICO_ONLINE}/backup_in_progress"
##
##if [ -f "$backPITR" ]
##then
##        echo "Backup PITR esta em execucao ...  erro  abortando $datai  "  >> $LOGDIR/backup$varDATALOG.log
##        erro=1
##else
##        echo "Backup PITR nao esta em execucao ... continuando $datai  ."  >> $LOGDIR/backup$varDATALOG.log
##fi
##
##if test $erro  -ne 0
##then
##  exit 1 
##fi
##
##echo "Remover o arquivo  ${OLD} "
##if  ls $OLD 1> /dev/null 2>&1;  
##then
##       echo "Removendo backups antigos found $datai  .$BACKUPARCHIVE/*$varDATALOG " 
##       rm $OLD
##       mv $BACKUPARCHIVE/backup.log $LOGDIR/backup$varDATALOG.log
##else
##      echo " not found. $OLD  "
##fi
##
##
##mkdir -p $BACKUPARCHIVE
##mkdir -p $ARCHIVE
##varDATA=`date +%d%m`
##erro=0
##datainicial=`date +%s`
##
##tar -rvf $BACKUPARCHIVE/backupARCHIVE$varDATA.tar $ARCHIVE/                                   >> $LOGDIR/backupArchive$varDATA.log  2>&1
##
##if test $? -ne 0
##then
## dataf=`date +%d/%m/%Y" as "%H:%M`
## echo Backup Archive efetuado. Iniciado em $datai e terminado em $dataf  Com falhas ou erros  >> $LOGDIR/backupArchive$varDATA.log  2>&1
## erro=4
##else
##  dataf=`date +%d/%m/%Y" as "%H:%M`
##  echo Backup Archive efetuado. Iniciado em $datai e terminado em $dataf  Com sucesso          >> $LOGDIR/backupArchive$varDATA.log  2>&1
##fi
##
##cat  $LOGDIR/backupArchive$varDATA.log
##exit 0

