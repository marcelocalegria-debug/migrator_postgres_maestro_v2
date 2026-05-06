#!/bin/bash
# ajustado por Marcelo Alegria em 08/08/2023 para a versao 18


export varDATA=`date +%d%m`
varDataHoraInicio="`date +%F\ %r`"
erro=0
erros=("Sem erro  "  "Banco fora" "Geracao do script" "DumpALL" "Backup das configuracoes 1" "Backup das CONFIGURACOES 2" "Backup das configuracoes 3" "Erro no DUMP de um database" "Erro na compactacao GZIP" "Erro na remocao do TAR antigo" "Erro na gravacao do TAR")
LOGDIR=/backup/Log
BACKPATH=/backup/backup_logico
SCRIPTS=/backup/scripts
GZIP='--fast'
PGOPTIONS='--client-min-messages=warning' 
PGDATA=/var/lib/postgresql/18/main


mkdir -p $BACKPATH
mkdir -p $LOGDIR

# Apaga os logs da ultima execução
rm -vf $BACKPATH/backup*.tar
rm -vf $LOGDIR/backup_logico.log  
rm -vf $BACKPATH/erro_*.log*   
rm -vf $BACKPATH/*[0-9][0-9][0-9][0-9]*
rm -vf $BACKPATH/saida_stderr_pg_dumpall_postgres.log
rm -vf $LOGDIR/erro_pgdump.log
START_TIME=$SECONDS

echo " "                                                                       >> $LOGDIR/backup_logico.log 2>&1
echo " "                                                                       >> $LOGDIR/backup_logico.log 2>&1
echo "-----------------------------------------------------------------------" >> $LOGDIR/backup_logico.log 2>&1
echo " "''>> $LOGDIR/backup_logico.log 2>&1
echo "Backup Logico Full iniciado as `date +%F\ %r` "                          >> $LOGDIR/backup_logico.log 2>&1
echo " "                                                                       >> $LOGDIR/backup_logico.log 2>&1

# Verifica se o serviço de banco esta disponivel. 
/usr/lib/postgresql/18/bin/pg_ctl -p 5432 status -D $PGDATA  > /dev/null 2>> $LOGDIR/backup_logico.log 
if test $? -ne 0
then
    echo "ERRO FATAL: O SERVICO DE BANCO DE DADOS ESTA INDISPONIVEL!"
	echo "ERRO FATAL: O SERVICO DE BANCO DE DADOS ESTA INDISPONIVEL!"  >> $LOGDIR/backup_logico.log 2>&1
    erro=1
fi

comando=`ps -ef | grep backupPITR | wc -l`
if test $comando == '1'
then
  echo "Teste OK - Backup PITR nao estava em execucao no inicio do backup logico"
  echo "Teste OK - Backup PITR nao estava em execucao no inicio do backup logico"   >> $LOGDIR/backup_logico.log 2>&1  
else
   echo "AVISO - Backup PITR sendo feito na mesma janela do backup PITR. Ajustar schedules dos scripts de backups para nao haver sobrecarga"
   echo "AVISO - Backup PITR sendo feito na mesma janela do backup PITR. Ajustar schedules dos scripts de backups para nao haver sobrecarga"  >> $LOGDIR/backup_logico.log 2>&1  
fi

#
# Gera arquivo com as ddls para criacao de objetos globais (roles e tablespaces)
#
if test $erro -eq 0
then
  /usr/bin/pg_dumpall -p 5432 -g -U postgres >> $BACKPATH/global$varDATA.sql  2> $BACKPATH/saida_stderr_pg_dumpall_postgres.log  
  if test $? -ne 0 
   then 
    echo "AVISO - ERRO DE UM BUG NO PG_DUMPALL DO BANCO POSTGRES" 
	echo "AVISO - ERRO DE UM BUG NO PG_DUMPALL DO BANCO POSTGRES"  >> $LOGDIR/backup_logico.log  2>&1 
    erro=3
  fi
fi

#
# Gera um script shell com todos os bancos a serem backupeados: backup_logico_staging.sh
#
echo "/usr/bin/psql -q -d postgres -U postgres -f $SCRIPTS/listabancos.sql -o $BACKPATH/backup_logico_staging.sh " >> $LOGDIR/backup_logico.log 
/usr/bin/psql -p 5432 -q -d postgres -U postgres -f $SCRIPTS/listabancos.sql -o $BACKPATH/backup_logico_staging.sh  

if test $? -ne 0
 then 
  echo "ATENCAO ERRO NA GERACAO DO SCRIPT backup_logico_staging.sh" 
  echo "ATENCAO ERRO NA GERACAO DO SCRIPT backup_logico_staging.sh" >> $LOGDIR/backup_logico.log   
  erro=2
fi

### -----------------------------------------------------------------------------------------------------------
#    Retira caracteres +
sed -i -- 's/+/ /g' $BACKPATH/backup_logico_staging.sh
sed -i -- 's/~/+/g' $BACKPATH/backup_logico_staging.sh
chmod 755 $BACKPATH/backup_logico_staging.sh

echo "Iniciando backup full dos bancos" 
echo "Iniciando backup full dos bancos"                  >> $LOGDIR/backup_logico.log 2>&1

### EXECUTA O BACKUP FULL DOS BANCOS
if test $erro -eq 0 ; then
  echo "Iniciando PGDump dos bancos em `date +%F\ %r` "  >> $LOGDIR/backup_logico.log 2>&1
  $BACKPATH/backup_logico_staging.sh $varDATA            >> $LOGDIR/backup_logico.log 2>&1
  # ----------------------------------------------------------------------------------------------------------
  echo "Finalizando PGDump em `date +%F\ %r`"
  echo "Finalizando PGDump em `date +%F\ %r`"            >> $LOGDIR/backup_logico.log 2>&1
else 
  echo "ERRO $erro no backup_logico_staging.sh "
  echo "ERRO $erro no backup_logico_staging.sh "         >> $LOGDIR/backup_logico.log 2>&1  
fi 

echo "Fim Backup full em `date +%F\ %r`"                 >> $LOGDIR/backup_logico.log 2>&1

#
# copiando os arquivos de configuracao para o diretório de backup
#
cp /etc/postgresql/18/main/pg_hba.conf    $BACKPATH         >> $LOGDIR/backup_logico.log  2>> $LOGDIR/backup_logico.log 
if test $? -ne 0 
  then echo "ATENCAO ERRO COPY PG_HBA"                   >> $LOGDIR/backup_logico.log  2>&1
  erro=4
fi

cp /etc/postgresql/18/main/pg_ident.conf  $BACKPATH         >> $LOGDIR/backup_logico.log 2>&1
if test $? -ne 0 
  then echo "ATENCAO ERRO COPY PG_IDENT"                 >> $LOGDIR/backup_logico.log 2>&1
  erro=5
fi

cp /etc/postgresql/18/main/postgresql.conf  $BACKPATH       >> $LOGDIR/backup_logico.log 2>&1 
if test $? -ne 0 ;  then 
  erro=6
  echo "ATENCAO ERRO COPY POSTGRESQL "$erro              >> $LOGDIR/backup_logico.log 2>&1
fi

#
# Verifica algum erro nos logs PGdump
#
grep -n "FATAL:" $BACKPATH/erro_*.log$varDATA              >>  $LOGDIR/erro_pgdump.log 2> /dev/null
grep -n "ERROR:" $BACKPATH/erro_*.log$varDATA              >>  $LOGDIR/erro_pgdump.log 2> /dev/null

erro=`wc -l < $LOGDIR/erro_pgdump.log`
if test $erro -gt 0  ; then 
  erro=7
  echo "ATENCAO ERRO NO DUMP $erro"
  echo "ATENCAO ERRO NO DUMP $erro"                      >> $LOGDIR/backup_logico.log 2>&1
  cat $LOGDIR/erro_pgdump.log                          >> $LOGDIR/backup_logico.log 2>&1
fi

# 
# comprimindo os backups
#

echo "Iniciando Gzip -  iniciado em `date +%F\ %r` "     >> $LOGDIR/backup_logico.log 2>&1
gzip $BACKPATH/*_${varDATA}.bak                          >> $LOGDIR/backup_logico.log 2>&1
if test $? -ne 0 ; then 
    erro=8
    echo "ATENCAO ERRO NO GZIP "$erro >> $LOGDIR/backup_logico.log
fi

#
# compactando os arquivos gerados 
#

echo "Iniciando Tar -  iniciado em `date +%F\ %r` "           >> $LOGDIR/backup_logico.log 2>&1

tar -c --remove-files -P -f $BACKPATH/backup$varDATA.tar     $BACKPATH/*_${varDATA}.bak.gz $BACKPATH/*.conf      >> $LOGDIR/backup_logico.log 2>&1
if test $? -ne 0 ; then 
 echo "ATENCAO ERRO NO TAR" >> $LOGDIR/backup_logico.log
 erro=10
fi

#
ELAPSED_TIME=$(($SECONDS - $START_TIME))

echo "#### Estatisticas do Job de Backup:"                                                      >> $LOGDIR/backup_logico.log 2>&1
echo "**************************************************"                                       >> $LOGDIR/backup_logico.log 2>&1
echo "HORA INICIO               : " $varDataHoraInicio                                          >> $LOGDIR/backup_logico.log 2>&1
echo "TEMPO DE EXECUCAO (min)   : " $(( $ELAPSED_TIME/60))                                      >> $LOGDIR/backup_logico.log 2>&1
echo "TAMANHO DO BACKUP EM DISCO: " $(du -sh $BACKPATH/backup$varDATA.tar | awk '{print $1}')   >> $LOGDIR/backup_logico.log 2>&1
echo "**************************************************"                                       >> $LOGDIR/backup_logico.log 2>&1
echo ""       
if test $erro -eq 0 ; then 
  echo "Backup Logico iniciado em $varDataHoraInicio e finalizado em `date +%F\ %r` foi concluido com sucesso!" >> $LOGDIR/backup_logico.log 2>&1
else
  echo "Backup Logico iniciado em $varDataHoraInicio e finalizado em `date +%F\ %r` foi concluido com falha: Codigo de erro => $erro "${erros[$erro]}" !" >> $LOGDIR/backup_logico.log 2>&1
fi

cat $LOGDIR/backup_logico.log >> $LOGDIR/backup_logico_historico.log  
cat $LOGDIR/backup_logico.log 

exit 0
