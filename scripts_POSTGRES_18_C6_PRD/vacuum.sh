#!/bin/bash
# Ajustado por Marcelo Alegria em 23/12/2023 para a versão 18

erro=0
LOG=/backup/Log
SCRIPTS=/backup/scripts/

mkdir -p $LOG

rm $LOG/vacuum.log
datai=`date +%d/%m/%Y" as "%H:%M`

erro=`ps -ef | grep -v grep | grep -ic pg_dump`
echo $erro

if test $erro -gt 0 
	  then
	  # mostra na tela
	  echo "ERRO: O SERVICO DE BANCO DE DADOS ESTA EM BACKUP !" 
	  # grava no log
	  echo "ERRO: O SERVICO DE BANCO DE DADOS ESTA EM BACKUP !" >> $LOG/vacuum.log
	 exit 1
		 
else

	# Verifica se o sgbd esta disponivel. (Andre - 25/08/2014)
	/usr/lib/postgresql/18/bin/pg_ctl -p 5432 status -D /var/lib/postgresql/18/main  > /dev/null
	if test $? -ne 0
	then
		echo "ERRO: O SERVICO DE BANCO DE DADOS ESTA INDISPONIVEL!" >> $LOG/vacuum.log
		erro=1
	fi

	# 
	# Gerar o comando de vacuum separadamente para evitar que o cancelamento da execucao em um banco prejudique os demais
	#
	param=`date +%d%m`
	#
	# Gera arquivo com todos os bancos a serem tratados
	#
	/usr/bin/psql -p 5432 -q  -d postgres -U postgres -f /backup/scripts/vacuum_bancos.sql -o $SCRIPTS/vacuum_bancos.sh >> $LOG/vacuum.log 2>&1
	if test $? -ne 0
	then 
		echo "ATENCAO ERRO GRAVACAO VACUUM BANCOS" >> $LOG/vacuum.log 
		erro=1
	fi

	sed -i -- 's/+//g' $SCRIPTS/vacuum_bancos.sh
	#

	echo "Vaccum Iniciado $datai " >> $LOG/vacuum.log 2>&1
	chmod +x $SCRIPTS/vacuum_bancos.sh
	$SCRIPTS/vacuum_bancos.sh     >> $LOG/vacuum.log 2>&1                            
	#

	data=`date +%d/%m/%Y" as "%H:%M`
	echo "Vaccum Finalizado  $data " >> $LOG/vacuum.log 2>&1  

	if test $erro -eq 0
	then
		echo "Vacuum iniciado as $datai e finalizado em $data foi concluido com sucesso!" >> $LOG/vacuum.log 2>&1
	fi
fi

cat $LOG/vacuum.log >> $LOG/vacuum_historico.log

cat $LOG/vacuum.log

exit 0
