#!/bin/bash
# ajustado por Marcelo Alegria em 23/12/2022 para a versao 18
export varDATA=`date +%d%m`
varDataHoraInicio="`date +%F\ %r`"
erro=0
erros=("Sem erro  "  "Banco fora" "Geracao do script" "DumpALL" "Backup das configuracoes 1" "Backup das CONFIGURACOES 2" "Backup das configuracoes 3" "Erro no DUMP de um database" "Erro na compactacao GZIP" "Erro na remocao do TAR antigo" "Erro na gravacao do TAR")
varDIRBACKUP=/backup/backup_logico
SCRIPTS=/backup/scripts
varDIR_BKP_LOG=/backup/Log
varARQ_LOG_prognum=${varDIRBACKUP}/copiabkpcommvault.log
GZIP='--fast'
PGOPTIONS='--client-min-messages=warning' 
PGDATA=/var/lib/postgresql/18/main
START_TIME=$SECONDS


mkdir -p $varDIR_BKP_LOG
mkdir -p $varDIRBACKUP

rm -f $varARQ_LOG_prognum
rm -f $varDIR_BKP_LOG/backup_logico_prognum.log 
$SCRIPTS/backup_logico.sh                                                                                       >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1

#####################################################

echo "************************************************************************************"                     >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1
echo "Enviando o backup lógico para o Commvault "                                                               >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1
INICIO_prognum=$SECONDS   
###                                                                                                               
## chamada de script externo                                                                                   
## cd ${SCRIPTS}                                                                                      
#${SCRIPTS}/copiabkplogicocommvault.sh                                                                           >> $varARQ_LOG_prognum  2>&1

if [ $? -ne 0 ]; then
   echo "ERRO: Erro na chamada do script copia do backup lógico para o Commvault "  `date`                      >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1
   varERRO_BACKUP="sim"
else
   echo "OK , copiado. "  `date`                                                                                >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1
fi
                                                          
                                                                                                              
if [ $(cat $varARQ_LOG_prognum | grep -E "Fail|fail"| wc -l) -gt 0 ] ;                                       
then                                                                                                           
     echo "************************************************************************************"                >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1
     echo "ERRO: Erro na copia do backup lógico para o Commvault"  `date`                                       >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1
     cat $varARQ_LOG_prognum                                                                                  >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1
     echo "************************************************************************************"                >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1
	 varERRO_BACKUP="sim"
else                                                                                                            
     echo "Copia do backup lógico para o Commvault realizada com sucesso"  `date`                               >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1
fi                                                                                                             
    

ELAPSED_TIME=$(($SECONDS - $INICIO_prognum))

echo "Tempo Execução script de cópia para o COMMVAULT em $(( $ELAPSED_TIME/60)) minutos "                       >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1
echo ""                                                                                                         >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1
echo "************************************************************************************"                     >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1
echo ""                                                                                                         >> $varDIR_BKP_LOG/backup_logico_prognum.log  2>&1

echo "#### Estatisticas do Job de Backup:"                                                                      >> $varDIR_BKP_LOG/backup_logico_prognum.log 2>&1
echo "**************************************************"                                                       >> $varDIR_BKP_LOG/backup_logico_prognum.log 2>&1
echo "HORA INICIO               : " $varDataHoraInicio                                                          >> $varDIR_BKP_LOG/backup_logico_prognum.log 2>&1
echo "TEMPO DE EXECUCAO (min)   : " $(( $ELAPSED_TIME/60))                                                      >> $varDIR_BKP_LOG/backup_logico_prognum.log 2>&1
echo "TAMANHO DO BACKUP EM DISCO: " $(du -sh $varDIRBACKUP/backup$varDATA.tar | awk '{print $1}')               >> $varDIR_BKP_LOG/backup_logico_prognum.log 2>&1
echo "**************************************************"                                                       >> $varDIR_BKP_LOG/backup_logico_prognum.log 2>&1
echo ""                                                                                                         >> $varDIR_BKP_LOG/backup_logico_prognum.log 2>&1
																												>> $varDIR_BKP_LOG/backup_logico_prognum.log 2>&1

if test $erro -eq 0 ; then 
  echo "Backup Logico iniciado em $varDataHoraInicio e finalizado em " `date +%F\ %r` "foi concluido com sucesso!"  >> $varDIR_BKP_LOG/backup_logico_prognum.log 2>&1
else
  echo "Backup Logico iniciado em $varDataHoraInicio e finalizado em " `date +%F\ %r` "foi concluido com falha: Codigo de erro => $erro "${erros[$erro]}" !" >> $varDIR_BKP_LOG/backup_logico_prognum.log 2>&1
fi

cat $varDIR_BKP_LOG/backup_logico_prognum.log >> $varDIRBACKUP/backup_logico_prognum_historico.log  
cat $varDIR_BKP_LOG/backup_logico_prognum.log

exit 0
