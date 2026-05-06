#!/bin/bash
# Script Backup Complementar ROOT (scripts, configuracao, etc)
# Marcelo Alegria 

####################### INICIO - Tratar parâmetro de entrada do Script e setar as variáveis de ambiente
varDIR_SCRIPTS="/backup/scripts"
varDIR_BACKUP="/backup/backup_fisico_offline/18/ConfiguracaoHost"
varDIR_LOG="/backup/Log"
varDiaMes="`date +%d%m`"
varDataHoraInicio="`date +%F\ %r`"
varDataArquivo="`date +%Y_%m_%d`"


####################### Seta variáveis para uso da rotina de Backup
mkdir -p $varDIR_BACKUP
mkdir -p $varDIR_LOG

chown -R postgres:postgres $varDIR_BACKUP

echo '########## INICIO BACKUP COMPLEMENTAR ROOT  ${HOSTNAME} em ${varDataHoraInicio} #################' >  $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '# uname '                                                                                          >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
uname -a                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo 'cat /etc/issue'                                                                                    >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
cat /etc/issue                                                                                           >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## CPUs #############'                                                                     >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#' 
lscpu                                                                                                    >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
cat /proc/cpuinfo | grep "model name"                                                                    >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
cat /proc/cpuinfo | grep cores                                                                           >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## MEMORIA #############'                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#' 
cat /proc/meminfo                                                                                        >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## LIMITS #############'                                                                   >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#' 
cat /etc/security/limits.conf                                                                            >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 

echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## BACKUP CRONTAB ROOT #############'                                                      >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1

crontab -l >  $varDIR_BACKUP/crontab_${USER}_${HOSTNAME}_${varDataArquivo}.bkp   
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## GROUPS ##############'                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
cat  /etc/group                                                                                          >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## PASSWD ##############'                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
cat  /etc/passwd                                                                                         >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## SHADOW ##############'                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
cat  /etc/shadow                                                                                         >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo '' 

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## MTAB ##############'                                                                    >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
cat  /etc/mtab                                                                                           >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo '' 

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## SAMBA ##############'                                                                   >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
tar -czvf $varDIR_BACKUP/backup_samba_dir.tar.gz  /etc/samba                                             >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## INITTAB ##############'                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
tar -czvf $varDIR_BACKUP/backup_inittab.tar.gz   /etc/inittab                                            >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## RC.D    ##############'                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
tar -czvf $varDIR_BACKUP/backup_rc.d.tar.gz  /etc/rc.d                                                   >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1


echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## SYSCTL ##############'                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
cat /etc/sysctl.conf                                                                                     >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## SYSCTL -a ##############'                                                               >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
sysctl -a                                                                                     			 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## LIMITS ##############'                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
cat /etc/security/limits.conf                                                                            >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
ls /etc/security/limits.d                                                                                >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
tar -czvf $varDIR_BACKUP/backup_limits.d.tar.gz /etc/security/limits.d                                   >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## KERBEROS ##############'                                                                >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
cat /etc/krb5.conf                                                                                       >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## NTP ##############'                                                                     >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
cat /etc/ntp.conf                                                                                        >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## SOFTLINKS NO / ##############'                                                          >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
ls -l / | grep '>'                                                                                       >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## PACKAGES INSTALADAS NO SO ##############'                                               >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
#zypper search -i                                                                                         >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
yum list installed                                                                                      >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## BACKUP IFCONFIG ################'                                                       >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
/sbin/ifconfig                                                                                           >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## FSTAB ################'                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
cat /etc/fstab                                                                                           >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## SYSLOG ##############'                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
cat /etc/audisp/plugins.d/syslog.conf                                                                    >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 

echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '########## SSHD_CONFIG ##############'                                                             >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo '#'                                                                                                 >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
cat  /etc/ssh/sshd_config                                                                                >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 
echo ''                                                                                                  >> $varDIR_LOG/backup_config_root_${varDataArquivo}.log 2>&1 


chown postgres:postgres ${varDIR_BACKUP}/*

cat $varDIR_LOG/backup_config_root_${varDataArquivo}.log

exit 0
