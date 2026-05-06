echo $PGDATA
echo $PGHOME
export free=$(free -mt | grep Total | awk '{print $4}')
export cpus=$(lscpu | grep "CPU(s):")
echo $free
echo $cpus

/usr/lib/postgresql/18/bin/pg_ctl reload -D /var/lib/postgresql/18/main


