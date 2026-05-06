\t
select '/usr/bin/psql -p 5432 -d postgres -U postgres -c "SELECT pg_terminate_backend(s.pid) FROM pg_stat_activity s inner join pg_locks l on l.database = s.datid WHERE s.usename='|| Chr(39) || datname || Chr(39) ||' and s.datname NOT IN ('|| Chr(39) ||'postgres'|| Chr(39) ||');"'|| chr(10) ||'/usr/bin/pg_dump -p 5432 -f 10 -C ' || datname || ' -F c -U postgres -v -f /backup/backup_logico/' || datname || '_${1}.bak >> /backup/backup_logico/backup_' || datname || '_$1.log  2>&1 '|| chr(10) ||'code=$? '|| chr(10) ||'if [ $code -ne 0 ]; then '|| chr(10) ||' echo 1>&2 "ERROR The backup failed (exit code $code), check for errors in $error" '|| datname || chr(10) ||' else '|| chr(10) ||' echo "Backup executado " '|| datname || chr(10) ||' fi '
from pg_database
where datallowconn = 't'
order by datname;

\q

