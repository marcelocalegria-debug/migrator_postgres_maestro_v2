\t
select ' if ! /usr/bin/vacuumdb -p 5432 -q -d ' || datname ||  '  -f -z -e >> /backup/Log/vacuum_' || datname ||'.log$1 2>&1 '||chr(10) ||' then ' ||chr(10)||' echo "Erro no Vacuum" '||datname|| chr(10) ||' fi ' 
from pg_database  where datallowconn = 't'
order by datname;
\q

