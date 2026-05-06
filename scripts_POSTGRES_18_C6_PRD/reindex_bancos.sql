\t
select '/usr/bin/psql -p 5432 -d ' || datname || ' -e >> /backup/scripts/reindexdb_' || datname ||'.log$1 2>&1'
from pg_database 
where datallowconn = 't'  --and not datname = 'contasnacionais'
order by datname;
\q
