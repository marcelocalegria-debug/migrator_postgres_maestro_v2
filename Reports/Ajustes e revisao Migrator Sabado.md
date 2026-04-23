## Ajustes e revisao Migrator Sabado - 11/04

##  firebird

docker exec -it firebird_padrao /usr/local/firebird/bin/isql 

SQL> CONNECT '/firebird/data/c6emb.fdb' USER 'SYSDBA' PASSWORD 'masterkey';
Database: '/firebird/data/c6emb.fdb', User: SYSDBA


###### postgres 18 docker 5435

$env:PGPASSWORD='5tEkZZwRydTUXarJ'
$env:PGCLIENTENCODING='LATIN1'

psql -h localhost -p 5435 -U "c6_producao_user" -d c6_producao 









######## postgres 18 local - 5432

"localhost:5432:*:postgres:c58d9143563c47398f8e170aab79963c" | Out-File -FilePath "$env:APPDATA\postgresql\pgpass.conf" -Encoding ascii
PS C:\Users\USER\AppData\Roaming> New-Item -ItemType Directory -Path "$env:APPDATA\postgresql" -Force


    Directory: C:\Users\USER\AppData\Roaming


Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
d-----        11/04/2026     10:59                postgresql


PS C:\Users\USER\AppData\Roaming> psql -h localhost -U postgres
psql (18.3)
ADVERTÊNCIA: A página de código da console (850) difere da página de código do Windows (1252)
             os caracteres de 8 bits podem não funcionar corretamente. Veja a página de
             referência do psql "Notes for Windows users" para obter detalhes.
Digite "help" para obter ajuda.

postgres=#

CREATE ROLE c6_producao_user WITH LOGIN PASSWORD 'c6_producao_user';

createdb -h localhost -U postgres -W c6_producao_pg_converter_equinix

createdb -h localhost -U postgres -W c6_producao_cria_sql_ec2

psql -h localhost -U postgres -W -d c6_producao_pg_converter_equinix -f c6_producao_pg_converter_equinix.sql

psql -h localhost -U postgres -W -d c6_producao_cria_sql_ec2 -f c6_producao_cria_sql_ec2.sql

c58d9143563c47398f8e170aab79963c



