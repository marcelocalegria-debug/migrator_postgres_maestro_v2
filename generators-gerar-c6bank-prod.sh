#!/bin/bash
export LANG=C.UTF-8
export LC_ALL=C.UTF-8

mkdir -p work
/opt/firebird/bin/isql -User sysdba -fe /backup/firebird/senhabd-firebird localhost:/backup/firebird/scci.gdb << EOF | tr -d "," | awk '{print "drop sequence sq_" $2 " ; create sequence sq_" $2 "; select setval(\047sq_" $2  "\047," $5 ");" }' | sed "s/,0);/,1);/" > work/gen-c6bank-prod.sql
show generators;
EOF

