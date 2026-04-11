#!/bin/bash
export LANG=C.UTF-8
export LC_ALL=C.UTF-8

psql -U c6_producao_user -W -h localhost -d c6_producao -f work/gen-c6bank-prod.sql

