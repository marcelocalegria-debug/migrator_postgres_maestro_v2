#!/bin/bash

psql -U c6_producao_user -W -h localhost -d c6_producao -f gen-c6bank-prod.sql

