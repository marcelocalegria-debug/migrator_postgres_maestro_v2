#!/bin/bash
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONIOENCODING=utf-8

cd "$(dirname "$0")"
. .venv/Scripts/activate

python migrator_smalltables_v2.py --small-tables "$@"
