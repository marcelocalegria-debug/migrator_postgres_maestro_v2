#!/bin/bash
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONIOENCODING=utf-8

cd "$(dirname "$0")"
source .venv/bin/activate

python migrator_smalltables.py --small-tables "$@"
