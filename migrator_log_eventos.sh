#!/bin/bash
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONIOENCODING=utf-8

cd "$(dirname "$0")"
. .venv/Scripts/activate

python migrator_log_eventos_v2.py "$@"
