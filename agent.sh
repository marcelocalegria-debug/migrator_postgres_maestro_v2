#!/usr/bin/env bash
# Para ativar o venv na sessão atual sem rodar o agent:
#   source agent.sh --only-env
cd "$(dirname "$0")"
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export PYTHONIOENCODING=utf-8
source .venv/bin/activate
[[ "$1" == "--only-env" ]] && return 0 2>/dev/null || true
uv run lib/ai/agent.py "$@"
