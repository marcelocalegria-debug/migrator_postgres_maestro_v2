@echo off
cd /d "%~dp0"
chcp 65001 > nul
set PGCLIENTENCODING=LATIN1
set LANG=C.UTF-8
set LC_ALL=C.UTF-8
set PYTHONIOENCODING=utf-8
call .\.venv\Scripts\activate
uv run lib/ai/agent.py %*
