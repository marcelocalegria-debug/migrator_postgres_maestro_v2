#!/usr/bin/env python3
"""
maestro.py
==========
Orquestrador interativo da migração Firebird 3 → PostgreSQL 18.
CLI com prompt_toolkit + Rich. Auto-resume da última migração ao iniciar.

Uso:
    python maestro.py               # inicia ou retoma última migração
    python maestro.py --resume 0005 # resume explícito da migração 0005

Comandos dentro do CLI:
    /init              Cria nova migração MIGRACAO_<SEQ>/
    /resume 0005       Carrega migração (alias: /load 0005)
    /status            Exibe steps S00–S13 com status e duração
    /check             Valida conexões FB+PG
    /compare           Roda comparação estrutural e abre relatório HTML
    /run [step]        Executa pipeline a partir do step pendente
    /rerun <step>      Força re-execução de um step concluído
    /monitor           Abre monitor Rich TUI em tempo real
    /agent             Chat com agente IA (diagnóstico de schema)
    /help              Lista comandos disponíveis
    /quit              Sai (Ctrl+C também funciona)
"""
import os
import sys
from dotenv import load_dotenv
from lib.cli import MaestroCLI

def main():
    # Carrega variáveis de ambiente (.env)
    load_dotenv()
    
    # Inicia a CLI
    cli = MaestroCLI()
    
    # Se houver argumentos (ex: --resume 0001), processa antes de entrar no loop
    if len(sys.argv) > 2 and sys.argv[1] == "--resume":
        cli.do_resume([sys.argv[2]])
    
    cli.run()

if __name__ == "__main__":
    main()
