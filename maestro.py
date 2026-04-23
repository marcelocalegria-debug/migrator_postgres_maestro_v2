#!/usr/bin/env python3
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
