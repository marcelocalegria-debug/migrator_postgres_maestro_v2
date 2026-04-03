 ---
  enable_constraints.py

  Funcionamento

  1. Lê cada enable_constraints_*.sql na ordem de dependência entre tabelas
  2. Executa statement por statement com autocommit=True (necessário para DDL)
  3. Classifica automaticamente cada statement: index, primary_key, unique, check, foreign_key, trigger, analyze,
  reindex
  4. Trata erros de forma inteligente:
    - already exists → SKIP (aviso, continua)
    - Outros erros → FAIL (loga, continua para próximos statements)
  5. Gera relatório Rich detalhado por tabela + sumário final
  6. Salva log + arquivo de relatório em texto

  Uso
  
  source .venv/bin/activate
  export PYTHONIOENCODING=utf-8

  # Execução completa (todas as 10 tabelas)
  python enable_constraints.py

  # Ver o que faria sem executar
  python enable_constraints.py --dry-run

  # Só uma tabela
  python enable_constraints.py --table operacao_credito

  # Múltiplas tabelas específicas
  python enable_constraints.py --table operacao_credito --table historico_operacao

  # Parar na primeira falha
  python enable_constraints.py --fail-fast

  # .sql estão em outro diretório (ex: no servidor)
  python enable_constraints.py --dir /migracao_firebird

  Arquivos gerados

  - enable_constraints_YYYYMMDD_HHMMSS.log — log completo da execução
  - relatorio_enable_constraints_YYYYMMDD_HHMMSS.txt — relatório em texto puro
