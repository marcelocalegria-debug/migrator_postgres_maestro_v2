
  Arquitetura implementada

  main()
   ├─ Fase 0: ConstraintManager → disable constraints + gera scripts
   ├─ Fase 1: count_rows + compute_pk_ranges (percentile queries)
   │           truncate (somente fresh start)
   ├─ Fase 2: AggregatorThread (daemon, a cada 2s)
   │            lê N state DBs dos workers → soma → escreve
   │            migration_state_documento_operacao.db  ← monitor.py vê isso
   │
   │           WorkerThread[0..N-1] (paralelas)
   │            SELECT * WHERE NU_OPERACAO >= start [AND < end]
   │            COPY protocol, batch, checkpoint por thread
   └─ Resumo + atualização final do estado agregado

  Comportamento por thread

  - Estado próprio: migration_state_documento_operacao_t{N}.db
  - Log próprio: migration_documento_operacao_t{N}.log
  - Checkpoint/Resume: salva last_pk_value = [NU_OPERACAO, NU_DOCUMENTO] do último batch; resume com WHERE composto
  correto
  - Ctrl+C: termina o batch atual, salva checkpoint, status paused

  Uso

  # Rodar com 4 threads
  python migrator_parallel_doc_oper.py --threads 4

  # Acompanhar no monitor (1 linha agregada)
  python monitor.py

  # Retomar após interrupção (sem --reset)
  python migrator_parallel_doc_oper.py --threads 4

  # Reiniciar do zero
  python migrator_parallel_doc_oper.py --threads 4 --reset

  Nota: não mude --threads entre execuções sem --reset — os ranges são calculados na primeira execução e o checkpoint
  assume os mesmos limites.

  Uso:
    source .venv/bin/activate
    export PYTHONIOENCODING=utf-8
    python migrator_parallel_doc_oper.py --threads 4
    python migrator_parallel_doc_oper.py --threads 4 --reset
    python migrator_parallel_doc_oper.py --threads 4 --dry-run
    python migrator_parallel_doc_oper.py --threads 4 --batch-size 5000
    python migrator_parallel_doc_oper.py --threads 4 --use-insert
    python migrator_parallel_doc_oper.py --threads 4 --generate-scripts-only

Arquivos gerados:
    migration_state_documento_operacao.db          → monitor.py (progresso agregado)
    migration_state_documento_operacao_tN.db       → checkpoint individual por thread
    migration_documento_operacao_tN.log            → log individual por thread
    migration_documento_operacao_parallel.log      → log do orquestrador
    disable_constraints_documento_operacao.sql
    enable_constraints_documento_operacao.sql
    constraint_state_documento_operacao.json