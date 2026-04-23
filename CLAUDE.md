# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Firebird 3 → PostgreSQL 18 migration pipeline for a ~21GB production database (~911 tables). All code and comments are in Portuguese. No ORM — all SQL is hand-crafted for performance (COPY protocol, direct `pg_catalog` queries).

Working directory on the Linux server: `/migracao_firebird`

## Architecture

### 3-Phase Migration Pipeline

1. **Phase 0 (Constraint Disable)**: `pg_constraints.py` discovers all constraints/PKs/indices/triggers via `pg_catalog`, saves state to JSON, generates DROP/CREATE SQL scripts, executes DROP
2. **Phase 1 (Data Load)**: Transfers rows from Firebird → PostgreSQL via COPY protocol with checkpoint/restart support
3. **Post-migration**: Re-enable constraints via `enable_constraints.py`, validate with comparison scripts

### Migration Strategies (4 migrators)

| Script | Strategy | Target |
|--------|----------|--------|
| `migrator_v2.py` | Sequential, config-driven | Most tables (via `config.yaml` or `--table`) |
| `migrator_parallel_doc_oper_v2.py` | ThreadPoolExecutor, PK range partitioning | DOCUMENTO_OPERACAO (largest, composite PK) |
| `migrator_log_eventos_v2.py` | ThreadPoolExecutor, RDB$DB_KEY partitioning | LOG_EVENTOS (no PK) |
| `migrator_smalltables_v2.py` | ProcessPoolExecutor, auto-discovery | ~901 small tables (excludes 10 big ones) |

All migrators share: checkpoint to SQLite (`migration_state_*.db` in `work/`), COPY protocol insertion, sub-batch retry on error, `MigrationProgress` dataclass for state serialization.

### Key Module: `pg_constraints.py`

`ConstraintManager` class — core library for constraint lifecycle. Queries `pg_catalog` (not `information_schema`) to avoid cartesian joins on composite FKs. Re-enable order: index → PK → unique → check → FK own → FK child → trigger.

### Validation Scripts (post-migration)

- `compara_cont_fb2pg.py` — Row count comparison Firebird vs PostgreSQL
- `compara_estrutura_fb2pg.py` — Structural comparison (PKs, FKs, indices, constraints)
- `PosMigracao_comparaChecksum_bytea.py` — MD5 checksum for BLOB/BYTEA columns
- `gera_relatorio_compara_estrutura_fb2pg_html.py` — HTML report generation

### Monitoring

`monitor.py` reads `migration_state_*.db` (SQLite) and renders a Rich TUI dashboard with progress bars, ETA, speed.

## Running

```bash
# Install dependencies (use uv or pip)
pip install -r requirements.txt

# Activate venv (Windows)
. .venv/Scripts/activate

# Sequential migration — table specified in config.yaml
python migrator_v2.py

# Sequential migration — override table via CLI
python migrator_v2.py --table OPERACAO_CREDITO

# Restart from scratch (drops SQLite checkpoint)
python migrator_v2.py --table OPERACAO_CREDITO --reset

# Dry run (no writes)
python migrator_v2.py --table OPERACAO_CREDITO --dry-run

# Generate constraint SQL scripts only
python migrator_v2.py --table OPERACAO_CREDITO --generate-scripts-only

# Parallel migration — DOCUMENTO_OPERACAO (composite PK, PK-range partitioned)
python migrator_parallel_doc_oper_v2.py --threads 4

# Parallel migration — LOG_EVENTOS (no PK, RDB$DB_KEY partitioned)
python migrator_log_eventos_v2.py --threads 8

# Small tables (~901 tables, ProcessPoolExecutor)
python migrator_smalltables_v2.py --config config_smalltables.yaml --small-tables

# Monitor progress (Rich TUI)
python monitor.py --state-db work/migration_state_operacao_credito.db

# Post-migration validation
python compara_estrutura_fb2pg.py --config config_smalltables.yaml
python gera_relatorio_compara_estrutura_fb2pg_html.py

# Re-enable constraints
python enable_constraints.py
```

Shell wrappers for server use:
```bash
./chama_migrator.sh CONTROLEVERSAO          # wraps migrator_v2.py, logs to logs/
./chama_migrator_doc_oper.sh                # wraps migrator_parallel_doc_oper_v2.py
```

## Configuration

- `config.yaml` — Main config: single/sequential table migration
- `config_smalltables.yaml` — Small tables: adds `parallel_workers`, `exclude_tables`, `master_state_db`

Table names: Firebird uses UPPERCASE, PostgreSQL uses lowercase. Conversion is automatic throughout.

## Technical Decisions

- **COPY protocol** over INSERT (3-5x faster) — fallback with `--use-insert`
- **pg_catalog** over `information_schema` for constraint queries (avoids composite FK cartesian join issue)
- **SQLite** for checkpoint state (lightweight, transactional, per-table) — files go in `work/`
- **RDB$DB_KEY** fallback for tables without PK (Firebird's physical row pointer)
- **WIN1252 → UTF-8** charset conversion for BLOB SUB_TYPE TEXT
- Fields `DADO`, `TE_IMAGEM_REDUZIDA`, `IMAGEM` forced to BYTEA regardless of declared subtype
- PostgreSQL session optimizations during load: `synchronous_commit=off`, `jit=off`, `autovacuum_enabled=false`

## Generated Files (per table at runtime)

Located under `work/`:
- `constraint_state_{table}.json` — Constraint state snapshot
- `disable_constraints_{table}.sql` / `enable_constraints_{table}.sql` — DROP/CREATE scripts
- `migration_state_{table}.db` — SQLite checkpoint database

Log files under `logs/`: `migration_{table}.log`, `{TABLE}_{DDMMYY_HHMMSS}.log`

## Dependencies

Python 3.13+. Key packages: `fdb` (Firebird driver), `psycopg2-binary` (PostgreSQL), `PyYAML`, `rich` (TUI). Requires `fbclient.dll` (Windows) or `libfbclient.so` (Linux) — `fbclient.dll` is bundled in the repo root.

## Environment

- **Server**: Debian 13 (AWS EC2), PostgreSQL port 5435
- **Database**: `c6_producao` (production clone)
- **Firebird DB**: `/firebird/data/c6emb.fdb`
- **Small tables**: ~901 migrated, 10 large tables handled separately
