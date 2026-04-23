# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Firebird 3 → PostgreSQL 18 migration pipeline for a ~21GB production database (~911 tables). All code is in Portuguese. No ORM — all SQL is hand-crafted for performance (COPY protocol, direct pg_catalog queries).

## Architecture

### 3-Phase Migration Pipeline

1. **Phase 0 (Constraint Disable)**: `pg_constraints.py` discovers all constraints/PKs/indices/triggers via `pg_catalog`, saves state to JSON, generates DROP/CREATE SQL scripts, executes DROP
2. **Phase 1 (Data Load)**: Transfers rows from Firebird → PostgreSQL via COPY protocol with checkpoint/restart support
3. **Post-migration (Manual)**: Re-enable constraints via `enable_constraints.py`, validate with comparison scripts

### Migration Strategies (4 migrators)

| Script | Strategy | Target |
|--------|----------|--------|
| `migrator.py` | Sequential, single-table | Most tables (via config or `--table`) |
| `migrator_parallel_doc_oper.py` | ThreadPoolExecutor, PK range partitioning | DOCUMENTO_OPERACAO (largest, composite PK) |
| `migrator_log_eventos.py` | ThreadPoolExecutor, RDB$DB_KEY partitioning | LOG_EVENTOS (no PK) |
| `migrator_smalltables.py` | ProcessPoolExecutor, auto-discovery | ~901 small tables (excludes 10 big ones) |

All migrators share: checkpoint to SQLite (`migration_state_*.db`), COPY protocol insertion, sub-batch retry on error, `MigrationProgress` dataclass for state serialization.

### Key Module: `pg_constraints.py`

`ConstraintManager` class — the core library for constraint lifecycle. Queries `pg_catalog` (not `information_schema`) to avoid cartesian joins on composite FKs. Re-enable order: index → PK → unique → check → FK own → FK child → trigger.

### Validation Scripts (post-migration)

- `compara_cont_fb2pg.py` — Row count comparison
- `compara_estrutura_fb2pg.py` — Structural comparison (PKs, FKs, indices, constraints)
- `PosMigracao_comparaChecksum_bytea.py` — MD5 checksum for BLOB/BYTEA columns
- `gera_relatorio_compara_estrutura_fb2pg_html.py` — HTML report generation

### Monitoring

`monitor.py` reads `migration_state_*.db` (SQLite) and renders a Rich TUI dashboard with progress bars, ETA, speed.

## Running

```bash
# Install dependencies
pip install -r requirements.txt

# Sequential migration (single table)
python migrator.py --table OPERACAO_CREDITO

# Dry run
python migrator.py --table OPERACAO_CREDITO --dry-run

# Generate constraint scripts only
python migrator.py --table OPERACAO_CREDITO --generate-scripts-only

# Small tables (auto-discovers ~901 tables)
python migrator_smalltables.py --config config_smalltables.yaml --small-tables

# Monitor progress
python monitor.py --state-db migration_state_operacao_credito.db

# Post-migration validation
python compara_estrutura_fb2pg.py --config config_smalltables.yaml
python gera_relatorio_compara_estrutura_fb2pg_html.py

# Re-enable constraints
python enable_constraints.py
```

## Configuration

Two YAML configs drive behavior:
- `config.yaml` — Main config for sequential/big table migration
- `config_smalltables.yaml` — Small tables with `parallel_workers`, `exclude_tables`, `master_state_db`

Table names: Firebird uses UPPERCASE, PostgreSQL uses lowercase. Conversion is automatic.

## Technical Decisions

- **COPY protocol** over INSERT (3-5x faster) — fallback with `--use-insert`
- **pg_catalog** over information_schema for constraint queries (avoids composite FK issues)
- **SQLite** for checkpoint state (lightweight, transactional, per-table)
- **RDB$DB_KEY** fallback for tables without PK (Firebird's physical row pointer)
- **WIN1252 → UTF-8** charset conversion for BLOB SUB_TYPE TEXT
- **Fields `DADO`, `TE_IMAGEM_REDUZIDA`, `IMAGEM`** are forced to BYTEA regardless of declared subtype
- PostgreSQL session optimizations during load: `synchronous_commit=off`, `jit=off`, `autovacuum_enabled=false`

## Dependencies

Python 3.11+. Key packages: `fdb` (Firebird driver), `psycopg2-binary` (PostgreSQL), `PyYAML`, `rich` (TUI). Requires `fbclient.dll` (Windows) or `libfbclient.so` (Linux).

## Generated Files (per table at runtime)

- `constraint_state_{table}.json` — Constraint state snapshot
- `disable_constraints_{table}.sql` / `enable_constraints_{table}.sql` — DROP/CREATE scripts
- `migration_state_{table}.db` — SQLite checkpoint database
- `migration_{table}.log` — Per-table log

## Environment

- **Server**: Debian 13 (AWS EC2), PostgreSQL port 5435
- **Database**: `c6_alegria` (production clone)
- **Migration state**: 901 small tables migrated, constraints re-enabled with some duplicate errors (benign)
