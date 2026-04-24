# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Firebird 3 → PostgreSQL 18 migration pipeline for a ~21GB production database (~911 tables). All code and comments are in Portuguese. No ORM — all SQL is hand-crafted for performance (COPY protocol, direct `pg_catalog` queries).

Working directory on the Linux server: `/migracao_firebird`

## Architecture

The project has two layers:

1. **Maestro V2** (`maestro.py` + `lib/`) — interactive CLI orchestrator that manages full migrations end-to-end
2. **Legacy migrators** — standalone scripts invoked by Maestro via subprocess, but also runnable directly

### Maestro V2

Primary entry point. Interactive CLI built with `prompt_toolkit` + `rich`. Commands: `/init`, `/resume <SEQ>`, `/status`, `/check`, `/compare`, `/monitor`, `/run`, `/agent`, `/help`, `/quit`.

Each migration creates an isolated workspace: `MIGRACAO_<SEQ>/` (4-digit sequence starting at `0001`). All runtime files — `config.yaml`, `migration.db`, `logs/`, `sql/`, `json/`, `reports/` — live inside this directory.

**Windows warning**: When exiting Maestro (Ctrl+C or `/quit`), subprocess-spawned migrators may survive as orphan processes. Kill them in Task Manager before re-running `/run` to avoid PK violations or data duplication.

### `lib/` Module

| Module | Purpose |
|--------|---------|
| `lib/cli.py` | `MaestroCLI` — interactive REPL, dispatches all commands |
| `lib/project.py` | Workspace management (`MIGRACAO_<SEQ>/` directory lifecycle) |
| `lib/db.py` | `MigrationDB` — single SQLite state DB per migration (`migration.db`) |
| `lib/config.py` | `MigrationConfig` — loads/validates `config.yaml` |
| `lib/state.py` | State serialization helpers |
| `lib/steps/` | Step implementations S00–S13, each as its own file |
| `lib/ai/agent.py` | AI agent via Google ADK + LiteLLM (OpenRouter) |
| `lib/ai/prompts.py` | System prompt and diagnostic prompt templates (Portuguese) |

Steps run in order: S00 precheck → S01 create DB → S02 import schema → S03 pre-compare → S05 disable constraints → S06 migrate big tables → S07 migrate small tables → S08 enable constraints → S09 sequences → S10 post-compare → S11 validate → S12 analyze → S13 report.

### 3-Phase Data Load Pipeline

1. **Phase 0 (Constraint Disable)**: `pg_constraints.py` discovers all constraints/PKs/indices/triggers via `pg_catalog`, saves state to JSON, generates DROP/CREATE SQL scripts, executes DROP
2. **Phase 1 (Data Load)**: Transfers rows Firebird → PostgreSQL via COPY protocol with checkpoint/restart support
3. **Post-migration**: Re-enable constraints via `enable_constraints.py`, validate with comparison scripts

### Migration Strategies (4 migrators)

| Script | Strategy | Target |
|--------|----------|--------|
| `migrator_v2.py` | Sequential, config-driven | Most tables (via `config.yaml` or `--table`) |
| `migrator_parallel_doc_oper_v2.py` | ThreadPoolExecutor, PK range partitioning | DOCUMENTO_OPERACAO (largest, composite PK) |
| `migrator_log_eventos_v2.py` | ThreadPoolExecutor, RDB$DB_KEY partitioning | LOG_EVENTOS (no PK) |
| `migrator_smalltables_v2.py` | ProcessPoolExecutor, auto-discovery | ~901 small tables (excludes 10 big ones) |

All migrators share: checkpoint to SQLite, COPY protocol insertion, sub-batch retry on error, `MigrationProgress` dataclass for state serialization.

### Key Module: `pg_constraints.py`

`ConstraintManager` class — core library for constraint lifecycle. Queries `pg_catalog` (not `information_schema`) to avoid cartesian joins on composite FKs. Re-enable order: index → PK → unique → check → FK own → FK child → trigger.

### Validation Scripts

- `compara_cont_fb2pg.py` — Row count comparison Firebird vs PostgreSQL
- `compara_estrutura_fb2pg.py` — Structural comparison (PKs, FKs, indices, constraints)
- `PosMigracao_comparaChecksum_bytea.py` — MD5 checksum for BLOB/BYTEA columns
- `gera_relatorio_compara_estrutura_fb2pg_html.py` — HTML report generation

### Monitoring

`monitor.py` reads `migration.db` (Maestro) or `migration_state_*.db` (legacy) and renders a Rich TUI dashboard with progress bars, ETA, and speed.

## Running

```bash
# Install dependencies (use uv or pip)
pip install -r requirements.txt

# Activate venv (Windows)
. .venv/Scripts/activate

# ── Maestro V2 (recommended) ─────────────────────────────────────────────────
python maestro.py                       # Interactive CLI (auto-resumes last migration)
python maestro.py --resume 0005         # Explicitly resume migration 0005

# ── Legacy standalone scripts ─────────────────────────────────────────────────
# Sequential — table from config.yaml
python migrator_v2.py

# Sequential — override table via CLI
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
python migrator_smalltables_v2.py --small-tables

# Monitor progress (Rich TUI)
python monitor.py --state-db MIGRACAO_0005/migration.db

# Post-migration validation
python compara_estrutura_fb2pg.py
python gera_relatorio_compara_estrutura_fb2pg_html.py

# Re-enable constraints
python enable_constraints.py

# Emergency: repair composite FK JSON/SQL if cartesian-join bug produced duplicate columns
python repair_fk_scripts.py
```

## Configuration

`config.yaml` (root) is the template. Each `MIGRACAO_<SEQ>/config.yaml` is a per-run copy created by `/init`.

Key sections:
- `firebird` / `postgresql` — connection params (host, port, database, user, password, charset)
- `migration.batch_size` — rows per COPY batch (default 10000)
- `migration.parallel_workers` — ProcessPoolExecutor workers for small tables
- `migration.exclude_tables` — tables excluded from `migrator_smalltables_v2.py` (the 10 large tables)

Table names: Firebird uses UPPERCASE, PostgreSQL uses lowercase. Conversion is automatic throughout.

## AI Agent

The `/agent` command in Maestro invokes `lib/ai/agent.py` (Google ADK + LiteLLM via OpenRouter). Used for schema diff resolution and error diagnosis only — never for bulk data operations. Requires `.env` at the project root:

```
OPENROUTER_URL=...
OPENROUTER_API_KEY=...
MODEL=...          # LiteLLM model string, e.g. anthropic/claude-sonnet-4-5
```

## Technical Decisions

- **COPY protocol** over INSERT (3-5x faster) — fallback with `--use-insert`
- **pg_catalog** over `information_schema` for constraint queries (avoids composite FK cartesian join issue)
- **Single `migration.db`** per `MIGRACAO_<SEQ>` (Maestro) vs one SQLite per table (legacy migrators)
- **RDB$DB_KEY** fallback for tables without PK (Firebird's physical row pointer)
- **WIN1252 → UTF-8** charset conversion for BLOB SUB_TYPE TEXT
- Fields `DADO`, `TE_IMAGEM_REDUZIDA`, `IMAGEM` forced to BYTEA regardless of declared subtype
- PostgreSQL session optimizations during load: `synchronous_commit=off`, `jit=off`, `autovacuum_enabled=false`

## Generated Files

**Maestro V2** stores everything inside `MIGRACAO_<SEQ>/`:
- `migration.db` — master SQLite state DB (steps + per-table progress + error log)
- `config.yaml` — copy of config used for this run
- `sql/` — `disable_constraints_*.sql`, `enable_constraints_*.sql`, sequence scripts
- `json/` — `constraint_state_*.json` snapshots
- `logs/` — per-step and per-table logs
- `reports/` — HTML comparison reports

**Legacy migrators** write to the project root:
- `migration_state_{table}.db` — per-table SQLite checkpoint
- `constraint_state_{table}.json`, `disable_constraints_{table}.sql`, `enable_constraints_{table}.sql`

## Dependencies

Python 3.13+. Key packages: `fdb` (Firebird driver), `psycopg2-binary` (PostgreSQL), `PyYAML`, `rich` (TUI), `prompt_toolkit` (interactive CLI), `google-adk` + `litellm` (AI agent). Requires `fbclient.dll` (Windows) or `libfbclient.so` (Linux) — `fbclient.dll` is bundled in the repo root.

## Environment

- **Server**: Debian 13 (AWS EC2), PostgreSQL port 5435
- **Database**: `c6_producao` (production clone)
- **Firebird DB**: `/firebird/data/c6emb.fdb`
- **Shell wrappers**: In `OLD/` directory (historical reference only)
- **Small tables**: ~901 migrated via S07; 10 large tables handled separately via S06
