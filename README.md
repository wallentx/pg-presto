# pg-presto

A CLI tool for batch converting PostgreSQL SQL files to Presto/Athena SQL. 

It uses [sqlglot](https://github.com/tobymao/sqlglot) for AST-based transpilation and includes custom rules to identify and handle specific PostgreSQL-to-Athena migration traps.

## Features

- **Syntax Conversion**: Translates PostgreSQL operators, casts (`::type`), and functions (`NOW()`, `ILIKE`) to their Presto equivalents.
- **Complex Types**: Handles JSON/JSONB operators (e.g., `->>`, `@>`) and array functions.
- **Limitation Warnings**: Statically analyzes the output to warn about Athena-specific limitations and semantic traps, such as:
  - Unsupported DDL (e.g., `SERIAL`, `PRIMARY KEY`).
  - Unsupported DML (e.g., `ON CONFLICT`, `RETURNING`).
  - Semantic differences (e.g., `EXTRACT(DOW)` returning 1-7 in Athena instead of 0-6).

## Usage

Requires Python 3.9+ and [uv](https://docs.astral.sh/uv/).

```bash
# Convert a directory of SQL files
uv run pg2athena ./path/to/postgres_sql ./path/to/athena_sql

# Generate a JSON report of the conversion process
uv run pg2athena ./source ./output --report report.json
```

See the `examples/` directory for sample inputs, outputs, and limitation warnings.

## Development

This project uses `uv` for dependency management, `pytest` for testing, `ruff` for linting and formatting, and `mypy` for static type checking.

```bash
# Run tests, linting, and type checking
make all
```
