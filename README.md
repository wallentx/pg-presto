# pg-presto

A CLI tool for batch converting PostgreSQL SQL files to Presto/Athena SQL. 

It uses [sqlglot](https://github.com/tobymao/sqlglot) for AST-based transpilation and includes custom rules to identify and handle specific PostgreSQL-to-Athena migration traps.

## Features

- **Syntax Conversion**: Translates PostgreSQL operators, casts (`::type`), and functions (`NOW()`, `ILIKE`) to their Presto equivalents.
- **Complex Types**: Handles JSON/JSONB operators (e.g., `->>`, `@>`) and array functions.
- **Pipe Mode**: Accepts SQL from STDIN and prints to STDOUT for easy integration with other CLI tools.
- **Standalone Binary**: Can be built into a single executable file.
- **Limitation Warnings**: Statically analyzes the output to warn about Athena-specific limitations and semantic traps.

## Installation

### From Source
Requires Python 3.9+ and [uv](https://docs.astral.sh/uv/).

```bash
git clone git@github.com:wallentx/pg-presto.git
cd pg-presto
make build
# The executable will be at dist/pg2athena
```

## Usage

### Pipe Mode (STDIN -> STDOUT)
```bash
echo "SELECT NOW();" | ./dist/pg2athena
```

### File Mode
```bash
./dist/pg2athena input.sql output.sql
```

### Batch Directory Mode
```bash
./dist/pg2athena ./path/to/postgres_sql ./path/to/athena_sql
```

## Development
```bash
# Run tests, linting, type checking, and build executable
make all
```

