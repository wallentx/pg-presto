# pg-aegis

A CLI tool for converting PostgreSQL SQL to Athena SQL, using Presto-compatible output where appropriate.

It uses [sqlglot](https://github.com/tobymao/sqlglot) for AST-based transpilation and includes custom rewrites and warnings for common PostgreSQL-to-Athena migration issues.

## Features

- **Syntax Conversion**: Translates common PostgreSQL operators, casts (`::type`), and functions (`NOW()`, `ILIKE`) to Athena-compatible SQL.
- **Targeted Rewrites**: Handles supported JSON/JSONB and array patterns such as `->>`, `@>`, and `JSONB_ARRAY_ELEMENTS_TEXT(...)`.
- **Pipe Mode**: Accepts SQL from STDIN and prints to STDOUT for easy integration with other CLI tools.
- **Standalone Binary**: Can be built into a single executable file.
- **Warnings and Validation**: Emits Athena-specific limitation warnings, supports schema-aware offline validation, and can run Athena-backed validation with `EXPLAIN (TYPE VALIDATE)`.

## Installation

### From Source
Requires Python 3.9+ and [uv](https://docs.astral.sh/uv/).

```bash
git clone git@github.com:wallentx/pg-aegis.git
cd pg-aegis
make build
# The executable will be at dist/pg-aegis
```

## Usage

### Pipe Mode (STDIN -> STDOUT)
```bash
echo "SELECT NOW();" | ./dist/pg-aegis
```

### File Mode
```bash
./dist/pg-aegis input.sql output.sql
```

### Batch Directory Mode
```bash
./dist/pg-aegis ./path/to/postgres_sql ./path/to/athena_sql
```

### Write a JSON Report
```bash
./dist/pg-aegis input.sql output.sql --report report.json
```

### Validate with a Schema
```bash
./dist/pg-aegis input.sql output.sql --schema schema.json
```

`schema.json` should be a JSON object in the format expected by `sqlglot`, for example:

```json
{
  "users": {
    "id": "INT",
    "email": "VARCHAR"
  }
}
```

### Validate in Athena
```bash
./dist/pg-aegis input.sql output.sql \
  --validate \
  --athena-database analytics \
  --athena-workgroup primary \
  --athena-output-location s3://my-bucket/athena-results/
```

## Validation

- Source SQL is parsed as PostgreSQL before conversion. Invalid PostgreSQL syntax fails conversion.
- Converted SQL is always checked for built-in Athena limitation warnings.
- `--schema` enables schema-aware offline validation of the converted SQL.
- `--validate` runs Athena validation with `EXPLAIN (TYPE VALIDATE)` through the AWS CLI.

## Development
```bash
# Run tests, linting, type checking, and build executable
make all
```
