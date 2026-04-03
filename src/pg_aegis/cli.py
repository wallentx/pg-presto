#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional, TextIO, cast

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError
from sqlglot.optimizer.qualify import qualify

# Athena engine version 3 is Trino-based, but sqlglot's presto dialect is currently
# the more practical offline approximation for this project. It tolerates some
# pass-through constructs we still need to inspect and warn on, such as unsupported
# DDL that Athena-specific validation should flag later.
TARGET_DIALECT = "presto"
UNSUPPORTED_POSTGRES_TABLE_FUNCTIONS = {
    "GENERATE_SUBSCRIPTS",
    "JSONB_EACH",
    "JSONB_EACH_TEXT",
    "JSONB_OBJECT_KEYS",
    "JSONB_POPULATE_RECORDSET",
    "JSONB_TO_RECORDSET",
    "JSON_ARRAY_ELEMENTS",
    "JSON_ARRAY_ELEMENTS_TEXT",
    "JSON_POPULATE_RECORDSET",
    "JSON_TO_RECORDSET",
    "JSONB_ARRAY_ELEMENTS",
    "JSON_EACH",
    "JSON_EACH_TEXT",
    "JSON_OBJECT_KEYS",
    "REGEXP_SPLIT_TO_TABLE",
}
UNSUPPORTED_POSTGRES_SCALAR_FUNCTIONS = {
    "REGEXP_MATCHES",
}


@dataclass
class ConversionResult:
    source: str
    destination: str | None
    success: bool
    error: str | None = None
    warnings: list[str] = field(default_factory=list)
    validation_errors: list[str] = field(default_factory=list)


@dataclass
class AthenaValidationConfig:
    database: str | None = None
    catalog: str | None = None
    workgroup: str | None = None
    output_location: str | None = None
    poll_interval_seconds: float = 1.0


def iter_sql_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.sql"):
        if path.is_file():
            yield path


def _parse_sql_expressions(sql: str, *, read: str) -> list[exp.Expression]:
    return [cast(exp.Expression, expression) for expression in sqlglot.parse(sql, read=read) if expression is not None]


def _load_sqlglot_schema(schema_path: Path | None) -> dict[str, Any] | None:
    if schema_path is None:
        return None

    loaded = json.loads(schema_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("schema file must contain a JSON object")
    return cast(dict[str, Any], loaded)


def _is_json_type(data_type: exp.DataType | None) -> bool:
    return data_type is not None and data_type.is_type("JSON", "JSONB")


def _normalize_postgres_expression(node: exp.Expression) -> exp.Expression:
    def _rewrite(expression: exp.Expression) -> exp.Expression:
        if isinstance(expression, exp.Lambda):
            return cast(exp.Expression, sqlglot.parse_one(expression.sql(dialect="postgres"), read="postgres"))
        return expression

    return cast(exp.Expression, node.transform(_rewrite))


def _build_jsonb_array_unnest_alias(alias: exp.TableAlias | None) -> exp.TableAlias | None:
    if alias is None:
        return None

    if alias.columns:
        columns = [column.copy() for column in alias.columns]
    elif alias.this is not None:
        columns = [alias.this.copy()]
    else:
        columns = []

    return exp.TableAlias(this=exp.to_identifier("_t"), columns=columns)


def _rewrite_jsonb_array_elements_table(node: exp.Table) -> exp.Expression:
    if not isinstance(node.this, exp.Anonymous):
        return node

    if str(node.this.this).upper() != "JSONB_ARRAY_ELEMENTS_TEXT" or len(node.this.expressions) != 1:
        return node

    return exp.Unnest(
        expressions=[exp.cast(_normalize_postgres_expression(node.this.expressions[0].copy()), "ARRAY<VARCHAR>")],
        alias=_build_jsonb_array_unnest_alias(node.args.get("alias")),
        offset=False,
    )


def _unwrap_json_expression(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Cast) and _is_json_type(node.args.get("to")):
        return cast(exp.Expression, node.this.copy())

    return node.copy()


def _build_contains_pattern(node: exp.Expression) -> exp.Expression:
    normalized = _unwrap_json_expression(node)
    if isinstance(normalized, exp.Literal) and normalized.is_string:
        return exp.Literal.string(f"%{normalized.this}%")

    return cast(
        exp.Expression,
        exp.func(
            "CONCAT",
            exp.Literal.string("%"),
            exp.cast(normalized, "VARCHAR"),
            exp.Literal.string("%"),
        ),
    )


def _rewrite_array_contains_all(node: exp.ArrayContainsAll) -> exp.Expression:
    rhs = node.args.get("expression")
    assert rhs is not None

    return exp.Like(
        this=exp.cast(cast(exp.Expression, node.this.copy()), "VARCHAR"),
        expression=_build_contains_pattern(rhs),
    )


def rewrite_common_postgresisms(expression: exp.Expression) -> exp.Expression:
    def _rewrite(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.ArrayContainsAll):
            return _rewrite_array_contains_all(node)
        if isinstance(node, exp.Table):
            return _rewrite_jsonb_array_elements_table(node)
        return node

    return cast(exp.Expression, expression.transform(_rewrite))


def convert_sql(sql: str, *, apply_rewrites: bool) -> str:
    if not apply_rewrites:
        return ";\n\n".join(
            sqlglot.transpile(sql, read="postgres", write=TARGET_DIALECT, identify=False, pretty=True)
        )

    statements = [rewrite_common_postgresisms(statement) for statement in _parse_sql_expressions(sql, read="postgres")]
    return ";\n\n".join(statement.sql(dialect=TARGET_DIALECT, identify=False, pretty=True) for statement in statements)


def validate_athena_limitations(original_sql: str, transpiled_sql: str) -> list[str]:
    warnings: list[str] = []
    try:
        original_expressions = _parse_sql_expressions(original_sql, read="postgres")
    except Exception:
        original_expressions = []

    try:
        expressions = _parse_sql_expressions(transpiled_sql, read=TARGET_DIALECT)
    except Exception:
        return warnings

    for expression in expressions:
        if not expression:
            continue

        # Stored procedures
        if isinstance(expression, exp.Create) and str(expression.args.get("kind")).upper() == "PROCEDURE":
            warnings.append("Stored procedures are not supported in Athena.")
        if isinstance(expression, exp.Command) and str(expression.this).upper() == "CALL":
            warnings.append("Stored procedures (CALL) are not supported in Athena.")

        # Unsupported statements
        if isinstance(expression, exp.Update):
            warnings.append(
                "UPDATE statements are generally not supported in Athena (except for transactional tables)."
            )
        if isinstance(expression, exp.Delete):
            warnings.append(
                "DELETE statements are generally not supported in Athena (except for transactional tables)."
            )

        # LIMIT clause maximum with ORDER BY
        if isinstance(expression, exp.Select):
            limit_arg = expression.args.get("limit")
            order_arg = expression.args.get("order")
            if limit_arg and order_arg:
                limit_exp = limit_arg.expression
                if isinstance(limit_exp, exp.Literal) and limit_exp.is_int:
                    if int(limit_exp.this) > 2147483647:
                        warnings.append("ORDER BY LIMIT > 2147483647 is not supported in Athena.")

        # Array initializations > 254
        for array_node in expression.find_all(exp.Array):
            if len(array_node.expressions) > 254:
                warnings.append("Array initializations with more than 254 arguments are not supported in Athena.")

        # Semantic Traps
        for extract_node in expression.find_all(exp.Extract):
            if str(extract_node.this).upper() in ("DOW", "ISODOW", "DAYOFWEEK"):
                warnings.append(
                    "SEMANTIC TRAP: EXTRACT(DOW) in Athena returns 1-7 (Mon-Sun), unlike PostgreSQL's 0-6 (Sun-Sat)."
                )

        for g_node in expression.find_all(exp.Greatest):
            warnings.append(
                "SEMANTIC TRAP: GREATEST returns NULL if ANY argument is NULL in Athena. PostgreSQL ignores NULLs."
            )
        for l_node in expression.find_all(exp.Least):
            warnings.append(
                "SEMANTIC TRAP: LEAST returns NULL if ANY argument is NULL in Athena. PostgreSQL ignores NULLs."
            )

        for constraint_node in expression.find_all(exp.ColumnConstraint):
            if str(constraint_node.kind).upper() == "PRIMARY KEY":
                warnings.append("DDL TRAP: 'PRIMARY KEY' constraints are not supported in Athena.")

        # DML Traps
        for insert_node in expression.find_all(exp.Insert):
            if insert_node.args.get("conflict"):
                warnings.append(
                    "DML TRAP: 'ON CONFLICT' is not supported in Athena. Use MERGE INTO for upserts (Iceberg only)."
                )
            if insert_node.args.get("returning"):
                warnings.append("DML TRAP: 'RETURNING' clauses are not supported in Athena.")

    for expression in original_expressions:
        for data_type in expression.find_all(exp.DataType):
            data_type_name = str(data_type.this).upper()
            if data_type_name == "SERIAL":
                warnings.append(
                    "DDL TRAP: Type 'SERIAL' is not supported in Athena. Use INT/BIGINT and sequence strategies."
                )
            if data_type_name == "UUID":
                warnings.append("DDL TRAP: Type 'UUID' is not supported in Athena. Use VARCHAR.")
            if data_type_name == "JSONB":
                warnings.append("DDL TRAP: Type 'JSONB' is not supported in Athena. Use JSON or VARCHAR.")

        for function_node in expression.find_all(exp.Anonymous):
            function_name = str(function_node.this).upper()
            if function_name in UNSUPPORTED_POSTGRES_TABLE_FUNCTIONS | UNSUPPORTED_POSTGRES_SCALAR_FUNCTIONS:
                warnings.append(
                    "PASS-THROUGH TRAP: PostgreSQL function "
                    f"'{function_name}' is not automatically rewritten for Athena. Review manually."
                )

        for string_to_array_node in expression.find_all(exp.StringToArray):
            warnings.append(
                "PASS-THROUGH TRAP: PostgreSQL function "
                "'STRING_TO_ARRAY' is not automatically rewritten for Athena. Review manually."
            )

    return list(dict.fromkeys(warnings))


def validate_sqlglot_output(transpiled_sql: str, schema: dict[str, Any] | None) -> list[str]:
    if schema is None:
        return []

    errors: list[str] = []
    expressions = _parse_sql_expressions(transpiled_sql, read=TARGET_DIALECT)

    for expression in expressions:
        try:
            qualify(
                expression.copy(),
                dialect=TARGET_DIALECT,
                schema=schema,
                validate_qualify_columns=True,
                identify=False,
            )
        except Exception as exc:
            errors.append(f"SQLGLOT VALIDATION: {exc}")

    return list(dict.fromkeys(errors))


def _run_aws_cli_json(args: list[str]) -> dict[str, Any]:
    completed = subprocess.run(
        args,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        message = completed.stderr.strip() or completed.stdout.strip() or "aws cli invocation failed"
        raise RuntimeError(message)

    return cast(dict[str, Any], json.loads(completed.stdout))


def _athena_cli_base_args(config: AthenaValidationConfig) -> list[str]:
    if shutil.which("aws") is None:
        raise RuntimeError("aws cli is required for Athena validation")

    args = ["aws", "athena"]
    if config.workgroup:
        args.extend(["--work-group", config.workgroup])
    return args


def _athena_query_context_args(config: AthenaValidationConfig) -> list[str]:
    values: list[str] = []
    if config.database:
        values.append(f"Database={config.database}")
    if config.catalog:
        values.append(f"Catalog={config.catalog}")
    return ["--query-execution-context", ",".join(values)] if values else []


def _athena_result_config_args(config: AthenaValidationConfig) -> list[str]:
    if not config.output_location:
        return []
    return ["--result-configuration", f"OutputLocation={config.output_location}"]


def _athena_validate_statement(statement_sql: str, config: AthenaValidationConfig) -> str | None:
    query = f"EXPLAIN (TYPE VALIDATE) {statement_sql.rstrip().rstrip(';')}"
    start_args = [
        *_athena_cli_base_args(config),
        "start-query-execution",
        "--query-string",
        query,
        *_athena_query_context_args(config),
        *_athena_result_config_args(config),
    ]
    start_response = _run_aws_cli_json(start_args)
    query_execution_id = cast(Optional[str], start_response.get("QueryExecutionId"))
    if not query_execution_id:
        raise RuntimeError("aws athena did not return QueryExecutionId")

    while True:
        execution = _run_aws_cli_json(
            [
                *_athena_cli_base_args(config),
                "get-query-execution",
                "--query-execution-id",
                query_execution_id,
            ]
        )
        query_execution = cast(dict[str, Any], execution["QueryExecution"])
        status = cast(dict[str, Any], query_execution["Status"])
        state = cast(str, status["State"])

        if state == "SUCCEEDED":
            return None
        if state in {"FAILED", "CANCELLED"}:
            reason = cast(Optional[str], status.get("StateChangeReason"))
            return reason or f"Athena validation ended with state {state}"

        time.sleep(config.poll_interval_seconds)


def validate_athena_output(transpiled_sql: str, config: AthenaValidationConfig) -> list[str]:
    errors: list[str] = []
    expressions = _parse_sql_expressions(transpiled_sql, read=TARGET_DIALECT)

    for expression in expressions:
        statement_sql = expression.sql(dialect=TARGET_DIALECT, identify=False, pretty=False)
        error = _athena_validate_statement(statement_sql, config)
        if error:
            errors.append(f"ATHENA VALIDATION: {error}")

    return errors


def process_sql(
    input_sql: str,
    source_name: str,
    apply_rewrites: bool,
    output_stream: TextIO | None = None,
    sqlglot_schema: dict[str, Any] | None = None,
    athena_validation: AthenaValidationConfig | None = None,
) -> ConversionResult:
    try:
        converted = convert_sql(input_sql, apply_rewrites=apply_rewrites)
        warnings = validate_athena_limitations(input_sql, converted)
        validation_errors = validate_sqlglot_output(converted, sqlglot_schema)
        if athena_validation is not None:
            validation_errors.extend(validate_athena_output(converted, athena_validation))
        if output_stream:
            output_stream.write(converted + "\n")
        return ConversionResult(
            source=source_name,
            destination="STDOUT" if output_stream == sys.stdout else None,
            success=True,
            warnings=warnings,
            validation_errors=list(dict.fromkeys(validation_errors)),
        )
    except ParseError as exc:
        return ConversionResult(
            source=source_name,
            destination=None,
            success=False,
            error=f"Invalid PostgreSQL SQL: {exc}",
        )
    except Exception as exc:
        return ConversionResult(
            source=source_name,
            destination=None,
            success=False,
            error=str(exc),
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert PostgreSQL SQL to Athena-compatible SQL.")
    parser.add_argument(
        "source",
        nargs="?",
        default="-",
        help="Source directory, file, or '-' for STDIN (default: STDIN)",
    )
    parser.add_argument(
        "output",
        nargs="?",
        default="-",
        help="Output directory, file, or '-' for STDOUT (default: STDOUT)",
    )
    parser.add_argument(
        "-n",
        "--no-rewrites",
        action="store_true",
        help="Disable extra regex-based rewrites",
    )
    parser.add_argument(
        "-r",
        "--report",
        type=Path,
        default=None,
        help="Optional path to write a JSON conversion report",
    )
    parser.add_argument(
        "-s",
        "--schema",
        type=Path,
        default=None,
        help="Optional JSON schema for sqlglot column validation",
    )
    parser.add_argument(
        "-v",
        "--validate",
        action="store_true",
        help="Validate converted SQL in Athena with EXPLAIN (TYPE VALIDATE)",
    )
    parser.add_argument(
        "-d",
        "--athena-database",
        default=None,
        help="Athena database for --validate",
    )
    parser.add_argument(
        "-c",
        "--athena-catalog",
        default=None,
        help="Athena catalog for --validate",
    )
    parser.add_argument(
        "-w",
        "--athena-workgroup",
        default=None,
        help="Athena workgroup for --validate",
    )
    parser.add_argument(
        "-o",
        "--athena-output-location",
        default=None,
        help="Athena result output location (for example s3://bucket/prefix/) for --validate",
    )
    parser.add_argument(
        "-p",
        "--athena-poll-interval",
        type=float,
        default=1.0,
        help="Polling interval in seconds for Athena validation",
    )

    args = parser.parse_args()

    results: list[ConversionResult] = []
    apply_rewrites = not args.no_rewrites
    sqlglot_schema = _load_sqlglot_schema(args.schema)
    athena_validation = (
        AthenaValidationConfig(
            database=args.athena_database,
            catalog=args.athena_catalog,
            workgroup=args.athena_workgroup,
            output_location=args.athena_output_location,
            poll_interval_seconds=args.athena_poll_interval,
        )
        if args.validate
        else None
    )

    # Handle STDIN
    if args.source == "-":
        input_sql = sys.stdin.read()
        res = process_sql(
            input_sql,
            "STDIN",
            apply_rewrites,
            sys.stdout,
            sqlglot_schema=sqlglot_schema,
            athena_validation=athena_validation,
        )
        results.append(res)
    else:
        src_path = Path(args.source).resolve()
        if not src_path.exists():
            print(f"error: source does not exist: {src_path}", file=sys.stderr)
            return 2

        if src_path.is_dir():
            # Batch Directory Conversion
            if args.output == "-":
                print("error: output must be a directory when source is a directory", file=sys.stderr)
                return 2

            out_root = Path(args.output).resolve()
            for sql_file in iter_sql_files(src_path):
                rel = sql_file.relative_to(src_path)
                dst = out_root / rel
                dst.parent.mkdir(parents=True, exist_ok=True)

                input_sql = sql_file.read_text(encoding="utf-8")
                with dst.open("w", encoding="utf-8") as f:
                    res = process_sql(
                        input_sql,
                        str(sql_file),
                        apply_rewrites,
                        f,
                        sqlglot_schema=sqlglot_schema,
                        athena_validation=athena_validation,
                    )
                    res.destination = str(dst)
                    results.append(res)
        else:
            # Single File Conversion
            input_sql = src_path.read_text(encoding="utf-8")
            if args.output == "-":
                res = process_sql(
                    input_sql,
                    str(src_path),
                    apply_rewrites,
                    sys.stdout,
                    sqlglot_schema=sqlglot_schema,
                    athena_validation=athena_validation,
                )
                results.append(res)
            else:
                dst = Path(args.output).resolve()
                dst.parent.mkdir(parents=True, exist_ok=True)
                with dst.open("w", encoding="utf-8") as f:
                    res = process_sql(
                        input_sql,
                        str(src_path),
                        apply_rewrites,
                        f,
                        sqlglot_schema=sqlglot_schema,
                        athena_validation=athena_validation,
                    )
                    res.destination = str(dst)
                    results.append(res)

    success_count = sum(1 for r in results if r.success)
    fail_count = len(results) - success_count
    warning_count = sum(len(r.warnings) for r in results if r.success)
    validation_error_count = sum(len(r.validation_errors) for r in results if r.success)

    # Only print summary to stderr if we are using STDOUT for SQL
    summary_stream = sys.stderr if args.output == "-" else sys.stdout

    if len(results) > 1 or args.output != "-":
        print(f"converted: {success_count}", file=summary_stream)
        print(f"warnings:  {warning_count}", file=summary_stream)
        print(f"invalid:   {validation_error_count}", file=summary_stream)
        print(f"failed:    {fail_count}", file=summary_stream)

    if warning_count:
        print("\nwarnings:", file=summary_stream)
        for result in results:
            if result.success and result.warnings:
                print(f"- {result.source}", file=summary_stream)
                for w in result.warnings:
                    print(f"  WARNING: {w}", file=summary_stream)

    if validation_error_count:
        print("\nvalidation failures:", file=summary_stream)
        for result in results:
            if result.success and result.validation_errors:
                print(f"- {result.source}", file=summary_stream)
                for validation_error in result.validation_errors:
                    print(f"  {validation_error}", file=summary_stream)

    if fail_count:
        print("\nfailures:", file=summary_stream)
        for result in results:
            if not result.success:
                print(f"- {result.source}", file=summary_stream)
                print(f"  {result.error}", file=summary_stream)

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps([asdict(r) for r in results], indent=2),
            encoding="utf-8",
        )

    return 1 if fail_count or validation_error_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
