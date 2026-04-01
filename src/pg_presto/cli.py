#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from pathlib import Path

import sqlglot
import sqlglot.expressions as exp


@dataclass
class ConversionResult:
    source: str
    destination: str | None
    success: bool
    error: str | None = None
    warnings: list[str] = field(default_factory=list)


def iter_sql_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.sql"):
        if path.is_file():
            yield path


def rewrite_common_postgresisms(sql: str) -> str:
    # Handle JSONB_ARRAY_ELEMENTS_TEXT -> CROSS JOIN UNNEST
    # E.g., JSONB_ARRAY_ELEMENTS_TEXT(metadata->'tags') AS tag ->
    # UNNEST(CAST(JSON_EXTRACT(metadata, '$.tags') AS ARRAY(VARCHAR))) AS t(tag)
    # sqlglot doesn't natively map JSONB_ARRAY_ELEMENTS_TEXT, so we translate it to UNNEST.
    sql = re.sub(
        r"JSONB_ARRAY_ELEMENTS_TEXT\(([^)]+)\)\s+(?:AS\s+)?([a-zA-Z0-9_]+)",
        r"UNNEST(CAST(\1 AS ARRAY(VARCHAR))) AS _t(\2)",
        sql,
        flags=re.IGNORECASE,
    )

    # Note: For @> we do a naive replacement if it looks like metadata @> '{"visibility": "public"}'
    # This is hard to do generally with regex, but we can catch basic string literals.
    # E.g., A @> '{"a":"b"}'::jsonb -> JSON_EXTRACT_SCALAR(A, '$.a') = 'b'
    # For a robust solution, we'll just rewrite it to a generic JSON_EXTRACT check for now,
    # or let the user know @> isn't fully supported. We will do a generic LIKE as a naive fallback if simple:
    # E.g., metadata @> '{"visibility": "public"}'::jsonb -> CAST(metadata AS VARCHAR) LIKE '%\2%'
    sql = re.sub(
        r"([A-Za-z0-9_().]+)\s*@>\s*'([^']+)'(?:::jsonb|::json)?",
        r"CAST(\1 AS VARCHAR) LIKE '%\2%'",
        sql,
        flags=re.IGNORECASE,
    )

    return sql


def convert_sql(sql: str, *, apply_rewrites: bool) -> str:
    working = rewrite_common_postgresisms(sql) if apply_rewrites else sql
    # transpile returns a list of strings (one for each statement)
    statements = sqlglot.transpile(working, read="postgres", write="presto", identify=False, pretty=True)
    return ";\n\n".join(statements)


def validate_athena_limitations(original_sql: str, transpiled_sql: str) -> list[str]:
    warnings: list[str] = []
    try:
        expressions = sqlglot.parse(transpiled_sql, read="presto")
    except Exception:
        return warnings

    for expression in expressions:
        if not expression:
            continue

        # 1. Stored procedures
        if isinstance(expression, exp.Create) and str(expression.args.get("kind")).upper() == "PROCEDURE":
            warnings.append("Stored procedures are not supported in Athena.")
        if isinstance(expression, exp.Command) and str(expression.this).upper() == "CALL":
            warnings.append("Stored procedures (CALL) are not supported in Athena.")

        # 3. Unsupported statements
        if isinstance(expression, exp.Update):
            warnings.append(
                "UPDATE statements are generally not supported in Athena (except for transactional tables)."
            )
        if isinstance(expression, exp.Delete):
            warnings.append(
                "DELETE statements are generally not supported in Athena (except for transactional tables)."
            )

        # 9. LIMIT clause maximum with ORDER BY
        if isinstance(expression, exp.Select):
            limit_arg = expression.args.get("limit")
            order_arg = expression.args.get("order")
            if limit_arg and order_arg:
                limit_exp = limit_arg.expression
                if isinstance(limit_exp, exp.Literal) and limit_exp.is_int:
                    if int(limit_exp.this) > 2147483647:
                        warnings.append("ORDER BY LIMIT > 2147483647 is not supported in Athena.")

        # 11. Array initializations > 254
        for array_node in expression.find_all(exp.Array):
            if len(array_node.expressions) > 254:
                warnings.append("Array initializations with more than 254 arguments are not supported in Athena.")

        # --- Semantic & Pass-Through Traps ---

        # EXTRACT(DOW / ISODOW ...) Semantic mismatch
        for extract_node in expression.find_all(exp.Extract):
            if str(extract_node.this).upper() in ("DOW", "ISODOW", "DAYOFWEEK"):
                warnings.append(
                    "SEMANTIC TRAP: EXTRACT(DOW) in Athena returns 1-7 (Mon-Sun), "
                    "unlike PostgreSQL's 0-6 (Sun-Sat)."
                )

        # GREATEST / LEAST Semantic mismatch (NULL handling)
        for g_node in expression.find_all(exp.Greatest):
            warnings.append(
                "SEMANTIC TRAP: GREATEST returns NULL if ANY argument is NULL in Athena. "
                "PostgreSQL ignores NULLs."
            )
        for l_node in expression.find_all(exp.Least):
            warnings.append(
                "SEMANTIC TRAP: LEAST returns NULL if ANY argument is NULL in Athena. " "PostgreSQL ignores NULLs."
            )

        # DDL Traps: SERIAL, UUID, JSONB types and PRIMARY KEY constraints
        # sqlglot might translate these immediately, so we check the original SQL for types
        # and the expression for constraints
        upper_sql = original_sql.upper()
        if "SERIAL" in upper_sql:
            warnings.append(
                "DDL TRAP: Type 'SERIAL' is not supported in Athena. Use INT/BIGINT and sequence strategies."
            )
        if "UUID" in upper_sql:
            warnings.append("DDL TRAP: Type 'UUID' is not supported in Athena. Use VARCHAR.")
        if "JSONB" in upper_sql:
            warnings.append("DDL TRAP: Type 'JSONB' is not supported in Athena. Use JSON or VARCHAR.")

        for constraint_node in expression.find_all(exp.ColumnConstraint):
            if str(constraint_node.kind).upper() == "PRIMARY KEY":
                warnings.append("DDL TRAP: 'PRIMARY KEY' constraints are not supported in Athena.")

        # DML Traps: ON CONFLICT and RETURNING
        for insert_node in expression.find_all(exp.Insert):
            if insert_node.args.get("conflict"):
                warnings.append(
                    "DML TRAP: 'ON CONFLICT' is not supported in Athena. " "Use MERGE INTO for upserts (Iceberg only)."
                )
            if insert_node.args.get("returning"):
                warnings.append("DML TRAP: 'RETURNING' clauses are not supported in Athena.")

    # Remove duplicate warnings (since we check original SQL inside a loop over expressions)
    return list(dict.fromkeys(warnings))


def convert_file(src: Path, src_root: Path, out_root: Path, *, apply_rewrites: bool) -> ConversionResult:
    rel = src.relative_to(src_root)
    dst = out_root / rel

    try:
        sql = src.read_text(encoding="utf-8")
        converted = convert_sql(sql, apply_rewrites=apply_rewrites)
        warnings = validate_athena_limitations(sql, converted)
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(converted + "\n", encoding="utf-8")
        return ConversionResult(
            source=str(src),
            destination=str(dst),
            success=True,
            warnings=warnings,
        )
    except Exception as exc:
        return ConversionResult(
            source=str(src),
            destination=None,
            success=False,
            error=str(exc),
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Batch convert PostgreSQL SQL files to Athena/Presto SQL.")
    parser.add_argument("source_dir", type=Path, help="Directory containing .sql files")
    parser.add_argument("output_dir", type=Path, help="Directory to write converted .sql files")
    parser.add_argument(
        "--no-rewrites",
        action="store_true",
        help="Disable extra regex-based rewrites for common PostgreSQL syntax",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional path to write a JSON conversion report",
    )

    args = parser.parse_args()

    source_dir = args.source_dir.resolve()
    output_dir = args.output_dir.resolve()

    if not source_dir.exists() or not source_dir.is_dir():
        print(f"error: source_dir does not exist or is not a directory: {source_dir}", file=sys.stderr)
        return 2

    results: list[ConversionResult] = []
    for sql_file in iter_sql_files(source_dir):
        result = convert_file(
            sql_file,
            source_dir,
            output_dir,
            apply_rewrites=not args.no_rewrites,
        )
        results.append(result)

    success_count = sum(1 for r in results if r.success)
    fail_count = len(results) - success_count
    warning_count = sum(len(r.warnings) for r in results if r.success)

    print(f"converted: {success_count}")
    print(f"warnings:  {warning_count}")
    print(f"failed:    {fail_count}")

    if warning_count:
        print("\nwarnings:")
        for result in results:
            if result.success and result.warnings:
                print(f"- {result.source}")
                for w in result.warnings:
                    print(f"  WARNING: {w}")

    if fail_count:
        print("\nfailures:")
        for result in results:
            if not result.success:
                print(f"- {result.source}")
                print(f"  {result.error}")

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps([asdict(r) for r in results], indent=2),
            encoding="utf-8",
        )

    return 1 if fail_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
