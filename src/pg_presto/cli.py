#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TextIO

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
    sql = re.sub(
        r"JSONB_ARRAY_ELEMENTS_TEXT\(([^)]+)\)\s+(?:AS\s+)?([a-zA-Z0-9_]+)",
        r"UNNEST(CAST(\1 AS ARRAY(VARCHAR))) AS _t(\2)",
        sql,
        flags=re.IGNORECASE,
    )

    # Handle @> operator (naive replacement)
    sql = re.sub(
        r"([A-Za-z0-9_().]+)\s*@>\s*'([^']+)'(?:::jsonb|::json)?",
        r"CAST(\1 AS VARCHAR) LIKE '%\2%'",
        sql,
        flags=re.IGNORECASE,
    )

    return sql


def convert_sql(sql: str, *, apply_rewrites: bool) -> str:
    working = rewrite_common_postgresisms(sql) if apply_rewrites else sql
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

        # Stored procedures
        if isinstance(expression, exp.Create) and str(expression.args.get("kind")).upper() == "PROCEDURE":
            warnings.append("Stored procedures are not supported in Athena.")
        if isinstance(expression, exp.Command) and str(expression.this).upper() == "CALL":
            warnings.append("Stored procedures (CALL) are not supported in Athena.")

        # Unsupported statements
        if isinstance(expression, exp.Update):
            warnings.append("UPDATE statements are generally not supported in Athena (except for transactional tables).")
        if isinstance(expression, exp.Delete):
            warnings.append("DELETE statements are generally not supported in Athena (except for transactional tables).")

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
                    "SEMANTIC TRAP: EXTRACT(DOW) in Athena returns 1-7 (Mon-Sun), "
                    "unlike PostgreSQL's 0-6 (Sun-Sat)."
                )

        for g_node in expression.find_all(exp.Greatest):
            warnings.append(
                "SEMANTIC TRAP: GREATEST returns NULL if ANY argument is NULL in Athena. " "PostgreSQL ignores NULLs."
            )
        for l_node in expression.find_all(exp.Least):
            warnings.append(
                "SEMANTIC TRAP: LEAST returns NULL if ANY argument is NULL in Athena. " "PostgreSQL ignores NULLs."
            )

        # DDL Traps
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

        # DML Traps
        for insert_node in expression.find_all(exp.Insert):
            if insert_node.args.get("conflict"):
                warnings.append(
                    "DML TRAP: 'ON CONFLICT' is not supported in Athena. " "Use MERGE INTO for upserts (Iceberg only)."
                )
            if insert_node.args.get("returning"):
                warnings.append("DML TRAP: 'RETURNING' clauses are not supported in Athena.")

    return list(dict.fromkeys(warnings))


def process_sql(
    input_sql: str, source_name: str, apply_rewrites: bool, output_stream: TextIO | None = None
) -> ConversionResult:
    try:
        converted = convert_sql(input_sql, apply_rewrites=apply_rewrites)
        warnings = validate_athena_limitations(input_sql, converted)
        if output_stream:
            output_stream.write(converted + "\n")
        return ConversionResult(
            source=source_name,
            destination="STDOUT" if output_stream == sys.stdout else None,
            success=True,
            warnings=warnings,
        )
    except Exception as exc:
        return ConversionResult(
            source=source_name,
            destination=None,
            success=False,
            error=str(exc),
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert PostgreSQL SQL to Athena/Presto SQL.")
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
        "--no-rewrites",
        action="store_true",
        help="Disable extra regex-based rewrites",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional path to write a JSON conversion report",
    )

    args = parser.parse_args()

    results: list[ConversionResult] = []
    apply_rewrites = not args.no_rewrites

    # Handle STDIN
    if args.source == "-":
        input_sql = sys.stdin.read()
        res = process_sql(input_sql, "STDIN", apply_rewrites, sys.stdout)
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
                    res = process_sql(input_sql, str(sql_file), apply_rewrites, f)
                    res.destination = str(dst)
                    results.append(res)
        else:
            # Single File Conversion
            input_sql = src_path.read_text(encoding="utf-8")
            if args.output == "-":
                res = process_sql(input_sql, str(src_path), apply_rewrites, sys.stdout)
                results.append(res)
            else:
                dst = Path(args.output).resolve()
                dst.parent.mkdir(parents=True, exist_ok=True)
                with dst.open("w", encoding="utf-8") as f:
                    res = process_sql(input_sql, str(src_path), apply_rewrites, f)
                    res.destination = str(dst)
                    results.append(res)

    success_count = sum(1 for r in results if r.success)
    fail_count = len(results) - success_count
    warning_count = sum(len(r.warnings) for r in results if r.success)

    # Only print summary to stderr if we are using STDOUT for SQL
    summary_stream = sys.stderr if args.output == "-" else sys.stdout
    
    if len(results) > 1 or args.output != "-":
        print(f"converted: {success_count}", file=summary_stream)
        print(f"warnings:  {warning_count}", file=summary_stream)
        print(f"failed:    {fail_count}", file=summary_stream)

    if warning_count:
        print("\nwarnings:", file=summary_stream)
        for result in results:
            if result.success and result.warnings:
                print(f"- {result.source}", file=summary_stream)
                for w in result.warnings:
                    print(f"  WARNING: {w}", file=summary_stream)

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

    return 1 if fail_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
