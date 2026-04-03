from __future__ import annotations

import json
import subprocess
import sys
from io import StringIO
from pathlib import Path

from pg_aegis.cli import (
    AthenaValidationConfig,
    ConversionResult,
    convert_sql,
    main,
    process_sql,
    validate_athena_limitations,
    validate_athena_output,
    validate_sqlglot_output,
)


def test_convert_now():
    sql = "SELECT NOW();"
    converted = convert_sql(sql, apply_rewrites=True)
    assert "CURRENT_TIMESTAMP" in converted.upper()


def test_convert_ilike():
    sql = "SELECT * FROM users WHERE name ILIKE '%john%';"
    converted = convert_sql(sql, apply_rewrites=True)
    assert "LOWER(NAME) LIKE LOWER('%JOHN%')" in converted.upper()


def test_convert_casts():
    sql = "SELECT age::int, name::text FROM users;"
    converted = convert_sql(sql, apply_rewrites=True)
    assert "CAST(AGE AS INTEGER)" in converted.upper()
    assert "CAST(NAME AS VARCHAR)" in converted.upper()


def test_convert_intervals():
    sql = "SELECT created_at + INTERVAL '1 days';"
    converted = convert_sql(sql, apply_rewrites=True)
    assert "INTERVAL '1' DAY" in converted.upper()


def test_convert_json():
    sql = "SELECT metadata->>'name', data->'items' FROM table;"
    converted = convert_sql(sql, apply_rewrites=True)
    assert "JSON_EXTRACT_SCALAR(METADATA, '$.NAME')" in converted.upper()
    assert "JSON_EXTRACT(DATA, '$.ITEMS')" in converted.upper()


def test_convert_sql_full():
    sql = "SELECT id::text, name ILIKE 'a%' FROM users WHERE created_at > NOW() - INTERVAL '1 day';"
    converted = convert_sql(sql, apply_rewrites=True)
    assert "CAST(ID AS VARCHAR)" in converted.upper()
    assert "LOWER(NAME) LIKE LOWER('A%')" in converted.upper()
    assert "CURRENT_TIMESTAMP" in converted.upper()
    assert "INTERVAL '1' DAY" in converted.upper()


def test_convert_distinct_on():
    sql = "SELECT DISTINCT ON (customer_id) customer_id, session_id FROM sessions;"
    converted = convert_sql(sql, apply_rewrites=True)
    # sqlglot should convert DISTINCT ON to ROW_NUMBER
    assert "ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID" in converted.upper()


def test_convert_jsonb_array_elements():
    sql = "SELECT * FROM docs, JSONB_ARRAY_ELEMENTS_TEXT(metadata->'tags') AS tag;"
    converted = convert_sql(sql, apply_rewrites=True)
    assert "UNNEST(CAST(JSON_EXTRACT(METADATA, '$.TAGS') AS ARRAY(VARCHAR))) AS _T(TAG)" in converted.upper()


def test_convert_jsonb_contains():
    sql = 'SELECT * FROM docs WHERE metadata @> \'{"a": "b"}\'::jsonb;'
    converted = convert_sql(sql, apply_rewrites=True)
    assert 'CAST(METADATA AS VARCHAR) LIKE \'%{"A": "B"}%\'' in converted.upper()


def test_convert_jsonb_contains_nested_expression():
    sql = 'SELECT * FROM docs WHERE (metadata->\'nested\') @> \'{"a":"b"}\'::jsonb;'
    converted = convert_sql(sql, apply_rewrites=True)
    assert "JSON_EXTRACT(METADATA, '$.NESTED')" in converted.upper()
    assert 'LIKE \'%{"A":"B"}%\'' in converted.upper()


def test_convert_jsonb_array_elements_with_nested_expression():
    sql = "SELECT * FROM docs, JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(metadata->'tags', '[]'::jsonb)) AS tag;"
    converted = convert_sql(sql, apply_rewrites=True)
    assert (
        "UNNEST(CAST(COALESCE(JSON_EXTRACT(METADATA, '$.TAGS'), CAST('[]' AS JSONB)) AS ARRAY(VARCHAR))) AS _T(TAG)"
        in converted.upper()
    )


def test_validate_limitations_ignores_comments():
    sql = "SELECT 1; -- UUID appears only in a comment"
    warnings = validate_athena_limitations(sql, convert_sql(sql, apply_rewrites=True))
    assert "DDL TRAP: Type 'UUID' is not supported in Athena. Use VARCHAR." not in warnings


def test_validate_limitations_warns_on_unsupported_jsonb_each_text():
    sql = "SELECT * FROM events e CROSS JOIN LATERAL JSONB_EACH_TEXT(e.metadata->'attributes') AS attr(key, value);"
    warnings = validate_athena_limitations(sql, convert_sql(sql, apply_rewrites=True))
    assert (
        "PASS-THROUGH TRAP: PostgreSQL function "
        "'JSONB_EACH_TEXT' is not automatically rewritten for Athena. Review manually."
        in warnings
    )


def test_validate_limitations_warns_on_unsupported_jsonb_object_keys():
    sql = "SELECT * FROM events e CROSS JOIN LATERAL JSONB_OBJECT_KEYS(e.metadata) AS k;"
    warnings = validate_athena_limitations(sql, convert_sql(sql, apply_rewrites=True))
    assert (
        "PASS-THROUGH TRAP: PostgreSQL function "
        "'JSONB_OBJECT_KEYS' is not automatically rewritten for Athena. Review manually."
        in warnings
    )


def test_validate_limitations_warns_on_unsupported_jsonb_to_recordset():
    sql = "SELECT * FROM JSONB_TO_RECORDSET('[{\"a\":1}]'::jsonb) AS x(a int);"
    warnings = validate_athena_limitations(sql, convert_sql(sql, apply_rewrites=True))
    assert (
        "PASS-THROUGH TRAP: PostgreSQL function "
        "'JSONB_TO_RECORDSET' is not automatically rewritten for Athena. Review manually."
        in warnings
    )


def test_validate_limitations_warns_on_unsupported_generate_subscripts():
    sql = "SELECT * FROM GENERATE_SUBSCRIPTS(ARRAY[10, 20, 30], 1) AS s(i);"
    warnings = validate_athena_limitations(sql, convert_sql(sql, apply_rewrites=True))
    assert (
        "PASS-THROUGH TRAP: PostgreSQL function "
        "'GENERATE_SUBSCRIPTS' is not automatically rewritten for Athena. Review manually."
        in warnings
    )


def test_validate_limitations_warns_on_unsupported_regexp_split_to_table():
    sql = "SELECT * FROM REGEXP_SPLIT_TO_TABLE('a,b,c', ',') AS x(val);"
    warnings = validate_athena_limitations(sql, convert_sql(sql, apply_rewrites=True))
    assert (
        "PASS-THROUGH TRAP: PostgreSQL function "
        "'REGEXP_SPLIT_TO_TABLE' is not automatically rewritten for Athena. Review manually."
        in warnings
    )


def test_validate_limitations_warns_on_unsupported_string_to_array():
    sql = "SELECT STRING_TO_ARRAY('a,b,c', ',');"
    warnings = validate_athena_limitations(sql, convert_sql(sql, apply_rewrites=True))
    assert (
        "PASS-THROUGH TRAP: PostgreSQL function "
        "'STRING_TO_ARRAY' is not automatically rewritten for Athena. Review manually."
        in warnings
    )


def test_validate_limitations_warns_on_unsupported_regexp_matches():
    sql = "SELECT REGEXP_MATCHES('abc123', '[0-9]+');"
    warnings = validate_athena_limitations(sql, convert_sql(sql, apply_rewrites=True))
    assert (
        "PASS-THROUGH TRAP: PostgreSQL function "
        "'REGEXP_MATCHES' is not automatically rewritten for Athena. Review manually."
        in warnings
    )


def test_validate_sqlglot_output_requires_schema_for_column_checks():
    errors = validate_sqlglot_output("SELECT missing FROM tbl", schema=None)
    assert errors == []


def test_validate_sqlglot_output_with_schema_flags_unresolved_column():
    errors = validate_sqlglot_output("SELECT missing FROM tbl", schema={"tbl": {"col": "INT"}})
    assert len(errors) == 1
    assert "Column 'missing' could not be resolved" in errors[0]


def test_validate_athena_output_success(monkeypatch):
    responses = [
        {"QueryExecutionId": "query-123"},
        {"QueryExecution": {"Status": {"State": "QUEUED"}}},
        {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
    ]

    def fake_run(*args, **kwargs):
        payload = responses.pop(0)
        return subprocess.CompletedProcess(args[0], 0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr("pg_aegis.cli.shutil.which", lambda cmd: "/usr/bin/aws")
    monkeypatch.setattr("pg_aegis.cli.subprocess.run", fake_run)
    monkeypatch.setattr("pg_aegis.cli.time.sleep", lambda _: None)

    errors = validate_athena_output("SELECT 1", AthenaValidationConfig(poll_interval_seconds=0))
    assert errors == []


def test_validate_athena_output_failure(monkeypatch):
    responses = [
        {"QueryExecutionId": "query-123"},
        {
            "QueryExecution": {
                "Status": {
                    "State": "FAILED",
                    "StateChangeReason": "line 1:8: Table does not exist",
                }
            }
        },
    ]

    def fake_run(*args, **kwargs):
        payload = responses.pop(0)
        return subprocess.CompletedProcess(args[0], 0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr("pg_aegis.cli.shutil.which", lambda cmd: "/usr/bin/aws")
    monkeypatch.setattr("pg_aegis.cli.subprocess.run", fake_run)

    errors = validate_athena_output("SELECT * FROM missing_table", AthenaValidationConfig(poll_interval_seconds=0))
    assert errors == ["ATHENA VALIDATION: line 1:8: Table does not exist"]


def test_process_sql_reports_invalid_postgres_sql_with_rewrites():
    result = process_sql("SELECT FROM", "STDIN", apply_rewrites=True)

    assert result.success is False
    assert result.error is not None
    assert result.error.startswith("Invalid PostgreSQL SQL:")


def test_process_sql_reports_invalid_postgres_sql_without_rewrites():
    result = process_sql("SELECT FROM", "STDIN", apply_rewrites=False)

    assert result.success is False
    assert result.error is not None
    assert result.error.startswith("Invalid PostgreSQL SQL:")


def test_main_accepts_short_options(tmp_path, monkeypatch):
    source = tmp_path / "input.sql"
    source.write_text("SELECT NOW();", encoding="utf-8")

    schema = tmp_path / "schema.json"
    schema.write_text(json.dumps({"tbl": {"col": "INT"}}), encoding="utf-8")

    report = tmp_path / "report.json"
    captured: dict[str, object] = {}

    def fake_process_sql(
        input_sql,
        source_name,
        apply_rewrites,
        output_stream=None,
        sqlglot_schema=None,
        athena_validation=None,
    ):
        captured["input_sql"] = input_sql
        captured["source_name"] = source_name
        captured["apply_rewrites"] = apply_rewrites
        captured["sqlglot_schema"] = sqlglot_schema
        captured["athena_validation"] = athena_validation
        return ConversionResult(source=source_name, destination=None, success=True)

    monkeypatch.setattr("pg_aegis.cli.process_sql", fake_process_sql)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pg-aegis",
            "-n",
            "-r",
            str(report),
            "-s",
            str(schema),
            "-v",
            "-d",
            "db",
            "-c",
            "catalog",
            "-w",
            "wg",
            "-o",
            "s3://bucket/results/",
            "-p",
            "0.25",
            str(source),
            "-",
        ],
    )

    exit_code = main()

    assert exit_code == 0
    assert captured["input_sql"] == "SELECT NOW();"
    assert captured["apply_rewrites"] is False
    assert captured["sqlglot_schema"] == {"tbl": {"col": "INT"}}
    athena_validation = captured["athena_validation"]
    assert isinstance(athena_validation, AthenaValidationConfig)
    assert athena_validation.database == "db"
    assert athena_validation.catalog == "catalog"
    assert athena_validation.workgroup == "wg"
    assert athena_validation.output_location == "s3://bucket/results/"
    assert athena_validation.poll_interval_seconds == 0.25
    assert report.exists()


def test_examples_convert_successfully():
    examples_dir = Path(__file__).resolve().parents[1] / "examples" / "postgresql"

    for example_path in sorted(examples_dir.glob("*.sql")):
        input_sql = example_path.read_text(encoding="utf-8")
        result = process_sql(input_sql, str(example_path), apply_rewrites=True)
        assert result.success, f"{example_path.name}: {result.error}"


def test_example_goldens():
    base_dir = Path(__file__).resolve().parents[1]
    examples_dir = base_dir / "examples" / "postgresql"
    golden_dir = Path(__file__).resolve().parent / "golden"

    for name in [
        "cte_windowing",
        "nested_json_edge_cases",
        "time_series",
        "unsupported_recordset_functions",
        "unsupported_text_functions",
        "unsupported_json_table_functions",
    ]:
        input_sql = (examples_dir / f"{name}.sql").read_text(encoding="utf-8")
        output_buffer = StringIO()

        result = process_sql(input_sql, name, apply_rewrites=True, output_stream=output_buffer)

        assert result.success is True
        assert output_buffer.getvalue() == (golden_dir / f"{name}.out.sql").read_text(encoding="utf-8")

        warnings_path = golden_dir / f"{name}.warnings.txt"
        expected_warnings = warnings_path.read_text(encoding="utf-8").splitlines() if warnings_path.exists() else []
        assert result.warnings == expected_warnings


def test_unsupported_json_table_functions_example_emits_warnings():
    example_path = (
        Path(__file__).resolve().parents[1]
        / "examples"
        / "postgresql"
        / "unsupported_json_table_functions.sql"
    )
    input_sql = example_path.read_text(encoding="utf-8")

    result = process_sql(input_sql, str(example_path), apply_rewrites=True)

    assert result.success is True
    assert (
        "PASS-THROUGH TRAP: PostgreSQL function "
        "'JSONB_EACH_TEXT' is not automatically rewritten for Athena. Review manually."
        in result.warnings
    )


def test_unsupported_recordset_functions_example_emits_warnings():
    example_path = (
        Path(__file__).resolve().parents[1]
        / "examples"
        / "postgresql"
        / "unsupported_recordset_functions.sql"
    )
    input_sql = example_path.read_text(encoding="utf-8")

    result = process_sql(input_sql, str(example_path), apply_rewrites=True)

    assert result.success is True
    assert (
        "PASS-THROUGH TRAP: PostgreSQL function "
        "'JSONB_TO_RECORDSET' is not automatically rewritten for Athena. Review manually."
        in result.warnings
    )


def test_unsupported_text_functions_example_emits_warnings():
    example_path = (
        Path(__file__).resolve().parents[1]
        / "examples"
        / "postgresql"
        / "unsupported_text_functions.sql"
    )
    input_sql = example_path.read_text(encoding="utf-8")

    result = process_sql(input_sql, str(example_path), apply_rewrites=True)

    assert result.success is True
    assert (
        "PASS-THROUGH TRAP: PostgreSQL function "
        "'REGEXP_SPLIT_TO_TABLE' is not automatically rewritten for Athena. Review manually."
        in result.warnings
    )
    assert (
        "PASS-THROUGH TRAP: PostgreSQL function "
        "'STRING_TO_ARRAY' is not automatically rewritten for Athena. Review manually."
        in result.warnings
    )
    assert (
        "PASS-THROUGH TRAP: PostgreSQL function "
        "'REGEXP_MATCHES' is not automatically rewritten for Athena. Review manually."
        in result.warnings
    )
