from pg_presto.cli import convert_sql


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
    sql = "SELECT * FROM docs WHERE metadata @> '{\"a\": \"b\"}'::jsonb;"
    converted = convert_sql(sql, apply_rewrites=True)
    assert "CAST(METADATA AS VARCHAR) LIKE '%{\"A\": \"B\"}%'" in converted.upper()
