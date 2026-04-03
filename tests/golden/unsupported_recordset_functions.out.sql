/* Demonstrates PostgreSQL set-returning functions that are not automatically rewritten for Athena */
SELECT
  *
FROM JSONB_TO_RECORDSET(CAST('[{"a": 1}, {"a": 2}]' AS JSONB)) AS x(a INTEGER);

SELECT
  *
FROM GENERATE_SUBSCRIPTS(ARRAY[10, 20, 30], 1) AS s(i)
