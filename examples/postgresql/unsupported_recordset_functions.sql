-- Demonstrates PostgreSQL set-returning functions that are not automatically rewritten for Athena
SELECT
    *
FROM JSONB_TO_RECORDSET('[{"a": 1}, {"a": 2}]'::jsonb) AS x(a int);

SELECT
    *
FROM GENERATE_SUBSCRIPTS(ARRAY[10, 20, 30], 1) AS s(i);
