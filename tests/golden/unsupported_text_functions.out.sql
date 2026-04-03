/* Demonstrates PostgreSQL text and regex functions that are not automatically rewritten for Athena */
SELECT
  *
FROM REGEXP_SPLIT_TO_TABLE('a,b,c', ',') AS x(val);

SELECT
  STRING_TO_ARRAY('a,b,c', ',') AS parts,
  REGEXP_MATCHES('abc123', '[0-9]+') AS matches
