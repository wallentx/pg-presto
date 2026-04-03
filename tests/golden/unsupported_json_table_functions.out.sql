/* Demonstrates PostgreSQL JSON table functions that are not automatically rewritten for Athena */
SELECT
  e.id,
  attr.key,
  attr.value
FROM events AS e
CROSS JOIN LATERAL JSONB_EACH_TEXT(JSON_EXTRACT(e.metadata, '$.attributes')) AS attr(key, value)
WHERE
  LOWER(e.event_name) LIKE LOWER('%purchase%');

SELECT
  e.id,
  k
FROM events AS e
CROSS JOIN LATERAL JSONB_OBJECT_KEYS(e.metadata) AS k
