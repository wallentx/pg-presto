-- Demonstrates PostgreSQL JSON table functions that are not automatically rewritten for Athena
SELECT
    e.id,
    attr.key,
    attr.value
FROM events e
CROSS JOIN LATERAL JSONB_EACH_TEXT(e.metadata->'attributes') AS attr(key, value)
WHERE e.event_name ILIKE '%purchase%';

SELECT
    e.id,
    k
FROM events e
CROSS JOIN LATERAL JSONB_OBJECT_KEYS(e.metadata) AS k;
