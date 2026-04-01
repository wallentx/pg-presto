-- Demonstrates JSON extraction and ILIKE operators
SELECT
    id,
    metadata->>'source' AS event_source,
    data->'user'->>'email' AS user_email,
    event_name
FROM raw_events
WHERE
    event_name ILIKE '%LOGIN%'
    OR metadata->>'type' = 'auth';
