/* Demonstrates JSON extraction and ILIKE operators */
SELECT
  id,
  JSON_EXTRACT_SCALAR(metadata, '$.source') AS event_source,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(data, '$.user'), '$.email') AS user_email,
  event_name
FROM raw_events
WHERE
  LOWER(event_name) LIKE LOWER('%LOGIN%')
  OR JSON_EXTRACT_SCALAR(metadata, '$.type') = 'auth'
