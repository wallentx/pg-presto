/* Demonstrates PostgreSQL regex operators and string splitting */
SELECT
  request_id,
  path,
  SPLIT_PART(path, '/', 2) AS api_version,
  REGEXP_REPLACE(user_agent, '([0-9]+)\.[0-9]+', '\1.x') AS generalized_agent
FROM http_logs
WHERE
  REGEXP_I_LIKE(
    path /* Case insensitive regex match for paths starting with /api/v */,
    '^/api/v[0-9]+/'
  )
  AND /* Case sensitive regex match avoiding internal IPs */ NOT REGEXP_LIKE(client_ip, '^10\.')
  AND status_code = 500
