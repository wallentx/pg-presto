-- Demonstrates PostgreSQL regex operators and string splitting
SELECT
    request_id,
    path,
    SPLIT_PART(path, '/', 2) AS api_version,
    REGEXP_REPLACE(user_agent, '([0-9]+)\.[0-9]+', '\1.x') AS generalized_agent
FROM http_logs
WHERE
    -- Case insensitive regex match for paths starting with /api/v
    path ~* '^/api/v[0-9]+/'
    -- Case sensitive regex match avoiding internal IPs
    AND client_ip !~ '^10\.'
    AND status_code = 500;
