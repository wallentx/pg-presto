-- Demonstrates DISTINCT ON, DATE_TRUNC, and timezone conversions
SELECT DISTINCT ON (customer_id)
    customer_id,
    session_id,
    DATE_TRUNC('month', created_at AT TIME ZONE 'UTC') AS session_month,
    EXTRACT(EPOCH FROM (ended_at - created_at)) AS duration_seconds,
    total_pageviews
FROM web_sessions
WHERE
    created_at >= NOW() - INTERVAL '6 months'
ORDER BY
    customer_id,
    created_at DESC;
