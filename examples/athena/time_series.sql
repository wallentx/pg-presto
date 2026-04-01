/* Demonstrates DISTINCT ON, DATE_TRUNC, and timezone conversions */
SELECT
  customer_id,
  session_id,
  session_month,
  duration_seconds,
  total_pageviews
FROM (
  /* Demonstrates DISTINCT ON, DATE_TRUNC, and timezone conversions */
  SELECT
    customer_id AS customer_id,
    session_id AS session_id,
    DATE_TRUNC('MONTH', AT_TIMEZONE(created_at, 'UTC')) AS session_month,
    TO_UNIXTIME(CAST((
      ended_at - created_at
    ) AS TIMESTAMP)) AS duration_seconds,
    total_pageviews AS total_pageviews,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id, created_at DESC NULLS FIRST) AS _row_number
  FROM web_sessions
  WHERE
    created_at >= CURRENT_TIMESTAMP - INTERVAL '6' MONTH
) AS _t
WHERE
  _row_number = 1
