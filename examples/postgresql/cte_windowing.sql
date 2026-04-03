-- Demonstrates CTEs, aggregate FILTER clauses, ILIKE rewrites, and window functions
WITH sessionized AS (
    SELECT
        user_id,
        DATE_TRUNC('day', occurred_at) AS day_bucket,
        COUNT(*) FILTER (WHERE event_name ILIKE '%login%') AS login_events,
        MAX(occurred_at) AS last_event_at
    FROM raw_events
    GROUP BY user_id, DATE_TRUNC('day', occurred_at)
), ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY last_event_at DESC NULLS LAST) AS rn
    FROM sessionized
)
SELECT
    user_id,
    day_bucket,
    login_events,
    last_event_at
FROM ranked
WHERE rn = 1;
