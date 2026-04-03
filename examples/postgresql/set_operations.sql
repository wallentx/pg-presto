-- Demonstrates UNION ALL, EXCEPT, and output column alignment across sources
SELECT
    user_id,
    'events' AS source
FROM raw_events
UNION ALL
SELECT
    account_id AS user_id,
    'billing' AS source
FROM invoices
EXCEPT
SELECT
    user_id,
    'events' AS source
FROM blocked_users;
