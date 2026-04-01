-- Demonstrates NOW(), INTERVAL, and ::type casting
SELECT
    account_id,
    amount_cents::bigint AS amount_long,
    (amount_cents / 100.0)::varchar AS amount_string,
    status
FROM invoices
WHERE
    due_date > NOW() - INTERVAL '30 days'
    AND paid_at IS NOT NULL;
