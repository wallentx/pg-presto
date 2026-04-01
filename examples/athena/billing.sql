/* Demonstrates NOW(), INTERVAL, and ::type casting */
SELECT
  account_id,
  CAST(amount_cents AS BIGINT) AS amount_long,
  CAST((
    amount_cents / 100.0
  ) AS VARCHAR) AS amount_string,
  status
FROM invoices
WHERE
  due_date > CURRENT_TIMESTAMP - INTERVAL '30' DAY AND NOT paid_at IS NULL
