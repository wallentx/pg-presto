/* A realistic report combining multiple PostgreSQL-specific features */
SELECT
  u.id,
  u.name,
  JSON_EXTRACT_SCALAR(p.profile_data, '$.department') AS department,
  CAST((
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(u.settings, '$.notifications'), '$.enabled')
  ) AS BOOLEAN) AS notifications_enabled,
  CAST(COALESCE(s.last_login, CURRENT_TIMESTAMP) AS DATE) AS active_date
FROM users AS u
JOIN profiles AS p
  ON u.id = p.user_id
LEFT JOIN stats AS s
  ON u.id = s.user_id
WHERE
  LOWER(u.name) LIKE LOWER('%Corp%')
  AND u.created_at >= CURRENT_TIMESTAMP - INTERVAL '1' YEAR
  AND JSON_EXTRACT_SCALAR(p.profile_data, '$.type') <> 'guest'
