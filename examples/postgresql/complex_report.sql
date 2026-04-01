-- A realistic report combining multiple PostgreSQL-specific features
SELECT
    u.id,
    u.name,
    p.profile_data->>'department' AS department,
    (u.settings->'notifications'->>'enabled')::boolean AS notifications_enabled,
    COALESCE(s.last_login, NOW())::date AS active_date
FROM users u
JOIN profiles p ON u.id = p.user_id
LEFT JOIN stats s ON u.id = s.user_id
WHERE
    u.name ILIKE '%Corp%'
    AND u.created_at >= NOW() - INTERVAL '1 year'
    AND p.profile_data->>'type' != 'guest';
