-- Demonstrates JSONB array expansion and aggregation
SELECT
    doc_id,
    tag,
    COUNT(*) AS tag_frequency
FROM documents,
    JSONB_ARRAY_ELEMENTS_TEXT(metadata->'tags') AS tag
WHERE
    status = 'published'
    AND metadata @> '{"visibility": "public"}'::jsonb
GROUP BY
    doc_id,
    tag
HAVING
    COUNT(*) > 1;
