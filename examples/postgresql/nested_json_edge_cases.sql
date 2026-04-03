-- Demonstrates nested JSON extraction, nested @> containment, and JSONB_ARRAY_ELEMENTS_TEXT rewrites
WITH expanded_docs AS (
    SELECT
        d.doc_id,
        tag,
        d.metadata
    FROM documents d,
        JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(d.metadata->'tags', '[]'::jsonb)) AS tag
)
SELECT
    doc_id,
    tag,
    metadata->>'owner' AS owner,
    (metadata->'details'->>'priority')::int AS priority
FROM expanded_docs
WHERE
    (metadata->'details') @> '{"status": "active"}'::jsonb
    AND owner IS NOT NULL;
