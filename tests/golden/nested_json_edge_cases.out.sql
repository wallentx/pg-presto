/* Demonstrates nested JSON extraction, nested @> containment, and JSONB_ARRAY_ELEMENTS_TEXT rewrites */
WITH expanded_docs AS (
  SELECT
    d.doc_id,
    tag,
    d.metadata
  FROM documents AS d, UNNEST(CAST(COALESCE(JSON_EXTRACT(d.metadata, '$.tags'), CAST('[]' AS JSONB)) AS ARRAY(VARCHAR))) AS _t(tag)
)
SELECT
  doc_id,
  tag,
  JSON_EXTRACT_SCALAR(metadata, '$.owner') AS owner,
  CAST((
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(metadata, '$.details'), '$.priority')
  ) AS INTEGER) AS priority
FROM expanded_docs
WHERE
  CAST((
    JSON_EXTRACT(metadata, '$.details')
  ) AS VARCHAR) LIKE '%{"status": "active"}%'
  AND NOT owner IS NULL
