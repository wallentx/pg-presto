/* Demonstrates JSONB array expansion and aggregation */
SELECT
  doc_id,
  tag,
  COUNT(*) AS tag_frequency
FROM documents, UNNEST(CAST(JSON_EXTRACT(metadata, '$.tags') AS ARRAY(VARCHAR))) AS _t(tag)
WHERE
  status = 'published'
  AND CAST(metadata AS VARCHAR) LIKE '%{"visibility": "public"}%'
GROUP BY
  doc_id,
  tag
HAVING
  COUNT(*) > 1
