/* Demonstrates advanced array handling, string aggregations, and the ANY() operator */
SELECT
  department_id,
  ARRAY_AGG(employee_id ORDER BY hire_date DESC NULLS FIRST) AS recent_hires,
  ARRAY_JOIN(
    ARRAY_AGG(
      CONCAT(CAST(first_name AS VARCHAR), CAST(' ' AS VARCHAR), CAST(last_name AS VARCHAR))
    ),
    ', '
  ) AS employee_names,
  COUNT(*) AS total_employees
FROM employees
WHERE
  'engineering' = ANY(
    skills_array
  ) AND status <> 'terminated'
GROUP BY
  department_id
HAVING
  CARDINALITY(ARRAY_AGG(employee_id)) > 5
