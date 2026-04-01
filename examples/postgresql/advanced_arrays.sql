-- Demonstrates advanced array handling, string aggregations, and the ANY() operator
SELECT
    department_id,
    ARRAY_AGG(employee_id ORDER BY hire_date DESC) AS recent_hires,
    STRING_AGG(first_name || ' ' || last_name, ', ') AS employee_names,
    COUNT(*) AS total_employees
FROM employees
WHERE
    'engineering' = ANY(skills_array)
    AND status != 'terminated'
GROUP BY
    department_id
HAVING
    ARRAY_LENGTH(ARRAY_AGG(employee_id), 1) > 5;
