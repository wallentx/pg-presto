-- Examples of Semantic and Pass-Through Traps

-- 1. EXTRACT(DOW) semantic mismatch
SELECT EXTRACT(DOW FROM current_date) AS day_of_week;

-- 2. GREATEST/LEAST null handling mismatch
SELECT GREATEST(col1, col2, NULL) AS max_val FROM stats;

-- 3. DDL Traps: Unsupported types and constraints
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    user_uuid UUID,
    metadata JSONB
);

-- 4. DML Traps: ON CONFLICT and RETURNING
INSERT INTO logs (message) VALUES ('test') ON CONFLICT (id) DO NOTHING RETURNING id;
