WITH requests AS (
    SELECT
        json_extract_string(properties, '$.RequestPath') AS endpoint,
        level
    FROM events
    WHERE json_type(properties, '$.RequestPath') = 'VARCHAR'
)
SELECT
    endpoint,
    COUNT(*) AS request_count,
    AVG(CASE WHEN level IN ('Error', 'Fatal') THEN 1.0 ELSE 0.0 END) AS error_rate
FROM requests
WHERE regexp_matches(endpoint, '\S')
GROUP BY endpoint
ORDER BY endpoint;
