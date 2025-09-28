{{ config(materialized='table') }}

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY utc_date DESC) AS rn
    FROM matches
)
SELECT *
FROM ranked
WHERE rn = 1;
