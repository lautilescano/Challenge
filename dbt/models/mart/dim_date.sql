{{
    config(
        materialized='table'
    )
}}

WITH date_spine AS (
    SELECT 
        date_day::DATE as date_id
    FROM generate_series(
        '2000-01-01'::DATE,
        CURRENT_DATE,
        '1 day'::INTERVAL
    ) AS date_day
)

SELECT
    date_id,
    EXTRACT(YEAR FROM date_id) AS year,
    EXTRACT(MONTH FROM date_id) AS month,
    EXTRACT(DAY FROM date_id) AS day,
    EXTRACT(QUARTER FROM date_id) AS quarter,
    EXTRACT(DOW FROM date_id) AS day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM date_id) IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend
FROM date_spine
