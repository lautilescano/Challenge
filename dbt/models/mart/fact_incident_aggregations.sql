{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['incident_date_id']},
            {'columns': ['supervisor_district']},
            {'columns': ['battalion']}
        ]
    )
}}

WITH daily_aggregations AS (
    SELECT 
        DATE_TRUNC('day', incident_date) as incident_date_id,
        supervisor_district,
        battalion,
        COUNT(*) as total_incidents,
        AVG(EXTRACT(EPOCH FROM (arrival_dttm - alarm_dttm))) as avg_response_time_seconds,
        COUNT(CASE WHEN arrival_dttm - alarm_dttm > INTERVAL '5 minutes' THEN 1 END) as delayed_responses
    FROM {{ ref('stg_fire_incidents') }}
    WHERE incident_date IS NOT NULL
      AND supervisor_district IS NOT NULL
      AND battalion IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT 
    incident_date_id,
    supervisor_district,
    battalion,
    total_incidents,
    avg_response_time_seconds,
    delayed_responses,
    ROUND((delayed_responses::FLOAT / NULLIF(total_incidents, 0)) * 100, 2) as delayed_response_percentage
FROM daily_aggregations

