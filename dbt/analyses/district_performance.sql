-- District Performance Analysis
WITH district_stats AS (
    SELECT 
        supervisor_district,
        COUNT(DISTINCT DATE_TRUNC('day', incident_date_id)) as days_with_incidents,
        SUM(total_incidents) as total_incidents,
        AVG(avg_response_time_seconds) as avg_response_time,
        SUM(delayed_responses) as total_delayed_responses
    FROM {{ ref('fact_incident_aggregations') }}
    WHERE incident_date_id >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY 1
)

SELECT 
    supervisor_district,
    total_incidents,
    ROUND(total_incidents::float / days_with_incidents, 2) as avg_daily_incidents,
    ROUND(avg_response_time::numeric, 2) as avg_response_time_seconds,
    ROUND((total_delayed_responses::float / total_incidents * 100)::numeric, 2) as delayed_response_percentage
FROM district_stats
WHERE supervisor_district IS NOT NULL
ORDER BY total_incidents DESC

