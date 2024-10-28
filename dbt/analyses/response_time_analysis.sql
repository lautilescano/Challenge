-- Response Time Analysis by District and Battalion
WITH monthly_stats AS (
    SELECT 
        DATE_TRUNC('day', incident_date_id) as day,
        supervisor_district,
        battalion,
        SUM(total_incidents) as total_incidents,
        AVG(avg_response_time_seconds) as avg_response_time,
        SUM(delayed_responses) as total_delayed_responses
    FROM {{ ref('fact_incident_aggregations') }}
    WHERE incident_date_id >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY 1, 2, 3
)

SELECT 
    day,
    supervisor_district,
    battalion,
    total_incidents,
    ROUND(avg_response_time::numeric, 2) as avg_response_time_seconds,
    ROUND((total_delayed_responses::float / total_incidents * 100)::numeric, 2) as delayed_response_percentage
FROM monthly_stats
ORDER BY 
    day DESC,
    total_incidents DESC
LIMIT 100
