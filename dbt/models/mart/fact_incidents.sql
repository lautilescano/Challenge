{{
    config(
        materialized='incremental',
        unique_key='incident_id'
    )
}}

WITH incidents AS (
    SELECT 
        id as incident_id,
        incident_number,
        incident_date::DATE as incident_date_id,
        battalion,
        supervisor_district,
        neighborhood_district,
        EXTRACT(EPOCH FROM (arrival_dttm - alarm_dttm)) as response_time_seconds,
        EXTRACT(EPOCH FROM (close_dttm - alarm_dttm)) as total_time_seconds
    FROM {{ ref('stg_fire_incidents') }}
)

SELECT 
    i.*,
    d.year,
    d.month,
    d.quarter
FROM incidents i
LEFT JOIN {{ ref('dim_date') }} d
    ON i.incident_date_id = d.date_id

{% if is_incremental() %}
WHERE incident_date_id > (SELECT max(incident_date_id) FROM {{ this }})
{% endif %}
