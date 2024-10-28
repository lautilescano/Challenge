{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

SELECT 
    id,
    incident_number,
    incident_date,
    alarm_dttm,
    arrival_dttm,
    close_dttm,
    address,
    city,
    zipcode,
    battalion,
    station_area,
    supervisor_district,
    neighborhood_district,
    point,
    data_loaded_at,
    _loaded_at
FROM {{ source('raw', 'fire_incidents') }}

{% if is_incremental() %}
WHERE _loaded_at > (SELECT max(_loaded_at) FROM {{ this }})
{% endif %}
