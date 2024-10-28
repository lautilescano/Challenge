CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- Raw tables for initial data load
CREATE TABLE IF NOT EXISTS raw.fire_incidents (
    incident_number TEXT NOT NULL,
    id TEXT PRIMARY KEY,
    incident_date TIMESTAMP,
    alarm_dttm TIMESTAMP,
    arrival_dttm TIMESTAMP,
    close_dttm TIMESTAMP,
    address TEXT,
    city TEXT,
    zipcode TEXT,
    battalion TEXT,
    station_area TEXT,
    supervisor_district TEXT,
    neighborhood_district TEXT,
    point TEXT,
    data_loaded_at TIMESTAMP NOT NULL,
    _loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
