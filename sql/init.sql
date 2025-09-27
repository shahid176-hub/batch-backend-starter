CREATE SCHEMA IF NOT EXISTS features;

CREATE TABLE IF NOT EXISTS features.zone_quarter_agg (
    year INT,
    quarter INT,
    zone VARCHAR(64),
    trips BIGINT,
    revenue NUMERIC(18,2),
    avg_trip_distance NUMERIC(10,3),
    PRIMARY KEY (year, quarter, zone)
);