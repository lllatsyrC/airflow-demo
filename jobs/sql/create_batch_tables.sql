-- Tables for examples 8 and 9. Sample data only.
-- Run with:  mysql -u <user> -p < create_batch_tables.sql
CREATE DATABASE IF NOT EXISTS airflow_examples;
USE airflow_examples;

-- Example 8 (batch_run_scripts)
CREATE TABLE IF NOT EXISTS inventory_counts (
    batch_id   VARCHAR(64) NOT NULL,
    store      VARCHAR(64) NOT NULL,
    sku        VARCHAR(32) NOT NULL,
    counted_at DATETIME    NOT NULL,
    quantity   INT         NOT NULL,
    PRIMARY KEY (batch_id, store, sku)
);

-- Example 9 (batch_load_trips)
CREATE TABLE IF NOT EXISTS trips (
    batch_id     VARCHAR(64)  NOT NULL,
    trip_id      VARCHAR(32)  NOT NULL,
    start_time   DATETIME     NOT NULL,
    depot        VARCHAR(64)  NOT NULL,
    vehicle_type VARCHAR(32)  NOT NULL,
    heading_deg  DECIMAL(6,2) NOT NULL,
    planned_km   DECIMAL(8,2) NOT NULL,
    PRIMARY KEY (batch_id, trip_id)
);

CREATE TABLE IF NOT EXISTS trip_points (
    batch_id               VARCHAR(64)  NOT NULL,
    point_id               VARCHAR(32)  NOT NULL,
    trip_id                VARCHAR(32)  NOT NULL,
    t_min                  INT          NOT NULL,
    x_km                   DECIMAL(8,2) NOT NULL,
    y_km                   DECIMAL(8,2) NOT NULL,
    elevation_m            DECIMAL(8,2) NOT NULL,
    start_time             DATETIME     NOT NULL,
    vehicle_type           VARCHAR(32)  NOT NULL,
    distance_from_depot_km DECIMAL(8,2) NOT NULL,
    PRIMARY KEY (batch_id, point_id),
    INDEX idx_trip (batch_id, trip_id)
);
