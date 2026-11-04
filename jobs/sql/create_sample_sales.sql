-- Sample table for Example 6 (mysql_sales_export).
-- Run with:  mysql -u <user> -p < create_sample_sales.sql
-- Uses its own database so it never touches real data or Airflow's own tables.

CREATE DATABASE IF NOT EXISTS airflow_examples;
USE airflow_examples;

DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
    id        INT AUTO_INCREMENT PRIMARY KEY,
    sale_date DATE          NOT NULL,
    region    VARCHAR(20)   NOT NULL,
    product   VARCHAR(50)   NOT NULL,
    amount    DECIMAL(10,2) NOT NULL,
    INDEX idx_date_region (sale_date, region)
);

INSERT INTO sales (sale_date, region, product, amount) VALUES
    ('2026-10-01', 'East',  'Widget', 120.50),
    ('2026-10-01', 'West',  'Gadget',  75.00),
    ('2026-10-02', 'East',  'Gadget', 310.00),
    ('2026-10-02', 'North', 'Widget',  45.25),
    ('2026-10-03', 'South', 'Gizmo',  220.00),
    ('2026-10-03', 'East',  'Gizmo',   95.75),
    ('2026-10-04', 'West',  'Widget', 400.00),
    ('2026-10-04', 'East',  'Widget', 180.00),
    ('2026-10-05', 'North', 'Gadget', 260.40),
    ('2026-10-05', 'South', 'Widget', 130.00);

-- Optional: a read-only account for Airflow to use.
-- CREATE USER 'airflow_reader'@'%' IDENTIFIED BY 'change-me';
-- GRANT SELECT ON airflow_examples.* TO 'airflow_reader'@'%';
