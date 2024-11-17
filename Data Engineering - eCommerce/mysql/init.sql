-- Set the global timezone for the MySQL server to UTC
SET GLOBAL time_zone = '+00:00';

CREATE DATABASE IF NOT EXISTS ecommerce;
USE ecommerce;

DROP TABLE IF EXISTS sales_analytics;
CREATE TABLE IF NOT EXISTS sales_analytics (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    category VARCHAR(255),
    total_sales DOUBLE,
    avg_price DOUBLE,
    total_quantity BIGINT,
    num_transactions INT,
    PRIMARY KEY (window_start, window_end, category)
);

-- Grant all privileges on the `ecommerce` database to the Spark user
-- This allows Spark to read from and write to the database during processing
GRANT ALL PRIVILEGES ON ecommerce.* TO 'spark'@'%';

-- Flush the privileges to apply changes immediately
FLUSH PRIVILEGES;