CREATE DATABASE IF NOT EXISTS stock_dw;
USE stock_dw;

-- Date Dimension
CREATE TABLE dim_date (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    full_date DATE UNIQUE,
    day INT,
    month INT,
    year INT,
    quarter INT,
    weekday VARCHAR(10)
);

-- Stock Dimension
CREATE TABLE dim_stock (
    stock_id INT AUTO_INCREMENT PRIMARY KEY,
    stock_symbol VARCHAR(10) UNIQUE,
    company_name VARCHAR(100)
);

-- Fact Table
CREATE TABLE fact_stock_prices (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    date_id INT,
    stock_id INT,
    open_price FLOAT,
    close_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    volume BIGINT,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (stock_id) REFERENCES dim_stock(stock_id)
);
