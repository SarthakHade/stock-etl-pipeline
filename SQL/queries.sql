CREATE DATABASE stock_analysis;
USE stock_analysis;

CREATE TABLE daily_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT
);
SELECT * FROM daily_prices LIMIT 5;

# Average Closing Price Per Year
SELECT 
    YEAR(date) AS year,
    ROUND(AVG(close), 2) AS avg_closing_price
FROM daily_prices
GROUP BY YEAR(date)
ORDER BY year;

# Monthly Highest Price
SELECT 
    YEAR(date) AS year,
    MONTH(date) AS month,
    MAX(high) AS highest_price
FROM daily_prices
GROUP BY YEAR(date), MONTH(date)
ORDER BY year, month;

# Days with Highest Trading Volume
SELECT 
    date,
    volume
FROM daily_prices
ORDER BY volume DESC
LIMIT 10;

# Yearly Highest and Lowest Prices
SELECT 
    YEAR(date) AS year,
    MAX(high) AS yearly_high,
    MIN(low) AS yearly_low
FROM daily_prices
GROUP BY YEAR(date)
ORDER BY year;

# Average Daily Volume Per Year
SELECT 
    YEAR(date) AS year,
    ROUND(AVG(volume), 0) AS avg_volume
FROM daily_prices
GROUP BY YEAR(date)
ORDER BY year;

# Days When Closing Price Was Higher Than Opening Price
SELECT 
    COUNT(*) AS positive_days
FROM daily_prices
WHERE close > open;

# Monthly Average Closing Price
SELECT 
    YEAR(date) AS year,
    MONTH(date) AS month,
    ROUND(AVG(close), 2) AS avg_monthly_close
FROM daily_prices
GROUP BY YEAR(date), MONTH(date)
ORDER BY year, month;

# Highest Closing Price Ever
SELECT 
    date,
    close
FROM daily_prices
ORDER BY close DESC
LIMIT 1;

# Trend Comparison: Yearly Opening vs Closing Average
SELECT 
    YEAR(date) AS year,
    ROUND(AVG(open), 2) AS avg_open,
    ROUND(AVG(close), 2) AS avg_close
FROM daily_prices
GROUP BY YEAR(date)
ORDER BY year;

# High-Volatility Days (Large Daily Price Range)
SELECT 
    date,
    (high - low) AS daily_range
FROM daily_prices
ORDER BY daily_range DESC
LIMIT 10;