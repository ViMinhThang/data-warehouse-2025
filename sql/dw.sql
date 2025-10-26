-- ===============================================
-- DATA WAREHOUSE SCHEMA FOR STOCK INDICATORS
-- Description: Tạo các bảng dim & fact theo sơ đồ bạn gửi
-- ===============================================

-- Tạo schema DW (nếu chưa có)
CREATE SCHEMA IF NOT EXISTS dw;

-- ===============================================
-- 1️. Dimension Table: dim_stock
-- ===============================================
CREATE TABLE IF NOT EXISTS dw.dim_stock (
    stock_id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL UNIQUE,
    company_name VARCHAR(255)
);

-- ===============================================
-- 2️. Dimension Table: dim_datetime
-- ===============================================
CREATE TABLE IF NOT EXISTS dw.dim_datetime (
    datetime_id SERIAL PRIMARY KEY,
    date TIMESTAMP NOT NULL,
    year INT,
    month INT,
    day INT,
    hour INT,
    weekday VARCHAR(20)
);

-- Index hỗ trợ truy vấn theo ngày nhanh hơn
CREATE INDEX IF NOT EXISTS idx_dim_datetime_date ON dw.dim_datetime (date);

-- ===============================================
-- 3️. Fact Table: fact_stock_indicators
-- ===============================================
CREATE TABLE IF NOT EXISTS dw.fact_stock_indicators (
    record_id SERIAL PRIMARY KEY,
    stock_id INT NOT NULL REFERENCES dw.dim_stock(stock_id) ON DELETE CASCADE,
    datetime_id INT NOT NULL REFERENCES dw.dim_datetime(datetime_id) ON DELETE CASCADE,
    close NUMERIC(12,4),
    volume BIGINT,
    diff NUMERIC(12,4),
    percent_change_close NUMERIC(12,6),
    rsi NUMERIC(8,4),
    roc NUMERIC(8,4),
    bb_upper NUMERIC(12,4),
    bb_lower NUMERIC(12,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index giúp tăng tốc join giữa Fact và Dim
CREATE INDEX IF NOT EXISTS idx_fact_stock_id ON dw.fact_stock_indicators(stock_id);
CREATE INDEX IF NOT EXISTS idx_fact_datetime_id ON dw.fact_stock_indicators(datetime_id);


