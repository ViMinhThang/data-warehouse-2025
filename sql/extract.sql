-- ==========================================================
--  ETL CONFIGURATION TABLES FOR POSTGRESQL
--  Author: ChatGPT (ETL Setup)
--  Description: Tạo bảng config_extract & log cho hệ thống ETL
-- ==========================================================

-- ==========================================================
-- 1. Bảng config_extract
--    Dùng để cấu hình việc trích xuất dữ liệu (Extract)
-- ==========================================================
CREATE TABLE IF NOT EXISTS config_extract (
    id SERIAL PRIMARY KEY,
    tickers TEXT NOT NULL,
    period VARCHAR(50) NOT NULL,
    interval VARCHAR(50) NOT NULL,
    output_path VARCHAR(255) NOT NULL,
    retry_count INT DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    note VARCHAR(255),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- Trigger cập nhật tự động trường update_time khi UPDATE
CREATE OR REPLACE FUNCTION update_timestamp_config_extract()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_time_config_extract
BEFORE UPDATE ON config_extract
FOR EACH ROW
EXECUTE FUNCTION update_timestamp_config_extract();

-- Index hỗ trợ truy vấn nhanh theo trạng thái
CREATE INDEX IF NOT EXISTS idx_config_extract_is_active ON config_extract (is_active);

-- ==========================================================
-- 2. Bảng log
--    Dùng để lưu thông tin tiến trình ETL (Extract / Transform / Load)
-- ==========================================================
CREATE TABLE IF NOT EXISTS log (
    id SERIAL PRIMARY KEY,
    stage VARCHAR(20) CHECK (stage IN ('EXTRACT', 'TRANSFORM', 'LOAD')) NOT NULL,
    config_id INT REFERENCES config_extract(id) ON DELETE CASCADE,
    ticker VARCHAR(20),
    status VARCHAR(20) CHECK (status IN ('READY','PROCESSING','SUCCESS','FAILURE')),
    log_level VARCHAR(20) CHECK (log_level IN ('INFO','WARNING','ERROR')) DEFAULT 'INFO',
    message VARCHAR(255),
    error_message VARCHAR(255),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trigger cập nhật auto update_time
CREATE OR REPLACE FUNCTION update_timestamp_log()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_time_log
BEFORE UPDATE ON log
FOR EACH ROW
EXECUTE FUNCTION update_timestamp_log();

-- Index hỗ trợ tìm log theo giai đoạn & trạng thái
CREATE INDEX IF NOT EXISTS idx_log_stage_status ON log (stage, status);
CREATE INDEX IF NOT EXISTS idx_log_config_id ON log (config_id);
INSERT INTO config_extract (
    tickers, period, interval, output_path, retry_count, is_active, note, created_by, updated_by
) VALUES
    ('AAPL,MSFT,GOOG', '1mo', '1d', './output/yfinance', 3, TRUE, 'Crawl dữ liệu cổ phiếu Mỹ hàng ngày', 'admin', 'admin'),
    ('VNINDEX,VIC,VCB', '1mo', '1d', './output/vn', 3, TRUE, 'Crawl dữ liệu cổ phiếu Việt Nam', 'admin', 'admin'),
    ('BTC-USD,ETH-USD', '1mo', '1h', './output/crypto', 2, TRUE, 'Crawl dữ liệu crypto', 'admin', 'admin'),
    ('META,AMZN,NVDA', '1mo', '1d', './output/tech', 3, FALSE, 'Config bị tạm ngưng', 'admin', 'admin');
