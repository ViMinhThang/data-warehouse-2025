\connect config;

DROP TABLE IF EXISTS log CASCADE;
DROP TABLE IF EXISTS config_load_staging CASCADE;
DROP TABLE IF EXISTS config_extract CASCADE;


CREATE TABLE config_extract (
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

INSERT INTO config_extract (
    tickers, period, interval, output_path, retry_count, is_active, note, created_by, updated_by
) VALUES
    ('AAPL,MSFT,GOOG', '1mo', '1d', './output/yfinance', 3, TRUE, 'Crawl dữ liệu cổ phiếu Mỹ hàng ngày', 'admin', 'admin'),
    ('VNINDEX,VIC,VCB', '1mo', '1d', './output/vn', 3, TRUE, 'Crawl dữ liệu cổ phiếu Việt Nam', 'admin', 'admin'),
    ('BTC-USD,ETH-USD', '1mo', '1h', './output/crypto', 2, TRUE, 'Crawl dữ liệu crypto', 'admin', 'admin'),
    ('META,AMZN,NVDA', '1mo', '1d', './output/tech', 3, FALSE, 'Config bị tạm ngưng', 'admin', 'admin');


CREATE TABLE config_load_staging (
    id SERIAL PRIMARY KEY,
    source_path VARCHAR(255) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    file_type VARCHAR(50) DEFAULT 'csv',
    has_header BOOLEAN DEFAULT TRUE, 
    delimiter VARCHAR(10) DEFAULT ',',
    load_mode VARCHAR(20) DEFAULT 'append',
    retry_count INT DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE, 
    note VARCHAR(255),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

INSERT INTO config_load_staging (
    source_path, target_table, file_type, has_header, delimiter,
    load_mode, retry_count, is_active, note, created_by, updated_by
) VALUES
    ('./output/yfinance', 'stg_market_prices', 'csv', TRUE, ',', 'append', 3, TRUE, 'Load dữ liệu cổ phiếu Mỹ vào staging', 'admin', 'admin'),
    ('./output/vn', 'stg_market_prices', 'csv', TRUE, ',', 'truncate', 3, TRUE, 'Load dữ liệu cổ phiếu Việt Nam vào staging', 'admin', 'admin'),
    ('./output/crypto', 'stg_market_prices', 'csv', TRUE, ',', 'overwrite', 2, TRUE, 'Load dữ liệu crypto vào staging (ghi đè)', 'admin', 'admin'),
    ('./output/tech', 'stg_market_prices', 'csv', TRUE, ',', 'append', 3, FALSE, 'Load tạm ngưng', 'admin', 'admin');


CREATE TABLE log (
    id SERIAL PRIMARY KEY,
    stage VARCHAR(100) CHECK (stage IN ('EXTRACT', 'TRANSFORM', 'LOAD_DW','LOAD_STAGING')) NOT NULL,
    config_id INT,
    ticker VARCHAR(20),
    status VARCHAR(20) CHECK (status IN ('READY','PROCESSING','SUCCESS','FAILURE')),
    log_level VARCHAR(20) CHECK (log_level IN ('INFO','WARNING','ERROR')) DEFAULT 'INFO',
    message VARCHAR(255),
    error_message VARCHAR(255),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (config_id) REFERENCES config_extract(id) ON DELETE CASCADE
);
