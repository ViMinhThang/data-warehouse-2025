
DROP TABLE IF EXISTS log CASCADE;
DROP TABLE IF EXISTS config_load_staging CASCADE;
DROP TABLE IF EXISTS config_extract CASCADE;
DROP TABLE IF EXISTS config_transform CASCADE;
DROP TABLE IF EXISTS config_load_dw CASCADE;
DROP TABLE IF EXISTS config_transform_staging CASCADE;

-- Etract
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
    ('AAPL,MSFT,GOOG', '1mo', '5m', './output/yfinance', 3, TRUE, 'Crawl dữ liệu cổ phiếu Mỹ hàng ngày', 'admin', 'admin');

CREATE TABLE config_transform (
    id SERIAL PRIMARY KEY,
    rsi_window INT DEFAULT 14,
    roc_window INT DEFAULT 10,
    bb_window INT DEFAULT 20,
    source_table VARCHAR(100) NOT NULL,
    procedure_transform VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    note VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed data
INSERT INTO config_transform (
    rsi_window,
    roc_window,
    bb_window,
    source_table,
    procedure_transform,
    is_active,
    note
) VALUES
(14, 10, 20, 'stg_market_prices', 'transform_market_prices', TRUE, 'Default transform for stock prices');

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
    ('./output/yfinance', 'stg_market_prices', 'csv', TRUE, ',', 'truncate', 3, TRUE, 'Load dữ liệu cổ phiếu Mỹ vào staging', 'admin', 'admin');



CREATE TABLE log (
    id SERIAL PRIMARY KEY,
    stage VARCHAR(100) CHECK (stage IN ('EXTRACT', 'TRANSFORM', 'LOAD_DW','LOAD_STAGING')) NOT NULL,
    config_id INT,
    status VARCHAR(255) CHECK (status IN ('READY','PROCESSING','SUCCESS','FAILURE')),
    log_level VARCHAR(255) CHECK (log_level IN ('INFO','WARNING','ERROR')) DEFAULT 'INFO',
    message VARCHAR(255),
    error_message VARCHAR(255),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
