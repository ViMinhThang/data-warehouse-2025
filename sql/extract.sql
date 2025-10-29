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


INSERT INTO config_extract (
    tickers, period, interval, output_path, retry_count, is_active, note, created_by, updated_by
) VALUES
    ('AAPL,MSFT,GOOG', '1mo', '1d', './output/yfinance', 3, TRUE, 'Crawl dữ liệu cổ phiếu Mỹ hàng ngày', 'admin', 'admin'),
    ('VNINDEX,VIC,VCB', '1mo', '1d', './output/vn', 3, TRUE, 'Crawl dữ liệu cổ phiếu Việt Nam', 'admin', 'admin'),
    ('BTC-USD,ETH-USD', '1mo', '1h', './output/crypto', 2, TRUE, 'Crawl dữ liệu crypto', 'admin', 'admin'),
    ('META,AMZN,NVDA', '1mo', '1d', './output/tech', 3, FALSE, 'Config bị tạm ngưng', 'admin', 'admin');
