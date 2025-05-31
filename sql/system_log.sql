CREATE TABLE IF NOT EXISTS system_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    action VARCHAR(255) NOT NULL,
    duration_ms INT NOT NULL CHECK (duration_ms >= 0),

    INDEX idx_system_logs_timestamp (timestamp)
);
