CREATE TABLE IF NOT EXISTS inbox_messages (
    consumer_id  VARCHAR(255) NOT NULL,
    message_id   VARCHAR(512) NOT NULL,
    processed_at DATETIME(6)  NOT NULL,
    claim_token  BIGINT UNSIGNED NULL,
    PRIMARY KEY (consumer_id, message_id),
    KEY idx_inbox_processed_at (processed_at)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
