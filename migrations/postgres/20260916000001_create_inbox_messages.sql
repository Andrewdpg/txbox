CREATE TABLE IF NOT EXISTS inbox_messages (
    consumer_id  TEXT        NOT NULL,
    message_id   TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_inbox_processed_at
    ON inbox_messages (processed_at);
