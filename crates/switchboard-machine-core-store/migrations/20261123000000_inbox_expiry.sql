-- Original local arrival plus Central's remaining delivery window, UTC Unix ms.
-- Central's system notices and outcomes have no delivery expiry.
ALTER TABLE inbox_messages ADD COLUMN expires_at INTEGER;
