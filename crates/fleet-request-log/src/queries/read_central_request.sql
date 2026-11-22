SELECT caller, content_digest, answer
FROM runtime_request_log WHERE request_id=? AND created_at>?
