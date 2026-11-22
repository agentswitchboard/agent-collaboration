INSERT INTO runtime_request_log (request_id,caller,content_digest,answer,created_at,updated_at)
VALUES (?,?,?,?,?,?)
ON CONFLICT(request_id) DO UPDATE SET caller=excluded.caller,content_digest=excluded.content_digest,
  answer=excluded.answer,created_at=excluded.created_at,updated_at=excluded.updated_at
