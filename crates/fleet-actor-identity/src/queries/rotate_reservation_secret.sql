UPDATE actor_sessions SET create_attempt=?, secret_hash=?, updated_at=?
WHERE actor_id=? AND session_state='reserved' AND create_attempt<?
