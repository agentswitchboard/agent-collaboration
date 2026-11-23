UPDATE actor_sessions SET harness_session_id=?, bound_at=?, updated_at=?, session_state='active'
WHERE actor_id=? AND session_state='reserved'
