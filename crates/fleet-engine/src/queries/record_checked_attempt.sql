INSERT INTO engine_work_attempts(
  attempt_id,machine_kind,subject_key,state,failure_count,next_try_at,last_failure,
  last_failure_detail,first_failed_at,last_failed_at,retired_at,created_at,updated_at
)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(machine_kind,subject_key) DO UPDATE SET
  attempt_id=excluded.attempt_id,state=excluded.state,failure_count=excluded.failure_count,
  next_try_at=excluded.next_try_at,last_failure=excluded.last_failure,
  last_failure_detail=excluded.last_failure_detail,first_failed_at=excluded.first_failed_at,
  last_failed_at=excluded.last_failed_at,retired_at=excluded.retired_at,created_at=excluded.created_at,
  updated_at=excluded.updated_at
