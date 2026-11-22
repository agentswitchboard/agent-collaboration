UPDATE login_machines
SET label=?,state=?,enrollment_generation=?,updated_at=?
WHERE machine_id=?
  AND enrollment_generation=?
  AND state=?
  AND label=?
