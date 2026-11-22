UPDATE login_machines
SET label=?,state=?,allowed_workspaces=?,enrollment_generation=?,updated_at=?
WHERE machine_id=?
  AND enrollment_generation=?
  AND state=?
  AND label=?
  AND json(allowed_workspaces)=json(?)
