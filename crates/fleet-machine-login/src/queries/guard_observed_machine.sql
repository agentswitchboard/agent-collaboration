UPDATE login_machines
SET machine_id=machine_id
WHERE machine_id=?
  AND enrollment_generation=?
  AND state=?
  AND label=?
  AND json(allowed_workspaces)=json(?)
