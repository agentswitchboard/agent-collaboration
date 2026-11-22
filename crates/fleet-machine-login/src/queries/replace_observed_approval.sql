UPDATE login_machine_approvals
SET state=?,end_reason=?,machine_client_id=?,last_grant_at=?,ended_at=?,updated_at=?
WHERE approval_id=?
  AND machine_id=?
  AND state=?
  AND end_reason IS ?
  AND machine_client_id IS ?
  AND unixepoch(last_grant_at) IS ?
  AND ended_at IS NULL
  AND base_generation=?
  AND unixepoch(code_expires_at)=?
  AND request_id=?
  AND label=?
  AND json(allowed_workspaces)=json(?)
  AND approved_by_person_id=?
  AND permission_checked='machine:enroll'
  AND unixepoch(created_at)*1000+CAST(substr(strftime('%f',created_at),4,3) AS INTEGER)=?
