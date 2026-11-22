UPDATE login_machine_approvals
SET approval_id=approval_id
WHERE approval_id=?
  AND state='enrolled'
  AND ?-unixepoch(last_grant_at)>=?
