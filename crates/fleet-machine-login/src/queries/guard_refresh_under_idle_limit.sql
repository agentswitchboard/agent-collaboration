UPDATE login_machine_approvals
SET updated_at=updated_at
WHERE approval_id=?
  AND state='enrolled'
  AND ?-unixepoch(last_grant_at)<?
