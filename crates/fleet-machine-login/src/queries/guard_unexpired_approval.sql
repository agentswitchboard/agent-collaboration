UPDATE login_machine_approvals
SET updated_at=updated_at
WHERE approval_id=?
  AND state='approved'
  AND unixepoch(code_expires_at)*1000>?
