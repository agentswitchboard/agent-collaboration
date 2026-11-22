UPDATE login_machine_approvals
SET approval_id=approval_id
WHERE approval_id=?
  AND state='approved'
  AND unixepoch(code_expires_at)*1000<=?
