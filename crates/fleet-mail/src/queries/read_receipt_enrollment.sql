SELECT m.machine_id AS "machine_id!: String"
FROM login_machines m
JOIN login_machine_approvals a ON m.machine_id=a.machine_id
WHERE m.machine_id=? AND m.state='enrolled' AND a.state='enrolled'
  AND a.approval_id=? AND a.machine_client_id=?
