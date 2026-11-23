SELECT a.approval_id, m.machine_id, a.base_generation, a.state, a.end_reason,
       a.machine_client_id, a.last_grant_at, a.ended_at,
       m.state AS machine_state, m.enrollment_generation
FROM login_machine_approvals a
LEFT JOIN login_machines m ON m.machine_id = a.machine_id
WHERE a.approval_id = ? AND a.machine_id = ?
