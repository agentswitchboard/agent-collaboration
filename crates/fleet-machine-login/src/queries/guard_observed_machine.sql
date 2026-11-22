UPDATE login_machines
SET updated_at = updated_at
WHERE machine_id = ?1
  AND enrollment_generation = ?2
  AND state = ?3
  AND label = ?4
  AND (
      (?5 IS NULL AND login_machines.state = 'revoked' AND NOT EXISTS (
          SELECT 1 FROM login_machine_approvals
          WHERE machine_id = ?1 AND state = 'enrolled'
      )) OR (login_machines.state = 'enrolled' AND EXISTS (
          SELECT 1 FROM login_machine_approvals
          WHERE machine_id = ?1 AND approval_id = ?5 AND state = 'enrolled'
            AND base_generation + 1 = ?2
      ))
  )
