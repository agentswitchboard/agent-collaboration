UPDATE login_machines
SET updated_at = updated_at
WHERE machine_id = ?1
  AND (
      SELECT count(*) FROM login_machine_allowances
      WHERE machine_id = ?1 AND state = 'active'
  ) = json_array_length(?2)
  AND NOT EXISTS (
      SELECT 1 FROM json_each(?2) AS observed
      WHERE NOT EXISTS (
          SELECT 1 FROM login_machine_allowances
          WHERE machine_id = ?1 AND workspace_id = observed.value AND state = 'active'
      )
  )
