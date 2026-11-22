UPDATE login_machines
SET machine_id=machine_id
WHERE machine_id=?1
  AND enrollment_generation=?2
  AND state=?3
  AND label=?4
  AND (SELECT count(*) FROM login_machine_allowances WHERE machine_id=?1 AND state='active')=json_array_length(?5)
  AND NOT EXISTS (
      SELECT 1 FROM login_machine_allowances AS allowance
      WHERE allowance.machine_id=?1 AND allowance.state='active'
        AND NOT EXISTS (SELECT 1 FROM json_each(?5) WHERE value=allowance.workspace_id)
  )
