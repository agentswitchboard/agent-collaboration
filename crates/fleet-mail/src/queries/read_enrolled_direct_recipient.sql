SELECT machine_id AS "machine_id!: String"
FROM login_machines
WHERE machine_id = ? AND state = 'enrolled'
