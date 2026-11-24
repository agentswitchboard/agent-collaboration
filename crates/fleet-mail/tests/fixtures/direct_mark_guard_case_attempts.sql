INSERT INTO engine_work_attempts(attempt_id,machine_kind,subject_key,state,failure_count,
    next_try_at,last_failure,first_failed_at,last_failed_at,created_at,updated_at)
VALUES
  ('019d0001-0000-7000-8000-000000000005','direct_message','019d0000-0000-7000-8000-000000000005',
   'failing',1,'1970-01-01T00:10:00.000Z','refused',
   '1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z'),
  ('019d0001-0000-7000-8000-000000000006','direct_message','019d0000-0000-7000-8000-000000000006',
   'retired',1,NULL,'refused',
   '1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z'),
  ('019d0001-0000-7000-8000-000000000009','direct_message','019d0000-0000-7000-8000-000000000009',
   'failing',1,'malformed','refused',
   '1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')
