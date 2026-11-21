//! Ordered storage fixtures and an independent alarm oracle for both real Central hosts.

pub struct AlarmInputScenario {
    pub name: &'static str,
    pub writes: &'static [(&'static str, u64)],
    pub expected: [Option<i64>; 2],
}

pub const ALARM_INPUT_SCENARIOS: &[AlarmInputScenario] = &[
    AlarmInputScenario {
        name: "empty Central",
        writes: &[],
        expected: [None, None],
    },
    AlarmInputScenario {
        name: "wake with milliseconds",
        writes: &[(
            "INSERT INTO automation_wakes VALUES ('01890f2e-7b4c-7cc0-98c4-000000000001','target','1970-01-01T00:05:00.123Z','wake','active','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')",
            1,
        )],
        expected: [Some(300123), None],
    },
    AlarmInputScenario {
        name: "wake subject retries",
        writes: &[(
            "INSERT INTO engine_work_attempts VALUES ('01890f2e-7b4c-7cc0-98c4-000000000002','wake_timer','01890f2e-7b4c-7cc0-98c4-000000000001','failing',1,'1970-01-01T00:09:00.000Z','statement_rejected',NULL,'1970-01-01T00:05:00.000Z','1970-01-01T00:05:00.000Z',NULL,'1970-01-01T00:05:00.000Z','1970-01-01T00:05:00.000Z')",
            1,
        )],
        expected: [Some(540000), None],
    },
    AlarmInputScenario {
        name: "retired wake and active schedule",
        writes: &[
            (
                "UPDATE engine_work_attempts SET state='retired',next_try_at=NULL,retired_at=created_at",
                1,
            ),
            (
                "INSERT INTO automation_schedules VALUES ('01890f2e-7b4c-7cc0-98c4-000000000003','target','timing','1970-01-01T00:00:00.000Z','1970-01-08T00:00:00.000Z',NULL,'schedule','1970-01-01T00:07:00.000Z','active','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')",
                1,
            ),
        ],
        expected: [Some(420000), None],
    },
    AlarmInputScenario {
        name: "schedule source retry overrides its subject",
        writes: &[(
            "INSERT INTO engine_work_attempts VALUES ('01890f2e-7b4c-7cc0-98c4-000000000004','engine_source','automation_timer','failing',1,'1970-01-01T00:11:00.000Z','statement_rejected',NULL,'1970-01-01T00:07:00.000Z','1970-01-01T00:07:00.000Z',NULL,'1970-01-01T00:07:00.000Z','1970-01-01T00:07:00.000Z')",
            1,
        )],
        expected: [Some(660000), None],
    },
    AlarmInputScenario {
        name: "queued direct mail",
        writes: &[(
            "INSERT INTO mail_messages(message_id,sender,target_machine_id,target_session,message_kind,body,state,created_at,updated_at,cancel_due_at) SELECT '01890f2e-7b4c-7cc0-98c4-000000000005','sender',machine_id,'target','direct','direct','queued','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:04:00.000Z' FROM login_machines LIMIT 1",
            1,
        )],
        expected: [Some(240000), None],
    },
    AlarmInputScenario {
        name: "delivered direct mail is excluded and queued system mail expires",
        writes: &[
            (
                "UPDATE mail_messages SET state='delivered' WHERE message_id='01890f2e-7b4c-7cc0-98c4-000000000005'",
                1,
            ),
            (
                "INSERT INTO mail_messages(message_id,sender,target_machine_id,target_session,message_kind,body,state,created_at,updated_at,delivery_window_ends_at) SELECT '01890f2e-7b4c-7cc0-98c4-000000000006','sender',machine_id,'target','deliveryOutcome','outcome','queued','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:05:00.000Z' FROM login_machines LIMIT 1",
                1,
            ),
        ],
        expected: [Some(300000), None],
    },
    AlarmInputScenario {
        name: "notice window legacy minimum",
        writes: &[(
            "INSERT INTO mail_notification_windows VALUES ('01890f2e-7b4c-7cc0-98c4-000000000007','recipient','1970-01-01T00:00:00.000Z','1970-01-01T00:15:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')",
            1,
        )],
        expected: [Some(300000), Some(900000)],
    },
    AlarmInputScenario {
        name: "pending workspace drain legacy minimum",
        writes: &[(
            "INSERT INTO mail_workspace_drains SELECT workspace_id,0,'1970-01-01T00:10:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z' FROM directory_workspaces LIMIT 1",
            1,
        )],
        expected: [Some(300000), Some(600000)],
    },
    AlarmInputScenario {
        name: "fixture-only alarm table",
        writes: &[(
            "INSERT INTO engine_alarm_only_items VALUES ('01890f2e-7b4c-7cc0-98c4-000000000008','1970-01-01T00:03:00.000Z','waiting','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')",
            1,
        )],
        expected: [Some(180000), Some(600000)],
    },
    AlarmInputScenario {
        name: "fixture probe table",
        writes: &[
            (
                "INSERT INTO engine_probe_groups VALUES ('01890f2e-7b4c-7cc0-98c4-000000000009',0,'1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')",
                1,
            ),
            (
                "INSERT INTO engine_probe_items(item_id,machine_kind,group_id,due_at,claim,state,created_at,updated_at) VALUES ('01890f2e-7b4c-7cc0-98c4-000000000010','engine_probe','01890f2e-7b4c-7cc0-98c4-000000000009','1970-01-01T00:02:00.000Z','probe','waiting','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')",
                1,
            ),
        ],
        expected: [Some(120000), Some(600000)],
    },
    AlarmInputScenario {
        name: "malformed times still return zero",
        writes: &[("UPDATE engine_probe_items SET due_at='damaged'", 1)],
        expected: [Some(0), Some(600000)],
    },
    AlarmInputScenario {
        name: "stamp guard suppresses both minima",
        writes: &[("DELETE FROM signin_organization_identity", 1)],
        expected: [None, None],
    },
];
