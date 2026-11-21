const id = (n: string): string => "printf('01890f2e-7b4c-7cc0-98c4-%012x'," + n + ")";
const machine = "'01890f2e-7b4c-7cc0-98c4-000000009002'";
const stamp = "'1970-01-01T00:00:00.000Z'";
const clock =
  "(CASE WHEN i%4 IN (0,1) THEN '1970-01-01T00:05:00.000Z' ELSE '1970-01-01T00:10:00.000Z' END)";
export function seedHistory(n: number, active: number, windows: number): string[] {
  const seq =
    "WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM seq WHERE i<" + n + ") ";
  function attempts(kind: string, base: number, subject: number, filter: string): string {
    const retired = "(i/8)%2=0";
    return (
      seq +
      "INSERT INTO engine_work_attempts SELECT " +
      id(base + "+i") +
      ",'" +
      kind +
      "'," +
      id(subject + "+i") +
      ",CASE WHEN " +
      retired +
      " THEN 'retired' ELSE 'failing' END,1,CASE WHEN NOT(" +
      retired +
      ") THEN '1970-01-01T00:12:00.000Z' END,'statement_rejected',NULL," +
      stamp +
      "," +
      stamp +
      ",CASE WHEN " +
      retired +
      " THEN " +
      stamp +
      " END," +
      stamp +
      "," +
      stamp +
      " FROM seq WHERE i<=" +
      active +
      " AND " +
      filter +
      ";"
    );
  }
  return [
    ...seedJobs(seq, active, windows),
    attempts("wake_timer", 700000, 100000, "i%8=0"),
    attempts("automation_timer", 800000, 200000, "i%8=0"),
    attempts("direct_message", 900000, 300000, "i%8=2"),
    attempts("system_message_expiry", 1000000, 300000, "i%8=1"),
  ];
}

function seedJobs(seq: string, active: number, windows: number): string[] {
  return [
    "INSERT INTO signin_organization_identity VALUES ('01890f2e-7b4c-7cc0-98c4-000000009001','fixture',1,1);",
    "INSERT INTO login_machines(machine_id,label,state,allowed_workspaces,created_at,updated_at) VALUES (" +
      machine +
      ",'fixture','active','[]'," +
      stamp +
      "," +
      stamp +
      ");",
    seq +
      "INSERT INTO automation_wakes SELECT " +
      id("100000+i") +
      ",'target'," +
      clock +
      ",'wake',CASE WHEN i<=" +
      active +
      " THEN 'active' WHEN i%2=0 THEN 'done' ELSE 'disabled' END," +
      stamp +
      "," +
      stamp +
      " FROM seq;",
    seq +
      "INSERT INTO automation_schedules SELECT " +
      id("200000+i") +
      ",'target','timing'," +
      stamp +
      ",'1970-01-08T00:00:00.000Z',NULL,'schedule'," +
      clock +
      ",CASE WHEN i<=" +
      active +
      " THEN 'active' WHEN i%2=0 THEN 'done' ELSE 'disabled' END," +
      stamp +
      "," +
      stamp +
      " FROM seq;",
    seq +
      "INSERT INTO mail_messages(message_id,sender,target_machine_id,target_session,message_kind,body,state,created_at,updated_at,cancel_due_at,delivery_window_ends_at) SELECT " +
      id("300000+i") +
      ",'sender'," +
      machine +
      ",'target',CASE WHEN i%2=0 THEN 'direct' WHEN i%4=1 THEN 'notice' ELSE 'deliveryOutcome' END,'mail',CASE WHEN i<=" +
      active +
      " THEN 'queued' WHEN i%4 IN (0,1) THEN 'delivered' ELSE 'cancelled' END," +
      stamp +
      "," +
      stamp +
      ",CASE WHEN i%2=0 THEN " +
      clock +
      " END,CASE WHEN i%2=1 THEN " +
      clock +
      " END FROM seq;",
    seq +
      "INSERT INTO mail_notification_windows SELECT " +
      id("400000+i") +
      "," +
      id("500000+i") +
      "," +
      stamp +
      "," +
      clock +
      "," +
      stamp +
      "," +
      stamp +
      " FROM seq WHERE i<=" +
      windows +
      ";",
    seq +
      "INSERT INTO directory_workspaces(workspace_id,name,state,created_at,updated_at) SELECT " +
      id("600000+i") +
      ",'fixture '||i,'active'," +
      stamp +
      "," +
      stamp +
      " FROM seq;",
    seq +
      "INSERT INTO mail_workspace_drains SELECT " +
      id("600000+i") +
      ",0,CASE WHEN i<=" +
      active +
      " THEN " +
      clock +
      " END," +
      stamp +
      "," +
      stamp +
      " FROM seq;",
  ];
}
