use std::{
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

use super::{
    approval_store_cases::{first_holder, raise_and_commit, register},
    fixtures::{key, provider, session},
    native_approval_store::NativeApprovalStoreFixture,
};
use approval_request_protocol::RaiseReply;

const PARENT_LOOKUP: &str = include_str!("../../src/queries/read_parent_approval_request.sql");

#[tokio::test]
async fn hidden_and_absent_approval_stop_at_the_scoped_destination_index() {
    let fixture = NativeApprovalStoreFixture::open_fresh().await;
    let holder = first_holder();
    register(&fixture, &holder).await;
    let requester = session(&holder.run.enrollment, 0xb901);
    let parent = session(&holder.run.enrollment, 0xb902);
    let unrelated = session(&holder.run.enrollment, 0xb903);
    for member in [&parent, &unrelated] {
        fixture
            .store_session(&holder.organization_id, member, None, true)
            .await;
    }
    fixture
        .store_session(&holder.organization_id, &requester, Some(&parent), true)
        .await;
    let hidden = key(&holder.run, 0xb904);
    let absent = key(&holder.run, 0xb905);
    let request = provider(&hidden, &requester, "Hidden from a different session");
    assert!(matches!(
        raise_and_commit(&fixture, &holder, &request).await.0,
        RaiseReply::Receipt { .. }
    ));
    fixture
        .alter_fixture("UPDATE approval_requests SET offer_json='malformed'")
        .await;

    let measure = |id: &str, caller: &str| {
        sqlite_lookup(
            &fixture.database_path(),
            [
                holder.organization_id.as_str(),
                id,
                holder.run.enrollment.machine_id.as_str(),
                holder.run.enrollment.enrollment_id.as_str(),
                caller,
            ],
        )
    };
    let hidden = measure(hidden.request_id.as_str(), unrelated.session_id.as_str());
    let absent = measure(absent.request_id.as_str(), unrelated.session_id.as_str());
    assert!(
        hidden
            .plan
            .iter()
            .any(|line| line.contains("USING COVERING INDEX approval_destinations_session_reads"))
    );
    assert_eq!(hidden.plan, absent.plan);
    assert_eq!(hidden.rows, 0);
    assert_eq!(absent.rows, 0);
    assert_eq!(hidden.steps, absent.steps);
    // The request-row seek is present in the prepared statement but skipped on both misses.
    assert_eq!(hidden.request_loops, 0);
    assert_eq!(absent.request_loops, 0);
    let visible = measure(request.request_id().as_str(), parent.session_id.as_str());
    assert_eq!(visible.rows, 1);
    assert_eq!(visible.request_loops, 1);
    assert!(visible.steps > hidden.steps);
    println!(
        "hidden and absent take {} VM steps each, visible takes {}",
        hidden.steps, visible.steps
    );
}

struct LookupWork {
    plan: Vec<String>,
    rows: usize,
    steps: u64,
    request_loops: u64,
}

fn sqlite_lookup(database: &Path, arguments: [&str; 5]) -> LookupWork {
    let shell = std::env::var_os("SESSION_SQLITE_SCAN_CLI").unwrap_or_else(|| "sqlite3".into());
    let mut child = Command::new(shell)
        .arg("-batch")
        .arg("-readonly")
        .arg(database)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    writeln!(input, ".bail on\n.parameter init").unwrap();
    for (index, value) in arguments.into_iter().enumerate() {
        writeln!(input, ".parameter set ?{} '{}'", index + 1, value).unwrap();
    }
    writeln!(input, "EXPLAIN QUERY PLAN {PARENT_LOOKUP};\n.print DATA\n.scanstats on\n.stats vmstep\n{PARENT_LOOKUP};").unwrap();
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        !String::from_utf8_lossy(&output.stderr).contains("not available"),
        "{stdout}"
    );
    let (plan, observed) = stdout.split_once("DATA\n").unwrap();
    assert!(
        !observed.contains("malformed"),
        "admission must only project ownership and timing"
    );
    let steps = observed
        .lines()
        .find_map(|line| {
            line.split_once("VM-steps:")
                .and_then(|(_, steps)| steps.trim().parse().ok())
        })
        .unwrap_or_else(|| panic!("SQLite shell did not report VM steps: {observed}"));
    let request_loops = observed
        .lines()
        .find_map(|line| {
            let (_, visits) = line.split_once("SEARCH requests")?;
            let (_, loops) = visits.split_once("loops=")?;
            loops.split_whitespace().next()?.parse().ok()
        })
        .unwrap_or_else(|| panic!("SQLite shell did not report the request-row loop: {observed}"));
    LookupWork {
        plan: plan.lines().map(str::to_owned).collect(),
        rows: observed
            .lines()
            .filter(|line| {
                line.split('|')
                    .next()
                    .is_some_and(|id| id.parse::<workspace_ids::RequestId>().is_ok())
            })
            .count(),
        steps,
        request_loops,
    }
}
