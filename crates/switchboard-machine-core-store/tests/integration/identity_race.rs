use std::{io, os::unix::fs::PermissionsExt as _, path::Path, process::Stdio};
use switchboard_machine_core_store::{MACHINE_LOCAL_DATABASE, load_or_create_machine_id};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdout, Command},
};
use workspace_ids::MachineId;

const CHILD_ROOT: &str = "MACHINE_CORE_IDENTITY_RACE_ROOT";
const FIRST_START: i64 = 1_795_000_000_123;
const READY: &str = "machine identity caller ready";
const ID_PREFIX: &str = "machine identity stored: ";

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn two_processes_initialize_one_fresh_machine_identity() -> TestResult<()> {
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        println!("{READY}");
        let mut release = String::new();
        io::stdin().read_line(&mut release)?;
        if release != "start\n" {
            return Err("identity caller did not receive its start signal".into());
        }
        let identity = load_or_create_machine_id(Path::new(&root), FIRST_START).await?;
        println!("{ID_PREFIX}{identity}");
        return Ok(());
    }

    let root = tempfile::tempdir()?;
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))?;
    let (mut first, mut first_lines) = start_caller(root.path())?;
    let (mut second, mut second_lines) = start_caller(root.path())?;
    for lines in [&mut first_lines, &mut second_lines] {
        if !read_signal(lines, READY).await?.is_empty() {
            return Err("unexpected identity caller readiness payload".into());
        }
    }
    if root.path().join(MACHINE_LOCAL_DATABASE).exists() {
        return Err("database exists before the fresh-root race starts".into());
    }

    tokio::try_join!(release(&mut first), release(&mut second))?;
    let (first_id, second_id) = tokio::try_join!(
        read_signal(&mut first_lines, ID_PREFIX),
        read_signal(&mut second_lines, ID_PREFIX),
    )?;

    let (first_exit, second_exit) = tokio::try_join!(first.wait(), second.wait())?;
    if !first_exit.success() || !second_exit.success() {
        return Err(format!("identity caller exits were {first_exit} and {second_exit}").into());
    }
    let reopened = load_or_create_machine_id(root.path(), FIRST_START + 1).await?;
    let first_id = first_id.parse::<MachineId>()?;
    let second_id = second_id.parse::<MachineId>()?;
    if first_id != reopened || second_id != reopened {
        return Err(format!(
            "identity callers returned {first_id} and {second_id}, reopened {reopened}"
        )
        .into());
    }
    Ok(())
}

fn start_caller(root: &Path) -> TestResult<(Child, Lines<BufReader<ChildStdout>>)> {
    let (_, module) = module_path!()
        .split_once("::")
        .ok_or("missing identity test module")?;
    let selector = format!("{module}::two_processes_initialize_one_fresh_machine_identity");
    // The parent binary already passed through the configured signing runner.
    let mut child = Command::new(std::env::current_exe()?)
        .args(["--exact", &selector, "--nocapture"])
        .env(CHILD_ROOT, root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let lines = BufReader::new(child.stdout.take().ok_or("missing child output pipe")?).lines();
    Ok((child, lines))
}

async fn release(child: &mut Child) -> TestResult<()> {
    child
        .stdin
        .take()
        .ok_or("missing child release pipe")?
        .write_all(b"start\n")
        .await?;
    Ok(())
}

async fn read_signal(
    lines: &mut Lines<BufReader<ChildStdout>>,
    prefix: &str,
) -> TestResult<String> {
    while let Some(line) = lines.next_line().await? {
        if let Some(payload) = line.strip_prefix(prefix) {
            return Ok(payload.to_owned());
        }
    }
    Err(format!("identity caller exited without its signal {prefix:?}").into())
}
