use super::FreshWakeCommand;
use workspace_ids::SessionRef;
use workspace_model::{
    PostText, Timestamp,
    central_mail::{CentralMailRefusal, TimingRequirement},
};

#[test]
fn fresh_wake_construction_requires_a_due_time_after_admission() {
    let target = SessionRef {
        machine_id: "019f0000-0000-7000-8000-000000000001".parse().unwrap(),
        session_id: "test-session".to_owned().try_into().unwrap(),
    };
    let body = PostText::try_from("wake".to_owned()).unwrap();
    let due_at: Timestamp = "2026-11-23T10:05:00Z".parse().unwrap();

    for admitted_at in [due_at, "2026-11-23T10:05:01Z".parse().unwrap()] {
        assert_eq!(
            FreshWakeCommand::new(&target, due_at, &body, admitted_at).unwrap_err(),
            CentralMailRefusal::TimingViolation {
                field: "dueAt".to_owned(),
                requirement: TimingRequirement::FutureTime,
            }
        );
    }
    let admitted_at = "2026-11-23T10:04:59Z".parse().unwrap();
    assert!(FreshWakeCommand::new(&target, due_at, &body, admitted_at).is_ok());
}
