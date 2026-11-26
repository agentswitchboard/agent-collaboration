use super::{
    HarnessCallbackIngress, HarnessLifecyclePayloads, HarnessProvider, OwnerHarnessBinding,
};

struct TestPayloads;

impl HarnessLifecyclePayloads for TestPayloads {
    type Event = ();
    type SkillView = ();
    type TurnOutcome = ();
    type Capabilities = ();
    type AuthStatus = ();
    type Settings = ();
    type SettingFailureReason = ();
}

#[test]
fn lifecycle_traits_are_object_safe() {
    let _: Option<&dyn HarnessProvider<TestPayloads>> = None;
    let _: Option<&dyn OwnerHarnessBinding<TestPayloads>> = None;
    let _: Option<&dyn HarnessCallbackIngress<TestPayloads>> = None;
}
