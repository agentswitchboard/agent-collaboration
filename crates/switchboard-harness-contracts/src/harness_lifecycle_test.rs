use super::{
    ApprovalLeaseBinding, HarnessCallbackIngress, HarnessLifecyclePayloads, HarnessOwnerEvents,
    HarnessProvider, OwnerHarnessBinding,
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
    type ApprovalContext = ();
}

#[test]
fn lifecycle_traits_are_object_safe() {
    let _: Option<&dyn HarnessProvider<TestPayloads>> = None;
    let binding: Option<&dyn OwnerHarnessBinding<TestPayloads>> = None;
    let _: Option<&dyn ApprovalLeaseBinding<()>> =
        binding.map(|binding| -> &dyn ApprovalLeaseBinding<()> { binding });
    let _: Option<&dyn HarnessCallbackIngress<TestPayloads>> = None;
    let _: Option<&dyn HarnessOwnerEvents<()>> = None;
}
