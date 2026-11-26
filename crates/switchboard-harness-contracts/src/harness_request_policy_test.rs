use crate::{
    ApprovalLease, ApprovalLeaseBinding, HarnessOwnerRecovery, RequestCancellationSignal,
    RequestPolicy,
};

#[test]
fn request_and_recovery_ports_can_be_injected_as_trait_objects() {
    type TestPayloads = ((), (), (), (), (), ());
    let _: Option<&dyn RequestPolicy<TestPayloads>> = None;
    let _: Option<&dyn ApprovalLeaseBinding<()>> = None;
    let _: Option<&dyn ApprovalLease> = None;
    let _: Option<&dyn RequestCancellationSignal> = None;
    let _: Option<&dyn HarnessOwnerRecovery> = None;
}
