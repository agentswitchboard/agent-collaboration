//! A test consumer of the two ports. The production association registry belongs to custody.

use std::{cell::RefCell, collections::HashMap, num::NonZeroU64};

use approval_request_protocol::{
    OfferView, QuestionField, QuestionFieldId, QuestionFields, QuestionOffer,
};
use switchboard_approval_ports::{
    ApprovalAsk, ApprovalCustodyPort, ApprovalScope, ApprovalTurnId, HarnessApprovalValues,
    LocalApprovalRefusal, LocalSessionBindingId, MacWaitEnded, ReplyHandleUnavailable,
    SessionBindingCustodyPort, approval_value_builders,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManagedSessionAssociation {
    session: u8,
    current_owner: u8,
}

#[derive(Default)]
struct BindingConsumer {
    bindings: RefCell<HashMap<LocalSessionBindingId, ManagedSessionAssociation>>,
    admissions: RefCell<Vec<Result<ManagedSessionAssociation, LocalApprovalRefusal>>>,
}

impl SessionBindingCustodyPort for BindingConsumer {
    type ManagedAssociation = ManagedSessionAssociation;

    fn register_session_binding(
        &self,
        binding: LocalSessionBindingId,
        association: ManagedSessionAssociation,
    ) {
        self.bindings.borrow_mut().insert(binding, association);
    }

    fn retire_session_binding(&self, binding: LocalSessionBindingId) {
        self.bindings.borrow_mut().remove(&binding);
    }
}

impl ApprovalCustodyPort for BindingConsumer {
    fn admit_ask(&self, ask: ApprovalAsk) {
        let association = self
            .bindings
            .borrow()
            .get(ask.scope().session_binding())
            .cloned();
        self.admissions
            .borrow_mut()
            .push(association.ok_or(LocalApprovalRefusal::UnregisteredSessionBinding));
    }

    fn wait_ended(&self, _: MacWaitEnded) {}

    fn reply_handle_unavailable(&self, _: ReplyHandleUnavailable) {}
}

fn submit_ask(
    custody: &impl ApprovalCustodyPort,
    harness: &HarnessApprovalValues,
    binding: LocalSessionBindingId,
    wait_nonce: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let turn = ApprovalTurnId::parse("01890f2e-7b4c-7cc0-98c4-000000000003")?;
    let scope = ApprovalScope::from_admitted_turn(binding, NonZeroU64::MIN, turn);
    let offer = OfferView::ProviderQuestion {
        offer: QuestionOffer {
            prompt: "Confirm the ask".to_owned().try_into()?,
            fields: QuestionFields::new(vec![QuestionField::Boolean {
                field_id: QuestionFieldId::new(1)?,
                label: "Confirm".to_owned().try_into()?,
                description: None,
                required: true,
            }])?,
        },
    };
    custody.admit_ask(harness.admitted_ask(scope, wait_nonce.parse()?, offer));
    Ok(())
}

#[test]
fn registered_binding_resolves_the_exact_association() {
    let (harness, _, owner) = approval_value_builders();
    let custody = BindingConsumer::default();
    let binding =
        owner.allocate_session_binding("01890f2e-7b4c-7cc0-98c4-000000000011".parse().unwrap());
    let association = ManagedSessionAssociation {
        session: 1,
        current_owner: 2,
    };
    custody.register_session_binding(binding.clone(), association.clone());
    submit_ask(
        &custody,
        &harness,
        binding,
        "01890f2e-7b4c-7cc0-98c4-000000000021",
    )
    .unwrap();
    assert_eq!(custody.admissions.into_inner(), [Ok(association)]);
}

#[test]
fn retiring_replaced_owner_leaves_only_the_replacement_binding() {
    let (harness, _, owner) = approval_value_builders();
    let custody = BindingConsumer::default();
    let old =
        owner.allocate_session_binding("01890f2e-7b4c-7cc0-98c4-000000000011".parse().unwrap());
    let new =
        owner.allocate_session_binding("01890f2e-7b4c-7cc0-98c4-000000000012".parse().unwrap());
    let replacement = ManagedSessionAssociation {
        session: 1,
        current_owner: 3,
    };
    assert_ne!(old, new);
    custody.register_session_binding(
        old.clone(),
        ManagedSessionAssociation {
            session: 1,
            current_owner: 2,
        },
    );
    custody.register_session_binding(new.clone(), replacement.clone());
    custody.retire_session_binding(old.clone());

    submit_ask(
        &custody,
        &harness,
        old,
        "01890f2e-7b4c-7cc0-98c4-000000000021",
    )
    .unwrap();
    submit_ask(
        &custody,
        &harness,
        new,
        "01890f2e-7b4c-7cc0-98c4-000000000022",
    )
    .unwrap();
    assert_eq!(
        custody.admissions.into_inner(),
        [
            Err(LocalApprovalRefusal::UnregisteredSessionBinding),
            Ok(replacement),
        ]
    );
}

#[test]
fn an_owner_allocated_handle_without_registration_is_a_composition_fault() {
    let (harness, _, owner) = approval_value_builders();
    let custody = BindingConsumer::default();
    let binding =
        owner.allocate_session_binding("01890f2e-7b4c-7cc0-98c4-000000000013".parse().unwrap());
    submit_ask(
        &custody,
        &harness,
        binding,
        "01890f2e-7b4c-7cc0-98c4-000000000021",
    )
    .unwrap();
    assert_eq!(
        custody.admissions.into_inner(),
        [Err(LocalApprovalRefusal::UnregisteredSessionBinding)]
    );
}
