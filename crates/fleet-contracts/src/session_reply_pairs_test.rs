use crate::{CreateFleetSessionRpc, EndFleetSessionRpc};
use fleet_identity_protocol::{
    CentralIdEndOutcome, EndSessionOutcome, EndedSessionCause, NewEndSettlement,
    OperationIdEndOutcome, OrganizationId, RouterEndCause, SessionReplyMismatch, SessionSettlement,
};
use fleet_rpc::{
    CallFailure, CorrelatedDispatchError, CorrelatedRpc, Envelope, RpcClient, Transport,
    dispatch_correlated,
};
use serde_json::{Value, json};
use std::{
    pin::pin,
    task::{Context, Poll, Waker},
};

const ORGANIZATION: &str = "019a3dce-2000-7000-8000-000000000010";
const SESSION: &str = "019a3dce-2000-7000-8000-000000000011";
const OPERATION: &str = "019a3dce-2000-7000-8000-000000000012";
const DIGEST: &str = "SZwBM9eK4U9QL0d9-wgv_SVQcRUyZXc3HzhJojc6a7c";
const SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const TIME: &str = "2026-11-22T20:01:02.003Z";

#[test]
fn create_checks_caller_and_kind_before_emitting_or_returning_a_reply() {
    let operator = json!({"kind":"routerOperator"});
    let with_secret = json!({
        "kind":"session", "sessionId":SESSION, "presented":{"kind":"secret","secret":SECRET},
    });
    let without_secret = json!({
        "kind":"session", "sessionId":SESSION, "presented":{"kind":"nothing"},
    });
    let ordinary = json!({"kind":"ordinary"});
    let delegate = json!({"kind":"delegate","parent":SESSION});
    let no_secret = json!({"result":"noSecret"});
    let parent_invalid = json!({"result":"refused","reason":{"kind":"parentNotValid"}});
    let superseded = json!({"result":"superseded"});

    for (creator, kind, reply, expected) in [
        (&operator, &ordinary, &superseded, None),
        (&with_secret, &ordinary, &superseded, None),
        (&without_secret, &ordinary, &no_secret, None),
        (&without_secret, &delegate, &no_secret, None),
        (
            &without_secret,
            &delegate,
            &parent_invalid,
            Some(SessionReplyMismatch::SecretPresentation),
        ),
        (
            &without_secret,
            &ordinary,
            &superseded,
            Some(SessionReplyMismatch::SecretPresentation),
        ),
        (
            &operator,
            &ordinary,
            &no_secret,
            Some(SessionReplyMismatch::SecretPresentation),
        ),
        (
            &with_secret,
            &ordinary,
            &no_secret,
            Some(SessionReplyMismatch::SecretPresentation),
        ),
        (
            &with_secret,
            &ordinary,
            &parent_invalid,
            Some(SessionReplyMismatch::ParentForOrdinarySession),
        ),
        (
            &operator,
            &ordinary,
            &parent_invalid,
            Some(SessionReplyMismatch::ParentForOrdinarySession),
        ),
        (&with_secret, &delegate, &parent_invalid, None),
    ] {
        check_pair::<CreateFleetSessionRpc>(create(creator, kind), reply, expected);
    }

    for refusal in ["noSuchSession", "sessionEnded", "wrongSecret"] {
        let reply = json!({"result":refusal});
        check_pair::<CreateFleetSessionRpc>(
            create(&operator, &ordinary),
            &reply,
            Some(SessionReplyMismatch::RefusalForOperator),
        );
        check_pair::<CreateFleetSessionRpc>(
            create(&without_secret, &ordinary),
            &reply,
            Some(SessionReplyMismatch::SecretPresentation),
        );
        check_pair::<CreateFleetSessionRpc>(create(&with_secret, &ordinary), &reply, None);
    }
}

#[test]
fn end_checks_target_and_cause_before_emitting_or_returning_a_reply() {
    let central_id = json!({"kind":"centralId","sessionId":SESSION});
    let operation_id =
        json!({"kind":"operationId","operationId":OPERATION,"contentsDigest":DIGEST});
    let central_delete = end(&central_id, "endedForGood");
    let central_give_up = end(&central_id, "givenUp");
    let operation_give_up = end(&operation_id, "givenUp");
    let freshly_ended = settlement("settled", "ended", "endedForGood");
    let freshly_given_up = settlement("settled", "ended", "givenUp");
    let freshly_abandoned = settlement("settled", "abandoned", "givenUp");
    let already_ended = settlement("alreadySettled", "ended", "revoked");
    let not_yours = json!({"result":"notYoursToEnd"});
    let absent = json!({"result":"noSuchStart"});

    for (request, reply, expected) in [
        (&central_delete, &freshly_ended, None),
        (&central_give_up, &freshly_given_up, None),
        (
            &central_delete,
            &freshly_given_up,
            Some(SessionReplyMismatch::EndedCause),
        ),
        (
            &central_give_up,
            &freshly_ended,
            Some(SessionReplyMismatch::EndedCause),
        ),
        (&central_delete, &freshly_abandoned, None),
        (&central_delete, &already_ended, None),
        (&operation_give_up, &freshly_abandoned, None),
        (
            &operation_give_up,
            &freshly_ended,
            Some(SessionReplyMismatch::EndedByOperationId),
        ),
        (
            &operation_give_up,
            &freshly_given_up,
            Some(SessionReplyMismatch::EndedByOperationId),
        ),
        (&operation_give_up, &already_ended, None),
        (
            &central_delete,
            &not_yours,
            Some(SessionReplyMismatch::NotYoursForCentralId),
        ),
        (&operation_give_up, &not_yours, None),
        (&central_delete, &absent, None),
        (&operation_give_up, &absent, None),
    ] {
        check_pair::<EndFleetSessionRpc>(request.clone(), reply, expected);
    }
}

#[test]
fn the_checked_end_reply_exposes_the_target_specific_outcome() {
    let central = end(
        &json!({"kind":"centralId","sessionId":SESSION}),
        "endedForGood",
    );
    let operation = end(
        &json!({"kind":"operationId","operationId":OPERATION,"contentsDigest":DIGEST}),
        "givenUp",
    );
    let organization = serde_json::from_value(json!(ORGANIZATION)).unwrap();
    let central_client = RpcClient::<EndFleetSessionRpc, _>::new(
        organization,
        WireAnswer(settlement("settled", "ended", "endedForGood")),
    );
    let Poll::Ready(Ok(EndSessionOutcome::ByCentralId(CentralIdEndOutcome::Settled(
        NewEndSettlement::Ended {
            cause: RouterEndCause::EndedForGood {},
            time,
        },
    )))) = poll_once(central_client.exchange_correlated(serde_json::from_value(central).unwrap()))
    else {
        panic!("expected the central-id settlement and its cause")
    };
    assert_eq!(time.as_str(), TIME);

    let organization = serde_json::from_value(json!(ORGANIZATION)).unwrap();
    let operation_client = RpcClient::<EndFleetSessionRpc, _>::new(
        organization,
        WireAnswer(settlement("alreadySettled", "ended", "revoked")),
    );
    let Poll::Ready(Ok(EndSessionOutcome::ByOperationId(OperationIdEndOutcome::AlreadySettled(
        SessionSettlement::Ended {
            cause: EndedSessionCause::Revoked {},
            time,
        },
    )))) =
        poll_once(operation_client.exchange_correlated(serde_json::from_value(operation).unwrap()))
    else {
        panic!("expected the stored Ended settlement on the operation-id path")
    };
    assert_eq!(time.as_str(), TIME);
}

fn create(creator: &Value, kind: &Value) -> Value {
    json!({
        "operationId":OPERATION, "attempt":1, "creator":creator, "agentProgram":"codex",
        "sessionKind":kind,
    })
}

fn end(target: &Value, cause: &str) -> Value {
    json!({"target":target,"cause":{"kind":cause}})
}

fn settlement(result: &str, state: &str, cause: &str) -> Value {
    json!({"result":result,"settlement":{"state":state,"cause":{"kind":cause},"time":TIME}})
}

fn poll_once<Output>(future: impl Future<Output = Output>) -> Poll<Output> {
    pin!(future).poll(&mut Context::from_waker(Waker::noop()))
}

struct WireAnswer(Value);

impl<R> Transport<R> for WireAnswer
where
    R: CorrelatedRpc<Target = OrganizationId>,
{
    fn exchange(
        &self,
        request: Envelope<OrganizationId, R::Request>,
    ) -> impl Future<Output = Result<Envelope<OrganizationId, R::Reply>, CallFailure>> + Send {
        std::future::ready(Ok(Envelope {
            target: request.target,
            body: serde_json::from_str(&self.0.to_string()).unwrap(),
        }))
    }
}

fn check_pair<R>(request: Value, reply: &Value, mismatch: Option<SessionReplyMismatch>)
where
    R: CorrelatedRpc<Target = OrganizationId, InvalidReply = SessionReplyMismatch>,
{
    let organization: OrganizationId = serde_json::from_value(json!(ORGANIZATION)).unwrap();
    let incoming: Envelope<OrganizationId, R::Request> =
        serde_json::from_str(&json!({"target":ORGANIZATION,"body":request}).to_string()).unwrap();
    let raw_reply: R::Reply = serde_json::from_str(&reply.to_string()).unwrap();

    let server = poll_once(dispatch_correlated::<R, _, _>(
        &organization,
        incoming,
        |_| std::future::ready(raw_reply),
    ));
    let client = RpcClient::<R, _>::new(organization, WireAnswer(reply.clone()));
    let received = poll_once(client.exchange_correlated(serde_json::from_value(request).unwrap()));

    if let Some(expected) = mismatch {
        assert!(matches!(
            server,
            Poll::Ready(Err(CorrelatedDispatchError::InvalidReply(error))) if error == expected
        ));
        assert!(matches!(
            received,
            Poll::Ready(Err(CallFailure::AnswerLost))
        ));
    } else {
        let Poll::Ready(Ok(sent)) = server else {
            panic!("a legal reply must be emitted")
        };
        let Poll::Ready(Ok(checked)) = received else {
            panic!("a legal reply must reach the client")
        };
        assert_eq!(
            serde_json::to_value(sent).unwrap(),
            json!({"target":ORGANIZATION,"body":reply})
        );
        assert_eq!(serde_json::to_value(R::into_wire(checked)).unwrap(), *reply);
    }
}
