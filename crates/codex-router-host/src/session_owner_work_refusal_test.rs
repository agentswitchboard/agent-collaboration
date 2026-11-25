use super::*;
use acp_client_runtime::acp_transport::{AcpTransport, TransportRefusal};
use provider_session_machines::{
    session_owner::{AuthorizationTarget, OwnerIncarnation, TakeBackRefusal},
    session_turn_machine::TurnId,
};
use session_event_model::InputId;
use std::num::NonZeroUsize;
use tokio::sync::mpsc::error::TryRecvError;
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
enum RecordedOffer {
    Offered,
    Taken,
    Revoked,
    OtherTarget,
}

#[test]
fn a_repeated_sequence_is_resolved_using_the_exact_authorization() {
    for recorded in [
        RecordedOffer::Offered,
        RecordedOffer::Taken,
        RecordedOffer::Revoked,
        RecordedOffer::OtherTarget,
    ] {
        let (transport, mut writer) = AcpTransport::new(NonZeroUsize::MIN);
        let run = TransportRun {
            incarnation: OwnerIncarnation(Uuid::now_v7()),
            generation: SessionGeneration::from_number(1),
        };
        transport.open_route(run, "fixture-session".into());
        transport.activate_route(run);
        let original = DispatchAuthorization {
            incarnation: run.incarnation,
            run: run.generation,
            input_id: InputId::generate(),
            authorization_seq: 1,
            target: AuthorizationTarget::Session,
        };
        let mut submitted = original.clone();
        transport
            .offer(original.clone(), None, "original")
            .expect("first offer");
        match recorded {
            RecordedOffer::Taken => {
                assert_eq!(
                    transport.take(&original),
                    Ok(DispatchResult::Taken(original.clone()))
                );
            }
            RecordedOffer::Revoked => {
                assert_eq!(
                    transport.revoke(&original),
                    Ok(DispatchResult::Revoked(original.clone()))
                );
            }
            RecordedOffer::OtherTarget => {
                submitted.target = AuthorizationTarget::ExistingTurn {
                    turn: TurnId(Uuid::now_v7()),
                };
            }
            RecordedOffer::Offered => {}
        }

        assert_eq!(
            transport.offer(submitted.clone(), None, "duplicate"),
            Err(TransportRefusal::AuthorizationMismatch),
        );
        let event = settle_refused_offer(
            submitted.clone(),
            WorkOfferRefusal::AuthorizationSequenceOnRecord,
            |authorization| {
                assert_eq!(authorization, &submitted);
                transport
                    .revoke(authorization)
                    .map_err(|_| TakeBackRefusal::NoRecord)
            },
        );

        let reported = match recorded {
            RecordedOffer::Taken => DispatchResult::AlreadyTaken(submitted),
            RecordedOffer::Offered | RecordedOffer::Revoked | RecordedOffer::OtherTarget => {
                DispatchResult::Revoked(submitted)
            }
        };
        assert_eq!(event, OwnerEvent::Dispatch(reported), "{recorded:?}");
        match recorded {
            RecordedOffer::Taken => {
                assert_eq!(
                    writer.try_recv().expect("one original frame").frame,
                    "original"
                );
                assert_eq!(
                    transport.take(&original),
                    Ok(DispatchResult::AlreadyTaken(original))
                );
            }
            RecordedOffer::Offered | RecordedOffer::Revoked => {
                assert_eq!(
                    transport.take(&original),
                    Ok(DispatchResult::DefinitelyUnsent(original))
                );
            }
            RecordedOffer::OtherTarget => {
                assert_eq!(
                    transport.take(&original),
                    Ok(DispatchResult::Taken(original))
                );
                assert_eq!(
                    writer
                        .try_recv()
                        .expect("the other target was untouched")
                        .frame,
                    "original"
                );
            }
        }
        assert!(matches!(writer.try_recv(), Err(TryRecvError::Empty)));
    }
}
