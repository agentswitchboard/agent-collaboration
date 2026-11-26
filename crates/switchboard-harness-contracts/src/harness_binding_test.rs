use super::*;
use RefusedWorkMove::{SettleAsNotSent, TakeBack};

#[test]
fn offered_work_refusals_name_their_valid_moves() {
    for reason in [
        WorkOfferRefusal::PreparationFailed,
        WorkOfferRefusal::SettingCatalogueUnavailable,
        WorkOfferRefusal::SettingValueNotOffered,
        WorkOfferRefusal::SettingSetterUnavailable,
        WorkOfferRefusal::RouteUnavailable,
        WorkOfferRefusal::RouteClosing,
        WorkOfferRefusal::WorkDoesNotFitAuthorization,
        WorkOfferRefusal::OtherAuthorizationOffered,
    ] {
        assert_eq!(reason.valid_moves(), [SettleAsNotSent], "{reason:?}");
    }
    assert_eq!(
        WorkOfferRefusal::AuthorizationSequenceOnRecord.valid_moves(),
        [TakeBack]
    );
}

#[test]
fn hand_over_refusals_allow_take_back_of_the_exact_authorization() {
    for reason in [
        HandOverRefusal::WriterFull,
        HandOverRefusal::WriterStopped,
        HandOverRefusal::NoSuchOffer,
    ] {
        assert_eq!(reason.valid_moves(), [TakeBack], "{reason:?}");
    }
}

#[test]
fn an_absent_record_or_refused_cancel_reservation_finishes_the_authorization() {
    assert!(TakeBackRefusal::NoRecord.valid_moves().is_empty());
    assert!(CancelReservationRefusal::Full.valid_moves().is_empty());
    assert!(CancelReservationRefusal::Stopped.valid_moves().is_empty());
}
