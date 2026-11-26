//! The controller's work calls for one harness connection.

use std::{future::Future, pin::Pin};

use crate::{
    DispatchAuthorization, DispatchResult, PromptContent, ProviderSettingKind, ReplyAdmission,
    ReservedReply, SettingValueId, TurnId,
};

/// A prompt or steer borrows the validated content for the duration of the offer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessContent<'content> {
    Input(&'content [PromptContent]),
    Compaction(&'content [PromptContent]),
}

impl<'content> HarnessContent<'content> {
    #[must_use]
    pub const fn blocks(self) -> &'content [PromptContent] {
        match self {
            Self::Input(blocks) | Self::Compaction(blocks) => blocks,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SteerIdlePolicy {
    AllowOwnerPrompt,
    Refuse,
}

/// A harness returns one work result through the caller's reserved reply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkReply {
    Dispatch(DispatchResult),
    ConnectionLost,
}

pub type ReservedWorkReply = Box<dyn ReservedReply<WorkReply>>;

/// The controller supplies the retirement signal for the exact run being cancelled.
pub type RunRetirement = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SettingOffer {
    Offered,
    Settled(DispatchResult),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelDrainStart {
    AlreadyDrained,
    Waiting,
    Refused(CancelReservationRefusal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelDrainWait {
    Drained,
    Expired,
}

/// Valid follow-up actions for the one authorization the harness refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefusedWorkMove {
    SettleAsNotSent,
    TakeBack,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkOfferRefusal {
    PreparationFailed,
    SettingCatalogueUnavailable,
    SettingValueNotOffered,
    SettingSetterUnavailable,
    RouteUnavailable,
    RouteClosing,
    WorkDoesNotFitAuthorization,
    OtherAuthorizationOffered,
    AuthorizationSequenceOnRecord,
}

impl WorkOfferRefusal {
    #[must_use]
    pub const fn valid_moves(self) -> &'static [RefusedWorkMove] {
        match self {
            Self::PreparationFailed
            | Self::SettingCatalogueUnavailable
            | Self::SettingValueNotOffered
            | Self::SettingSetterUnavailable
            | Self::RouteUnavailable
            | Self::RouteClosing
            | Self::WorkDoesNotFitAuthorization
            | Self::OtherAuthorizationOffered => &[RefusedWorkMove::SettleAsNotSent],
            Self::AuthorizationSequenceOnRecord => &[RefusedWorkMove::TakeBack],
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandOverRefusal {
    WriterFull,
    WriterStopped,
    NoSuchOffer,
}

impl HandOverRefusal {
    #[must_use]
    pub const fn valid_moves(self) -> &'static [RefusedWorkMove] {
        match self {
            Self::WriterFull | Self::WriterStopped | Self::NoSuchOffer => {
                &[RefusedWorkMove::TakeBack]
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TakeBackRefusal {
    NoRecord,
}

impl TakeBackRefusal {
    #[must_use]
    pub const fn valid_moves(self) -> &'static [RefusedWorkMove] {
        match self {
            Self::NoRecord => &[],
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelReservationRefusal {
    Full,
    Stopped,
}

impl CancelReservationRefusal {
    #[must_use]
    pub const fn valid_moves(self) -> &'static [RefusedWorkMove] {
        match self {
            Self::Full | Self::Stopped => &[],
        }
    }
}

pub trait HarnessBinding: Send + Sync {
    fn offer_prompt(
        &self,
        authorization: DispatchAuthorization,
        turn: TurnId,
        content: HarnessContent<'_>,
        reply: ReservedWorkReply,
    ) -> Result<(), WorkOfferRefusal>;

    fn offer_steer(
        &self,
        authorization: DispatchAuthorization,
        content: HarnessContent<'_>,
        idle: SteerIdlePolicy,
        reply: ReservedWorkReply,
    ) -> Result<(), WorkOfferRefusal>;

    fn offer_setting(
        &self,
        authorization: DispatchAuthorization,
        kind: ProviderSettingKind,
        wanted: SettingValueId,
        reply: ReservedWorkReply,
    ) -> Result<SettingOffer, WorkOfferRefusal>;

    /// Asks the caller's final question at most once under the ordered writer's lock.
    fn hand_over(
        &self,
        authorization: &DispatchAuthorization,
        still_requested: &mut dyn FnMut() -> bool,
    ) -> Result<DispatchResult, HandOverRefusal>;

    fn take_back(
        &mut self,
        authorization: &DispatchAuthorization,
    ) -> Result<DispatchResult, TakeBackRefusal>;

    /// Marks open requests cancelled before asking for the reply reservation.
    fn begin_cancel(
        &mut self,
        authorization: DispatchAuthorization,
        reserve_reply: &mut dyn FnMut() -> ReplyAdmission<WorkReply>,
        retired: RunRetirement,
    ) -> CancelDrainStart;

    fn send_cancel(
        &mut self,
        authorization: DispatchAuthorization,
        wait: CancelDrainWait,
    ) -> DispatchResult;
}

#[cfg(test)]
#[path = "harness_binding_test.rs"]
mod tests;
