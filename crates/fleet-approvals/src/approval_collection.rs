use std::collections::BTreeSet;

use approval_request_protocol::{
    PassOnReply, Raise, RaiseReply, RegisterRunReply, RunRefusal, StopReply,
};
use central_engine_model::{Context, Machine, TransitionError};
use workspace_ids::RouterRunId;

use crate::approval_lifecycle_admission::{applying_time, stop_binding};
use crate::{
    ActiveApprovalEnrollment, ApprovalActor, ApprovalApply, ApprovalCollection,
    ApprovalCollectionEffect, ApprovalCollectionEvent, ApprovalDecisionReply, ApprovalEffect,
    ApprovalEnrollment, ApprovalEvent, ApprovalLifecycle, ApprovalReceipt, ApprovalRefusal,
    ApprovalRequestKey, ApprovalRouting, ApprovalRun, ApprovalSession, ApprovalState,
    ProviderSessionAvailability, RaiseAdmission,
};

type CollectionChange =
    Result<(ApprovalCollection, Vec<ApprovalCollectionEffect>), TransitionError<ApprovalRefusal>>;

impl ApprovalCollection {
    /// Apply one organization-scoped input. The writer commits the resulting facts and effects together.
    pub fn apply(
        &self,
        event: ApprovalCollectionEvent,
        context: &mut Context<'_>,
    ) -> CollectionChange {
        let floor = applying_time(context.now, self.high_water)?;
        let mut next = self.clone();
        next.high_water = floor;
        next.apply_at_floor(event, context)
    }

    fn apply_at_floor(
        mut self,
        event: ApprovalCollectionEvent,
        context: &mut Context<'_>,
    ) -> CollectionChange {
        let mut effects = Vec::new();
        match event {
            ApprovalCollectionEvent::RegisterRun { run } => {
                let result = self.register(run, context, &mut effects)?;
                effects.push(ApprovalCollectionEffect::Registered(result));
            }
            ApprovalCollectionEvent::ActivateEnrollment { enrollment } => {
                self.activate(enrollment, context, &mut effects)?
            }
            ApprovalCollectionEvent::EndEnrollment { enrollment } => {
                self.end_enrollment(&enrollment, context, &mut effects)?
            }
            ApprovalCollectionEvent::EndSession { session } => {
                self.end_session(&session, context, &mut effects)?
            }
            ApprovalCollectionEvent::Raise {
                run,
                raise,
                routing,
                provider_session,
            } => match self.admit(&run, raise.router_run()) {
                Err(refusal) => push_reply(
                    &mut effects,
                    ApprovalReceipt::Raise(RaiseReply::AuthorityRefused { refusal }),
                ),
                Ok(()) => {
                    let key = ApprovalRequestKey {
                        run,
                        request_id: raise.request_id().clone(),
                    };
                    if self.has_other_owner(&key) {
                        push_reply(
                            &mut effects,
                            ApprovalReceipt::Raise(RaiseReply::ContentsDiffer {}),
                        );
                        return Ok((self, effects));
                    }
                    let provider_session = self.provider_session(&key, &raise, provider_session);
                    if !self.requests.contains_key(&key)
                        && provider_session == ProviderSessionAvailability::Live
                        && let ApprovalRouting::Delegate { admitted_parent } = &routing
                        && !self.active_actor(&ApprovalActor::Session(admitted_parent.clone()))
                    {
                        return Err(TransitionError::Domain(
                            ApprovalRefusal::InvalidInitialRoute,
                        ));
                    }
                    let outstanding = if self.requests.contains_key(&key) {
                        0
                    } else {
                        self.requests
                            .iter()
                            .filter(|(row_key, row)| {
                                row_key.run == key.run && row.occupies_capacity()
                            })
                            .count()
                    };
                    self.apply_request(
                        &key,
                        ApprovalEvent::Raise {
                            key: key.clone(),
                            raise,
                            routing,
                            admission: RaiseAdmission {
                                provider_session,
                                outstanding,
                            },
                        },
                        context,
                        &mut effects,
                    )?;
                }
            },
            ApprovalCollectionEvent::Stop { run, request } => {
                let (request_id, claimed_run, _) = stop_binding(&request);
                match self.admit(&run, claimed_run) {
                    Err(refusal) => push_reply(
                        &mut effects,
                        ApprovalReceipt::Stop(StopReply::AuthorityRefused { refusal }),
                    ),
                    Ok(()) => {
                        let key = ApprovalRequestKey {
                            run,
                            request_id: request_id.clone(),
                        };
                        if self.has_other_owner(&key) {
                            push_reply(
                                &mut effects,
                                ApprovalReceipt::Stop(StopReply::NotVisible {}),
                            );
                            return Ok((self, effects));
                        }
                        self.apply_request(
                            &key,
                            ApprovalEvent::Stop {
                                key: key.clone(),
                                request,
                            },
                            context,
                            &mut effects,
                        )?;
                    }
                }
            }
            ApprovalCollectionEvent::DeliveryReply {
                run,
                request,
                reply,
            } => {
                if self.admit(&run, &run.router_run).is_ok() {
                    let key = ApprovalRequestKey {
                        run,
                        request_id: request.request_id().clone(),
                    };
                    self.apply_if_current(
                        &key,
                        ApprovalEvent::DeliveryReply {
                            key: key.clone(),
                            request,
                            reply,
                        },
                        context,
                        &mut effects,
                    )?;
                }
            }
            ApprovalCollectionEvent::Decide {
                key,
                actor,
                request,
            } => {
                if self.active_actor(&actor.actor()) {
                    let prior = self
                        .decisions
                        .get(&request.operation_id)
                        .cloned()
                        .map(Box::new);
                    self.apply_request(
                        &key,
                        ApprovalEvent::Decide {
                            key: key.clone(),
                            actor,
                            request,
                            prior,
                        },
                        context,
                        &mut effects,
                    )?;
                } else {
                    push_reply(
                        &mut effects,
                        ApprovalReceipt::decide_for(
                            &key,
                            actor.actor().identity(),
                            ApprovalDecisionReply::NotVisible {},
                        ),
                    );
                }
            }
            ApprovalCollectionEvent::Read { key, person_id } => {
                self.apply_request(
                    &key,
                    ApprovalEvent::Read {
                        key: key.clone(),
                        person_id,
                    },
                    context,
                    &mut effects,
                )?;
            }
            ApprovalCollectionEvent::PassOn {
                key,
                parent,
                request,
            } => {
                let actor = ApprovalActor::Session(parent.clone());
                if self.active_actor(&actor) && key.request_id == request.request_id {
                    self.apply_request(
                        &key,
                        ApprovalEvent::PassOn {
                            key: key.clone(),
                            live_parent: parent,
                        },
                        context,
                        &mut effects,
                    )?;
                } else {
                    push_reply(
                        &mut effects,
                        ApprovalReceipt::pass_on_for(
                            &key,
                            actor.identity(),
                            PassOnReply::NotVisible {},
                        ),
                    );
                }
            }
            ApprovalCollectionEvent::Alarm { key, deadline } => {
                self.apply_if_current(
                    &key,
                    ApprovalEvent::Alarm { deadline },
                    context,
                    &mut effects,
                )?;
            }
            ApprovalCollectionEvent::Reoffer { key } => {
                if self.admit(&key.run, &key.run.router_run).is_ok() {
                    self.apply_if_current(&key, ApprovalEvent::Reoffer, context, &mut effects)?;
                }
            }
        }
        Ok((self, effects))
    }

    fn has_other_owner(&self, key: &ApprovalRequestKey) -> bool {
        self.requests
            .keys()
            .any(|incumbent| incumbent != key && incumbent.request_id == key.request_id)
    }

    fn provider_session(
        &self,
        key: &ApprovalRequestKey,
        raise: &Raise,
        admitted: ProviderSessionAvailability,
    ) -> ProviderSessionAvailability {
        let Some(session_id) = raise.requesting_session() else {
            return ProviderSessionAvailability::SessionFree;
        };
        let session = ApprovalSession {
            enrollment: key.run.enrollment.clone(),
            session_id: session_id.clone(),
        };
        if self.ended_sessions.contains(&session) {
            ProviderSessionAvailability::NotAvailable
        } else {
            admitted
        }
    }

    fn apply_request(
        &mut self,
        key: &ApprovalRequestKey,
        event: ApprovalEvent,
        context: &mut Context<'_>,
        collection_effects: &mut Vec<ApprovalCollectionEffect>,
    ) -> Result<(), TransitionError<ApprovalRefusal>> {
        let unraised = ApprovalState::Unraised;
        let prior = self.requests.get(key).unwrap_or(&unraised);
        let (next, effects) = ApprovalLifecycle::transition(
            prior,
            ApprovalApply {
                high_water: self.high_water,
                event,
            },
            context,
        )?;
        if &next != prior {
            self.requests.insert(key.clone(), next);
        }
        for effect in effects {
            if let ApprovalEffect::SaveDecision(saved) = &effect {
                self.decisions
                    .insert(saved.operation_id.clone(), (**saved).clone());
            }
            collection_effects.push(ApprovalCollectionEffect::Request(effect));
        }
        Ok(())
    }

    fn apply_if_current(
        &mut self,
        key: &ApprovalRequestKey,
        event: ApprovalEvent,
        context: &mut Context<'_>,
        effects: &mut Vec<ApprovalCollectionEffect>,
    ) -> Result<(), TransitionError<ApprovalRefusal>> {
        match self.apply_request(key, event, context, effects) {
            Ok(()) | Err(TransitionError::Stale) => Ok(()),
            Err(error @ (TransitionError::Domain(_) | TransitionError::NotAllowed { .. })) => {
                Err(error)
            }
        }
    }

    fn end_session(
        &mut self,
        session: &ApprovalSession,
        context: &mut Context<'_>,
        effects: &mut Vec<ApprovalCollectionEffect>,
    ) -> Result<(), TransitionError<ApprovalRefusal>> {
        if !self.active_actor(&ApprovalActor::Session(session.clone())) {
            return Ok(());
        }
        self.ended_sessions.insert(session.clone());
        let affected: Vec<_> = self
            .requests
            .iter()
            .filter_map(|(key, row)| match row {
                ApprovalState::Waiting { saved }
                    if saved.provider_session().as_ref() == Some(session) =>
                {
                    Some(key.clone())
                }
                ApprovalState::Unraised
                | ApprovalState::StoppedBeforeRaise { .. }
                | ApprovalState::Waiting { .. }
                | ApprovalState::Ended { .. } => None,
            })
            .collect();
        for key in affected {
            self.apply_request(&key, ApprovalEvent::SessionEnded, context, effects)?;
        }
        Ok(())
    }

    fn active_actor(&self, actor: &ApprovalActor) -> bool {
        match actor {
            ApprovalActor::Person(_) => true,
            ApprovalActor::Session(session) => {
                self.enrollment(&session.enrollment).is_ok()
                    && !self.ended_sessions.contains(session)
            }
        }
    }

    fn register(
        &mut self,
        run: ApprovalRun,
        context: &mut Context<'_>,
        effects: &mut Vec<ApprovalCollectionEffect>,
    ) -> Result<RegisterRunReply, TransitionError<ApprovalRefusal>> {
        let active = match self.enrollment(&run.enrollment) {
            Ok(active) => active,
            Err(refusal) => return Ok(register_refusal(refusal)),
        };
        if active.retired_runs.contains(&run.router_run) {
            return Ok(RegisterRunReply::RunRetired {});
        }
        let replaced = active
            .registered
            .clone()
            .filter(|prior| prior != &run.router_run);
        let active = self
            .active_enrollments
            .get_mut(&run.enrollment.machine_id)
            .ok_or(TransitionError::Stale)?;
        active.registered = Some(run.router_run);
        if let Some(prior) = replaced {
            active.retired_runs.insert(prior.clone());
            let affected = self.outstanding_keys(|key| {
                key.run.enrollment == run.enrollment && key.run.router_run == prior
            });
            for key in affected {
                self.apply_request(&key, ApprovalEvent::RunRetired, context, effects)?;
            }
        }
        Ok(RegisterRunReply::Registered {})
    }

    fn activate(
        &mut self,
        enrollment: ApprovalEnrollment,
        context: &mut Context<'_>,
        effects: &mut Vec<ApprovalCollectionEffect>,
    ) -> Result<(), TransitionError<ApprovalRefusal>> {
        if self.ended_enrollments.contains(&enrollment) {
            return Ok(());
        }
        if let Some(active) = self.active_enrollments.get(&enrollment.machine_id) {
            if active.enrollment_id == enrollment.enrollment_id {
                return Ok(());
            }
            let outgoing = ApprovalEnrollment {
                machine_id: enrollment.machine_id.clone(),
                enrollment_id: active.enrollment_id.clone(),
            };
            self.end_enrollment(&outgoing, context, effects)?;
        }
        self.active_enrollments.insert(
            enrollment.machine_id,
            ActiveApprovalEnrollment {
                enrollment_id: enrollment.enrollment_id,
                registered: None,
                retired_runs: BTreeSet::new(),
            },
        );
        Ok(())
    }

    fn end_enrollment(
        &mut self,
        ending: &ApprovalEnrollment,
        context: &mut Context<'_>,
        effects: &mut Vec<ApprovalCollectionEffect>,
    ) -> Result<(), TransitionError<ApprovalRefusal>> {
        if self.enrollment(ending).is_err() {
            return Ok(());
        }
        self.ended_enrollments.insert(ending.clone());
        self.active_enrollments.remove(&ending.machine_id);
        let affected = self.outstanding_keys(|key| &key.run.enrollment == ending);
        for key in affected {
            self.apply_request(&key, ApprovalEvent::EnrollmentEnded, context, effects)?;
        }
        Ok(())
    }

    fn outstanding_keys(
        &self,
        within: impl Fn(&ApprovalRequestKey) -> bool,
    ) -> Vec<ApprovalRequestKey> {
        self.requests
            .iter()
            .filter(|(key, row)| within(key) && row.occupies_capacity())
            .map(|(key, _)| key.clone())
            .collect()
    }

    fn enrollment(
        &self,
        enrollment: &ApprovalEnrollment,
    ) -> Result<&ActiveApprovalEnrollment, RunRefusal> {
        if self.ended_enrollments.contains(enrollment) {
            return Err(RunRefusal::EnrollmentEnded {});
        }
        self.active_enrollments
            .get(&enrollment.machine_id)
            .filter(|active| active.enrollment_id == enrollment.enrollment_id)
            .ok_or(RunRefusal::RunNotRegistered {})
    }

    fn admit(&self, run: &ApprovalRun, claimed_run: &RouterRunId) -> Result<(), RunRefusal> {
        let active = self.enrollment(&run.enrollment)?;
        if active.retired_runs.contains(&run.router_run) || &run.router_run != claimed_run {
            return Err(RunRefusal::RunRetired {});
        }
        if active.registered.as_ref() != Some(&run.router_run) {
            return Err(RunRefusal::RunNotRegistered {});
        }
        Ok(())
    }
}

fn register_refusal(refusal: RunRefusal) -> RegisterRunReply {
    match refusal {
        RunRefusal::RunNotRegistered {} => RegisterRunReply::RunNotRegistered {},
        RunRefusal::RunRetired {} => RegisterRunReply::RunRetired {},
        RunRefusal::EnrollmentEnded {} => RegisterRunReply::EnrollmentEnded {},
    }
}

fn push_reply(effects: &mut Vec<ApprovalCollectionEffect>, receipt: ApprovalReceipt) {
    effects.push(ApprovalCollectionEffect::Request(ApprovalEffect::Reply(
        receipt,
    )));
}
