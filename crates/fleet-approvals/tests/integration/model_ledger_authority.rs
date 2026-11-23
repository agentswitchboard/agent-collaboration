use super::*;

impl Ledger {
    pub(super) fn visible(&self, row: usize, actor: Actor) -> bool {
        if self.rows[row].first_save.is_none() {
            return false;
        }
        if actor.person() {
            return !ROWS[row].delegate || self.rows[row].forwarded;
        }
        let enrollment = RUN_ENROLLMENTS[ROWS[row].run];
        actor == Actor::Parent
            && ROWS[row].delegate
            && self.active_enrollment == Some(enrollment)
            && !self.ended_parents.contains(&enrollment)
    }

    pub(super) fn enrollment_refusal(&self, enrollment: usize) -> Option<RunRefusal> {
        if self.closed_enrollments.contains(&enrollment) {
            Some(RunRefusal::EnrollmentEnded {})
        } else if self.active_enrollment != Some(enrollment) {
            Some(RunRefusal::RunNotRegistered {})
        } else {
            None
        }
    }

    pub(super) fn holder_refusal(&self, run: usize) -> Option<RunRefusal> {
        self.enrollment_refusal(RUN_ENROLLMENTS[run]).or_else(|| {
            if self.retired.contains(&run) {
                Some(RunRefusal::RunRetired {})
            } else if self.registered != Some(run) {
                Some(RunRefusal::RunNotRegistered {})
            } else {
                None
            }
        })
    }

    fn close_deliveries(
        &mut self,
        matches_run: impl Fn(usize) -> bool,
        reason: Ending,
        sampled: i64,
        effects: &mut Vec<OperationEffect>,
    ) {
        for (row, scope) in ROWS.iter().enumerate() {
            if !matches_run(scope.run) {
                continue;
            }
            if self.rows[row].waiting() {
                self.finish(row, reason, Delivery::Closed, sampled, effects);
            } else if let Some((ending, at, Delivery::Unpaid)) = self.rows[row].ending {
                self.rows[row].ending = Some((ending, at, Delivery::Closed));
            }
        }
    }

    fn end_enrollment(
        &mut self,
        enrollment: usize,
        sampled: i64,
        effects: &mut Vec<OperationEffect>,
    ) {
        if self.active_enrollment != Some(enrollment) {
            return;
        }
        self.closed_enrollments.insert(enrollment);
        self.active_enrollment = None;
        self.registered = None;
        self.close_deliveries(
            |run| RUN_ENROLLMENTS[run] == enrollment,
            Ending::EnrollmentEnded,
            sampled,
            effects,
        );
    }

    pub(super) fn apply_authority(
        &mut self,
        step: Step,
        sampled: i64,
        effects: &mut Vec<OperationEffect>,
    ) {
        match step {
            Step::Register(run) => {
                let response = if let Some(refusal) = self.enrollment_refusal(RUN_ENROLLMENTS[run])
                {
                    match refusal {
                        RunRefusal::EnrollmentEnded {} => RegisterRunReply::EnrollmentEnded {},
                        RunRefusal::RunNotRegistered {} => RegisterRunReply::RunNotRegistered {},
                        RunRefusal::RunRetired {} => RegisterRunReply::RunRetired {},
                    }
                } else if self.retired.contains(&run) {
                    RegisterRunReply::RunRetired {}
                } else {
                    if let Some(previous) = self
                        .registered
                        .replace(run)
                        .filter(|previous| *previous != run)
                    {
                        self.retired.insert(previous);
                        self.close_deliveries(
                            |candidate| candidate == previous,
                            Ending::RequesterGone,
                            sampled,
                            effects,
                        );
                    }
                    RegisterRunReply::Registered {}
                };
                effects.push(OperationEffect::Registered(response));
            }
            Step::EndSession { enrollment, parent } => {
                if self.active_enrollment == Some(enrollment) {
                    if parent {
                        self.ended_parents.insert(enrollment);
                    } else if self.ended_requesters.insert(enrollment) {
                        for (row, scope) in ROWS.iter().enumerate() {
                            if !scope.parked
                                && RUN_ENROLLMENTS[scope.run] == enrollment
                                && self.rows[row].waiting()
                            {
                                self.finish(
                                    row,
                                    Ending::SessionEnded,
                                    Delivery::Unpaid,
                                    sampled,
                                    effects,
                                );
                            }
                        }
                    }
                }
            }
            Step::EndEnrollment(enrollment) => self.end_enrollment(enrollment, sampled, effects),
            Step::ReplaceEnrollment => {
                if !self.closed_enrollments.contains(&1) {
                    self.end_enrollment(0, sampled, effects);
                    self.active_enrollment = Some(1);
                }
            }

            Step::Raise { .. }
            | Step::Decide { .. }
            | Step::Stop { .. }
            | Step::Forward { .. }
            | Step::Read { .. }
            | Step::Alarm { .. }
            | Step::Deliver { .. } => {
                unreachable!("the row reducer handles these steps");
            }
        }
    }
}
