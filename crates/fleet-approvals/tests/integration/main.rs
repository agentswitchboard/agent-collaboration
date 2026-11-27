#[cfg(test)]
mod delivery;
#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod lifecycle;
#[cfg(test)]
mod model;
#[cfg(test)]
mod native_approval_store;
#[cfg(test)]
mod review;
#[cfg(test)]
mod scope;

#[cfg(test)]
mod model_ledger;

#[cfg(test)]
mod approval_atomicity_delivery_recovery;
#[cfg(test)]
mod approval_atomicity_erasure_cleanup;
#[cfg(test)]
mod approval_atomicity_person;
#[cfg(test)]
mod approval_atomicity_raise_stop;
#[cfg(test)]
mod approval_atomicity_session;
#[cfg(test)]
mod approval_decision_log_cases;
#[cfg(test)]
mod approval_retention_boundary;
#[cfg(test)]
mod approval_store_atomicity;
#[cfg(test)]
mod approval_store_cases;
#[cfg(test)]
mod approval_store_guards;
#[cfg(test)]
mod approval_store_review_cases;
#[cfg(test)]
mod approval_store_stop_markers;
#[cfg(test)]
mod native_session_approval_fixture;
#[cfg(test)]
mod unproven_call_lifecycle;

#[cfg(test)]
mod parent_visibility_budget;
