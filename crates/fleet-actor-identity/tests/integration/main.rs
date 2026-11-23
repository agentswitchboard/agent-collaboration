#![cfg(not(target_arch = "wasm32"))]

#[cfg(test)]
mod enrollment_endings;
#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod lifecycle;
#[cfg(test)]
mod query_budgets;
#[cfg(test)]
mod storage_guards;

#[cfg(test)]
mod statement_observer;
