//! Original test roots retain their case namespaces.

#[cfg(test)]
#[path = "../support/contract_artifacts.rs"]
mod contract_artifacts;
#[cfg(test)]
#[path = "../support/schema_validation.rs"]
mod schema_validation;

mod activity_contracts;
mod connection_wire_contract;
mod identity_value_contracts;
mod remote_start_contracts;
#[cfg(test)]
mod session_lifecycle_contracts;
#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
mod session_typescript;
mod validated_launch_values;
