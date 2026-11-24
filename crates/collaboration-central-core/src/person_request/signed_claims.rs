pub(crate) use fleet_signin::person_proof::{
    PersonClaims, PersonTokenHeader, ProofClaims, ProofHeader,
};

#[cfg(test)]
#[path = "signed_claims_test.rs"]
mod tests;

#[cfg(test)]
pub(crate) use tests::signed_claims_specimens;
