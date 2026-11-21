use super::{NOW_SECONDS, digest, first_statement, renewal, stamp, tamper_signature};
use crate::ConformanceResult;
use serde_json::{Value, json};

/// A small public-wire oracle for a first stamp and two competing successors.
pub struct StampTrace {
    statements: [String; 4],
    retained: Option<usize>,
    audit_count: usize,
}

impl StampTrace {
    pub fn new(organization: &str) -> ConformanceResult<Self> {
        let first = stamp(organization, NOW_SECONDS)?;
        Ok(Self {
            statements: [
                first.clone(),
                renewal(organization, &first, NOW_SECONDS, 1)?,
                renewal(organization, &first, NOW_SECONDS, 2)?,
                tamper_signature(&first_statement(organization, NOW_SECONDS, 3)?)?,
            ],
            retained: None,
            audit_count: 0,
        })
    }

    /// Four offers and one digest read. Each step predicts the expected wire
    /// reply and store from the fixture's fixed predecessor relationships.
    pub fn step(&mut self, operation: u8, organization: &str) -> (Value, u16, Value) {
        if operation == 4 {
            let addressed = json!({"organizationId": organization});
            return match self.retained {
                None => (addressed, 404, Value::Null),
                Some(index) => (
                    addressed,
                    200,
                    json!({
                        "digest": digest(self.statement(index)),
                        "expiresAt": NOW_SECONDS - 60 + super::LIFETIME_SECONDS + i64::try_from(index).unwrap_or(0),
                        "signingKeyId": "stamp-a",
                    }),
                ),
            };
        }
        let offer = usize::from(operation);
        let body =
            json!({"organizationId": organization, "organizationStatement": self.statement(offer)});
        let status = if offer == 3 {
            422
        } else {
            match (self.retained, offer) {
                (None, 0) | (Some(0), 1 | 2) => {
                    self.retained = Some(offer);
                    self.audit_count += 1;
                    200
                }
                (Some(retained), offered) if offered == 0 || offered == retained => 200,
                _ => 409,
            }
        };
        let reply = if status == 200 {
            json!({"result": "provisioned"})
        } else {
            Value::Null
        };
        (body, status, reply)
    }

    pub fn retained_statement(&self) -> Option<&str> {
        self.retained.map(|index| self.statement(index))
    }

    pub fn audit_count(&self) -> usize {
        self.audit_count
    }

    fn statement(&self, index: usize) -> &str {
        self.statements.get(index).map_or("", String::as_str)
    }
}
