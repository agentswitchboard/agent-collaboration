use jsonschema::{Keyword, ValidationError, Validator};
use schemars::{JsonSchema, generate::SchemaSettings, transform::ReplaceBoolSchemas};
use serde_json::Value;

struct Utf8ByteLimit(u64);

impl<'instance> Keyword<'instance> for Utf8ByteLimit {
    fn validate(&self, instance: &'instance Value) -> Result<(), ValidationError<'instance>> {
        if self.is_valid(instance) {
            Ok(())
        } else {
            Err(ValidationError::custom("text exceeds its UTF-8 byte limit"))
        }
    }

    fn is_valid(&self, instance: &'instance Value) -> bool {
        instance
            .as_str()
            .is_none_or(|text| u64::try_from(text.len()).is_ok_and(|length| length <= self.0))
    }
}

// Fleet identity registers this same house keyword. Its test support is
// crate-private and a dev dependency on another workspace crate is forbidden.
pub(super) fn contract_validator(schema: &Value) -> Validator {
    jsonschema::options()
        .with_keyword("x-maxUtf8Bytes", |_, value, _| {
            let maximum_bytes = value.as_u64().ok_or_else(|| {
                ValidationError::custom("x-maxUtf8Bytes must be a nonnegative integer")
            })?;
            Ok(Box::new(Utf8ByteLimit(maximum_bytes)))
        })
        .build(schema)
        .unwrap()
}

// The contract covers both canonical writes and optional inputs that normalize
// on read, such as explicit null for an omitted output.
pub(super) fn schema_for<T: JsonSchema>() -> Value {
    // An empty schema object represents unconstrained raw JSON for consumers
    // that translate schema objects into validators.
    let mut object_schemas = ReplaceBoolSchemas::default();
    object_schemas.skip_additional_properties = true;
    serde_json::to_value(
        SchemaSettings::draft2020_12()
            .for_deserialize()
            .with_transform(object_schemas)
            .into_generator()
            .into_root_schema_for::<T>(),
    )
    .unwrap()
}
