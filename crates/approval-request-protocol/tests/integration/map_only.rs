//! Replace one object at a time, keeping its surrounding wire specimen valid.
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

#[derive(Debug)]
enum Segment {
    Field(String),
    Index(usize),
}

pub(crate) fn assert_objects_require_named_maps<Wire: DeserializeOwned>(
    original: &Value,
) -> Result<(), String> {
    serde_json::from_str::<Wire>(&original.to_string()).map_err(|error| error.to_string())?;
    visit::<Wire>(original, original, &mut Vec::new())
}

fn visit<Wire: DeserializeOwned>(
    original: &Value,
    node: &Value,
    path: &mut Vec<Segment>,
) -> Result<(), String> {
    match node {
        Value::Object(object) => {
            let mut unknown_field = object.clone();
            unknown_field.insert("unrecognisedNestedField".to_owned(), json!(true));
            assert_rejects::<Wire>(
                original,
                path,
                Value::Object(unknown_field),
                "unknown field",
            )?;
            check_tag::<Wire>(original, object, path)?;
            let mut fields: Vec<_> = object
                .iter()
                .filter(|(name, _)| *name != "kind")
                .map(|(_, value)| value)
                .collect();
            // Try the original struct declaration order even when the JSON
            // map's iteration order is different from the declaration order.
            check_permutations::<Wire>(original, path, object.get("kind"), &mut fields, 0)?;
            for (name, child) in object {
                path.push(Segment::Field(name.clone()));
                visit::<Wire>(original, child, path)?;
                path.pop();
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                path.push(Segment::Index(index));
                visit::<Wire>(original, child, path)?;
                path.pop();
            }
        }
        Value::String(text) => {
            let is_parked =
                |value: &Value| value.get("kind").and_then(Value::as_str) == Some("unprovenCall");
            let parked_shape = is_parked(original) || original.get("ending").is_some_and(is_parked);
            if matches!(path.last(), Some(Segment::Field(name)) if name == "agentProgram"
                || name == "decision" || (parked_shape && name == "cause" && text == "callWithdrawn"))
            {
                assert_rejects::<Wire>(
                    original,
                    path,
                    json!({ (text): null }),
                    "object alias for a scalar",
                )?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn check_tag<Wire: DeserializeOwned>(
    original: &Value,
    object: &Map<String, Value>,
    path: &[Segment],
) -> Result<(), String> {
    if object.contains_key("kind") {
        let mut missing = object.clone();
        missing.remove("kind");
        assert_rejects::<Wire>(original, path, Value::Object(missing), "missing tag")?;

        let mut unknown = object.clone();
        unknown.insert("kind".to_owned(), json!("unknownApprovalVariant"));
        assert_rejects::<Wire>(original, path, Value::Object(unknown), "unknown tag")?;
    }
    Ok(())
}

fn check_permutations<Wire: DeserializeOwned>(
    original: &Value,
    path: &[Segment],
    tag: Option<&Value>,
    fields: &mut [&Value],
    offset: usize,
) -> Result<(), String> {
    if offset == fields.len() {
        let array = tag
            .into_iter()
            .chain(fields.iter().copied())
            .cloned()
            .collect();
        return assert_rejects::<Wire>(original, path, Value::Array(array), "positional object");
    }
    for index in offset..fields.len() {
        fields.swap(offset, index);
        check_permutations::<Wire>(original, path, tag, fields, offset + 1)?;
        fields.swap(offset, index);
    }
    Ok(())
}

fn assert_rejects<Wire: DeserializeOwned>(
    original: &Value,
    path: &[Segment],
    replacement: Value,
    change: &str,
) -> Result<(), String> {
    let mut mutated = original.clone();
    let mut target = &mut mutated;
    for segment in path {
        target = match segment {
            Segment::Field(name) => target.get_mut(name.as_str()),
            Segment::Index(index) => target.get_mut(*index),
        }
        .ok_or_else(|| format!("failed to reach {path:?}"))?;
    }
    *target = replacement;
    if serde_json::from_str::<Wire>(&mutated.to_string()).is_ok() {
        return Err(format!(
            "{} accepted {change} at {path:?}",
            std::any::type_name::<Wire>()
        ));
    }
    Ok(())
}
