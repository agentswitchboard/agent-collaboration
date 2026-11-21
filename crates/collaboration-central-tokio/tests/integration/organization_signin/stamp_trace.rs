use super::support::{A, DIGEST, Fixture, PROVISION, signed::StampTrace};
use central_storage::SqlValue;
use hegel::{TestCase, generators as gs};
use serde_json::Value;

#[hegel::test(test_cases = 64)]
fn private_stamp_trace_matches_the_wire_and_persisted_oracle(case: TestCase) {
    let steps = case.draw(gs::integers::<usize>().min_value(1).max_value(32));
    let operations: Vec<u8> = (0..steps)
        .map(|_| case.draw(gs::integers::<u8>().max_value(5)))
        .collect();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut fixture = Fixture::new().await.unwrap();
        let mut model = StampTrace::new(A).unwrap();
        for operation in operations {
            if operation == 5 {
                fixture.reopen().await.unwrap();
            }
            let operation = operation.min(4);
            let (body, expected_status, expected_reply) = model.step(operation, A);
            let path = if operation == 4 { DIGEST } else { PROVISION };
            let response = fixture.post(path, &body).await.unwrap();
            let reply = if response.body.is_empty() { Value::Null } else { response.value().unwrap() };
            assert_eq!((response.status, reply), (expected_status, expected_reply));
            let (identity, records) = fixture.snapshot().await.unwrap();
            assert_eq!(records.len(), model.audit_count());
            match model.retained_statement() {
                None => assert!(identity.is_empty()),
                Some(expected) => {
                    assert!(matches!(identity.as_slice(), [row]
                        if matches!(row.as_slice(), [SqlValue::Text(organization), SqlValue::Text(bytes), ..]
                            if organization == A && bytes == expected)));
                }
            }
        }
        fixture.close().await.unwrap();
    });
}
