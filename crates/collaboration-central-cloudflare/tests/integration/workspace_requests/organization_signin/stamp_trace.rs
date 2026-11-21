use super::support::{DIGEST, Fixture, ORG, PROVISION};
use central_storage::SqlValue;
use collaboration_central_conformance::signin_fixtures::StampTrace;
use hegel::{TestCase, generators as gs};

#[hegel::test(test_cases = 64)]
fn private_stamp_trace_matches_the_wire_and_persisted_oracle(case: TestCase) {
    let steps = case.draw(gs::integers::<usize>().min_value(1).max_value(32));
    let operations: Vec<u8> = (0..steps)
        .map(|_| case.draw(gs::integers::<u8>().max_value(4)))
        .collect();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let fixture = Fixture::new().await;
        let mut model = StampTrace::new(ORG).unwrap();
        for operation in operations {
            let (body, expected_status, expected_reply) = model.step(operation, ORG);
            let path = if operation == 4 { DIGEST } else { PROVISION };
            assert_eq!(fixture.post(path, &body).await, (expected_status, expected_reply));
            let records = fixture.read("SELECT id FROM signin_change_records").await;
            assert_eq!(records.rows.len(), model.audit_count());
            let identity = fixture.read("SELECT id,organization_statement FROM signin_organization_identity").await;
            match model.retained_statement() {
                None => assert!(identity.rows.is_empty()),
                Some(expected) => {
                    assert!(matches!(identity.rows.as_slice(), [row]
                        if matches!(row.as_slice(), [SqlValue::Text(organization), SqlValue::Text(bytes)]
                            if organization == ORG && bytes == expected)));
                }
            }
        }
        fixture.close().await;
    });
}
