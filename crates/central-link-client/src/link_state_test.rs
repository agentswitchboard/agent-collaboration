use super::*;
use crate::CallFailure;
use central_link_protocol::{FrameDirection, decode_text, encode_text};
use router_clock::ManualClock;
use workspace_model::{WorkspaceOperation, WorkspaceReply, WorkspaceRequest, WorkspaceResult};

#[path = "link_liveness_test.rs"]
mod liveness;

fn fixture() -> (LinkClient, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new("2030-01-01T00:00:00Z".parse().unwrap()));
    (
        LinkClient::new(
            "01890f2e-7b4c-7cc0-98c4-000000000002".parse().unwrap(),
            clock.clone(),
        ),
        clock,
    )
}
fn connect(client: &mut LinkClient) -> ConnectionAttempt {
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("connect action required")
    };
    client.dial_started(&attempt).unwrap();
    let welcome = LinkFrame::Data(DataFrame::Welcome {
        machine_id: client.machine.clone(),
        central_time: client.clock.utc_now(),
        effective_expires_at: workspace_model::Timestamp::from_timestamp(
            client.clock.utc_now().timestamp() + 600,
            0,
        )
        .unwrap(),
    });
    assert_eq!(
        client.receive(&attempt, welcome).unwrap(),
        ClientEvent::Welcomed {
            central_time: client.clock.utc_now(),
            effective_expires_at: workspace_model::Timestamp::from_timestamp(
                client.clock.utc_now().timestamp() + 600,
                0
            )
            .unwrap(),
        }
    );
    client.ready(&attempt).unwrap();
    attempt
}
fn caller() -> Caller {
    serde_json::from_str(r#"{"kind":"session","value":{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000002","sessionId":"s"}}"#).unwrap()
}
fn report_ping(client: &mut LinkClient, attempt: &ConnectionAttempt, clock: &ManualClock) {
    assert_eq!(client.ping_written(attempt, clock.monotonic_now()), Ok(()));
}
fn payload() -> RequestPayload {
    RequestPayload::Workspace(WorkspaceRequest {
        request_id: Some("01890f2e-7b4c-7cc0-98c4-000000000004".parse().unwrap()),
        workspace_id: Some("01890f2e-7b4c-7cc0-98c4-000000000003".parse().unwrap()),
        operation: WorkspaceOperation::CreateArea {
            name: "Work".to_owned().try_into().unwrap(),
            description: "Where the work happens".to_owned().try_into().unwrap(),
        },
    })
}
fn request(client: &mut LinkClient) -> CorrelationId {
    let frame = client.begin_request(caller(), payload()).unwrap();
    let wire = encode_text(&frame, FrameDirection::ToCentral).unwrap();
    let LinkFrame::Data(DataFrame::Request { request }) =
        decode_text(&wire, FrameDirection::ToCentral).unwrap()
    else {
        panic!("request")
    };
    request.correlation_id().clone()
}
fn reply(correlation_id: CorrelationId) -> LinkFrame {
    LinkFrame::Data(DataFrame::Reply {
        correlation_id,
        result: Box::new(ReplyPayload::Workspace(WorkspaceReply {
            result: WorkspaceResult::CentralUnavailable,
            actors: vec![],
        })),
    })
}

#[test]
fn typed_inbound_frames_are_forwarded_only_for_the_current_connected_attempt() {
    use central_link_protocol::{LinkCallRefusal, MacCall, TypedCallResult};

    let (mut client, _) = fixture();
    let attempt = connect(&mut client);
    let correlation_id: CorrelationId = "01890f2e-7b4c-7cc0-98c4-000000000012".parse().unwrap();
    let call = decode_text(
        r#"{"kind":"callMac","correlationId":"01890f2e-7b4c-7cc0-98c4-000000000012","request":{"call":"deliverEnding","envelope":{"target":{"organizationId":"01890f2e-7b4c-7cc0-98c4-000000000001","machineId":"01890f2e-7b4c-7cc0-98c4-000000000002","enrollmentId":"01890f2e-7b4c-7cc0-98c4-000000000003","routerRun":"01890f2e-7b4c-7cc0-98c4-000000000004"},"body":{"requestId":"01890f2e-7b4c-7cc0-98c4-000000000005","contentsDigest":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","ending":{"kind":"providerApproval","ending":{"kind":"timedOut"}}}}}}"#,
        FrameDirection::ToRouter,
    )
    .unwrap();
    let event = client.receive(&attempt, call.clone()).unwrap();
    assert!(
        matches!(event, ClientEvent::CallMac { correlation_id: id, request } if id == correlation_id && matches!(&*request, MacCall::DeliverEnding(envelope) if envelope.target.machine_id == client.machine))
    );

    let reply = LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id: correlation_id.clone(),
        result: Box::new(TypedCallResult::Refused {
            reason: LinkCallRefusal::WrongTarget,
        }),
    });
    assert_eq!(
        client.receive(&attempt, reply),
        Ok(ClientEvent::CentralCallReply {
            correlation_id,
            result: Box::new(TypedCallResult::Refused {
                reason: LinkCallRefusal::WrongTarget,
            }),
        })
    );
    client.closed(&attempt).unwrap();
    assert_eq!(
        client.receive(&attempt, call),
        Err(ClientError::StaleConnection)
    );
}

#[test]
fn down_pre_welcome_and_refused_calls_fail_without_queueing() {
    let (mut client, clock) = fixture();
    assert_eq!(
        client.begin_request(caller(), payload()),
        Err(ClientError::CentralUnavailable)
    );
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("connect")
    };
    assert_eq!(
        client.begin_request(caller(), payload()),
        Err(ClientError::CentralUnavailable)
    );
    assert!(
        client
            .finish_attempt(
                &attempt,
                AttemptOutcome::EnrollmentEnded {
                    cause: EnrollmentEnd::Revoked
                }
            )
            .unwrap()
            .is_empty()
    );
    clock.advance(Duration::from_secs(1000)).unwrap();
    assert_eq!(client.poll_time(), None);
    assert_eq!(client.next_deadline(), None);
    assert_eq!(
        client.begin_request(caller(), payload()),
        Err(ClientError::CentralUnavailable)
    );
    assert!(!client.retry_after_fault());
    let mut client = LinkClient::new(client.machine.clone(), clock);
    connect(&mut client);
    assert!(matches!(client.status(), LinkStatus::Connected { .. }));
}

#[test]
fn concurrent_out_of_order_replies_do_not_conflate_write_and_correlation_ids() {
    let (mut client, _) = fixture();
    let attempt = connect(&mut client);
    let first = request(&mut client);
    let second = request(&mut client);
    assert_ne!(first, second);
    assert!(
        matches!(client.receive(&attempt, reply(second.clone())), Ok(ClientEvent::Reply { correlation_id, .. }) if correlation_id == second)
    );
    assert_eq!(
        client.receive(&attempt, reply(second.clone())),
        Ok(ClientEvent::LateReply {
            correlation_id: second
        })
    );
    assert!(matches!(
        client.receive(&attempt, reply(first)),
        Ok(ClientEvent::Reply { .. })
    ));
    assert!(client.closed(&attempt).unwrap().is_empty());
}

#[test]
fn transport_frontier_preserves_unknown_writes_and_old_events_cannot_complete_new_calls() {
    let (mut client, clock) = fixture();
    let old = connect(&mut client);
    let unsent = request(&mut client);
    let sent = request(&mut client);
    let known_unsent = client
        .send_failed(&old, &unsent, crate::SendFailure::DefinitelyNotSent)
        .unwrap();
    assert_eq!(known_unsent.failure, CallFailure::CentralUnavailable);
    let failed = client.closed(&old).unwrap();
    assert!(!failed.iter().any(|call| call.correlation_id == unsent));
    assert!(failed.iter().any(|call| call.correlation_id == sent && matches!(&call.failure, CallFailure::OutcomeUnknown { request_id } if request_id.as_str() == "01890f2e-7b4c-7cc0-98c4-000000000004")));
    clock.advance(Duration::from_secs(1)).unwrap();
    let current = connect(&mut client);
    let fresh = request(&mut client);
    assert_ne!(sent, fresh);
    assert_eq!(
        client.receive(&old, reply(fresh.clone())),
        Err(ClientError::StaleConnection)
    );
    assert_eq!(client.closed(&old), Err(ClientError::StaleConnection));
    assert_eq!(
        client.receive(&current, reply(sent)),
        Err(ClientError::UnknownCorrelation)
    );
    assert!(matches!(
        client.receive(&current, reply(fresh)),
        Ok(ClientEvent::Reply { .. })
    ));
}

#[test]
fn injected_clock_drives_exact_pings_pong_recovery_and_two_missed_answers() {
    let (mut client, clock) = fixture();
    let attempt = connect(&mut client);
    clock.advance(Duration::from_secs(19)).unwrap();
    assert_eq!(client.poll_time(), None);
    clock.advance(Duration::from_secs(1)).unwrap();
    assert_eq!(client.poll_time(), Some(ClientAction::SendPing));
    report_ping(&mut client, &attempt, &clock);
    assert_eq!(
        client.receive(&attempt, LinkFrame::Pong),
        Ok(ClientEvent::Pong)
    );
    clock.advance(Duration::from_secs(20)).unwrap();
    assert_eq!(client.poll_time(), Some(ClientAction::SendPing));
    report_ping(&mut client, &attempt, &clock);
    clock.advance(Duration::from_secs(20)).unwrap();
    assert_eq!(client.poll_time(), Some(ClientAction::SendPing));
    report_ping(&mut client, &attempt, &clock);
    clock.advance(Duration::from_secs(20)).unwrap();
    assert!(matches!(
        client.poll_time(),
        Some(ClientAction::Reconnect { .. })
    ));
    assert_eq!(client.status(), LinkStatus::Connecting);
}

#[test]
fn reconnect_backoff_caps_at_thirty_and_welcome_resets_it() {
    let (mut client, clock) = fixture();
    for seconds in [1, 2, 4, 8, 16, 30, 30] {
        let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
            panic!("connect")
        };
        assert!(client.closed(&attempt).unwrap().is_empty());
        clock
            .advance(Duration::from_millis(seconds * 1000 - 1))
            .unwrap();
        assert_eq!(client.poll_time(), None);
        clock.advance(Duration::from_millis(1)).unwrap();
    }
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("connect at the cap");
    };
    client.dial_started(&attempt).unwrap();
    let central_time = clock.utc_now();
    client
        .receive(
            &attempt,
            LinkFrame::Data(DataFrame::Welcome {
                machine_id: client.machine.clone(),
                central_time,
                effective_expires_at: workspace_model::Timestamp::from_timestamp(
                    central_time.timestamp() + 600,
                    0,
                )
                .unwrap(),
            }),
        )
        .unwrap();
    client.closed(&attempt).unwrap();
    clock.advance(Duration::from_secs(1)).unwrap();
    assert_eq!(
        client.poll_time(),
        None,
        "Welcome alone cannot reset the grown backoff"
    );
    clock.advance(Duration::from_secs(29)).unwrap();

    let ready_attempt = connect(&mut client);
    client.closed(&ready_attempt).unwrap();
    clock.advance(Duration::from_millis(999)).unwrap();
    assert_eq!(client.poll_time(), None);
    clock.advance(Duration::from_millis(1)).unwrap();
    assert!(matches!(
        client.poll_time(),
        Some(ClientAction::Connect { .. })
    ));
}

#[test]
fn duplicate_wrong_welcome_reply_family_and_direction_never_consume_a_pending_call() {
    let (mut client, _) = fixture();
    let attempt = connect(&mut client);
    let wrong_welcome = LinkFrame::Data(DataFrame::Welcome {
        machine_id: client.machine.clone(),
        central_time: client.clock.utc_now(),
        effective_expires_at: workspace_model::Timestamp::from_timestamp(
            client.clock.utc_now().timestamp() + 600,
            0,
        )
        .unwrap(),
    });
    assert_eq!(
        client.receive(&attempt, wrong_welcome),
        Err(ClientError::InvalidWelcome)
    );
    let id = request(&mut client);
    let wrong = LinkFrame::Data(DataFrame::Reply {
        correlation_id: id.clone(),
        result: Box::new(ReplyPayload::Mail(
            workspace_model::central_mail::CentralMailResult::CentralUnavailable,
        )),
    });
    assert_eq!(
        client.receive(&attempt, wrong),
        Err(ClientError::WrongReplyFamily)
    );
    assert_eq!(
        client.receive(&attempt, LinkFrame::Ping),
        Err(ClientError::UnexpectedFrame)
    );
    assert!(matches!(
        client.receive(&attempt, reply(id)),
        Ok(ClientEvent::Reply { .. })
    ));
}

#[test]
fn wrong_machine_welcome_is_refused_and_read_disconnect_has_no_durable_retry_id() {
    let (mut client, _) = fixture();
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("connect")
    };
    client.dial_started(&attempt).unwrap();
    let wrong = LinkFrame::Data(DataFrame::Welcome {
        machine_id: "01890f2e-7b4c-7cc0-98c4-000000000099".parse().unwrap(),
        central_time: client.clock.utc_now(),
        effective_expires_at: workspace_model::Timestamp::from_timestamp(
            client.clock.utc_now().timestamp() + 600,
            0,
        )
        .unwrap(),
    });
    assert_eq!(
        client.receive(&attempt, wrong),
        Err(ClientError::WelcomeForAnotherMachine {
            expected: client.machine.clone(),
            actual: "01890f2e-7b4c-7cc0-98c4-000000000099".parse().unwrap(),
        })
    );
    assert_eq!(client.status(), LinkStatus::Connecting);
    let welcome = LinkFrame::Data(DataFrame::Welcome {
        machine_id: client.machine.clone(),
        central_time: client.clock.utc_now(),
        effective_expires_at: workspace_model::Timestamp::from_timestamp(
            client.clock.utc_now().timestamp() + 600,
            0,
        )
        .unwrap(),
    });
    client.receive(&attempt, welcome).unwrap();
    client.ready(&attempt).unwrap();
    let payload = RequestPayload::Workspace(WorkspaceRequest {
        request_id: None,
        workspace_id: Some("01890f2e-7b4c-7cc0-98c4-000000000003".parse().unwrap()),
        operation: WorkspaceOperation::List {
            query: workspace_model::ListQuery::Areas {
                include_archived: false,
            },
            cursor: workspace_model::ListCursor::start(workspace_model::ListOrder::OldestFirst),
            limit: 10_u16.try_into().unwrap(),
        },
    });
    let LinkFrame::Data(DataFrame::Request { request }) =
        client.begin_request(caller(), payload).unwrap()
    else {
        panic!("request")
    };
    assert!(request.payload().write_request_id().is_none());
    let failed = client.closed(&attempt).unwrap();
    assert!(matches!(
        failed.as_slice(),
        [crate::FailedCall {
            failure: CallFailure::CentralUnavailable,
            ..
        }]
    ));
}

#[test]
fn late_clock_poll_counts_actual_unanswered_probes_and_ids_follow_the_same_clock() {
    let (mut client, clock) = fixture();
    let attempt = connect(&mut client);
    clock.advance(Duration::from_secs(100)).unwrap();
    assert_eq!(client.poll_time(), Some(ClientAction::SendPing));
    report_ping(&mut client, &attempt, &clock);
    assert_eq!(
        client.receive(&attempt, LinkFrame::Pong),
        Ok(ClientEvent::Pong)
    );
    let correlation = request(&mut client);
    client
        .receive(&attempt, reply(correlation.clone()))
        .unwrap();
    let id: uuid::Uuid = String::from(correlation).parse().unwrap();
    let (seconds, _) = id.get_timestamp().unwrap().to_unix();
    assert_eq!(seconds, 1_893_456_100);
    clock.advance(Duration::from_secs(20)).unwrap();
    assert_eq!(client.poll_time(), Some(ClientAction::SendPing));
    report_ping(&mut client, &attempt, &clock);
    clock.advance(Duration::from_secs(40)).unwrap();
    assert_eq!(client.poll_time(), Some(ClientAction::SendPing));
    report_ping(&mut client, &attempt, &clock);
    clock.advance(Duration::from_secs(20)).unwrap();
    assert!(matches!(
        client.poll_time(),
        Some(ClientAction::Reconnect { .. })
    ));
}

#[test]
fn transport_failure_receipts_distinguish_unsent_from_ambiguous_write() {
    let (mut client, _) = fixture();
    let attempt = connect(&mut client);
    let known = request(&mut client);
    let ambiguous = request(&mut client);
    let known = client
        .send_failed(&attempt, &known, crate::SendFailure::DefinitelyNotSent)
        .unwrap();
    let ambiguous = client
        .send_failed(&attempt, &ambiguous, crate::SendFailure::MayHaveBeenSent)
        .unwrap();
    assert_eq!(known.failure, CallFailure::CentralUnavailable);
    assert!(matches!(
        ambiguous.failure,
        CallFailure::OutcomeUnknown { .. }
    ));
    assert!(client.closed(&attempt).unwrap().is_empty());
}

#[test]
fn a_reconnect_whose_welcome_names_another_installation_is_invalid() {
    let (mut client, clock) = fixture();
    let attempt = connect(&mut client);
    client.closed(&attempt).unwrap();
    clock.advance(Duration::from_secs(600)).unwrap();
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("reconnect action required")
    };
    client.dial_started(&attempt).unwrap();
    let other = LinkFrame::Data(DataFrame::Welcome {
        machine_id: "01890f2e-7b4c-7cc0-98c4-0000000000bb".parse().unwrap(),
        central_time: clock.utc_now(),
        effective_expires_at: workspace_model::Timestamp::from_timestamp(
            clock.utc_now().timestamp() + 600,
            0,
        )
        .unwrap(),
    });

    let refused = client.receive(&attempt, other);

    assert_eq!(
        refused,
        Err(ClientError::WelcomeForAnotherMachine {
            expected: client.machine.clone(),
            actual: "01890f2e-7b4c-7cc0-98c4-0000000000bb".parse().unwrap(),
        })
    );
    assert_eq!(client.status(), LinkStatus::Connecting);
}

#[test]
fn a_late_welcome_retries_for_this_machine_but_is_final_for_another() {
    let (mut client, clock) = fixture();
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("connect action required")
    };
    client.dial_started(&attempt).unwrap();
    clock.advance(Duration::from_secs(10)).unwrap();
    let welcome_naming = |machine_id: MachineId| {
        LinkFrame::Data(DataFrame::Welcome {
            machine_id,
            central_time: clock.utc_now(),
            effective_expires_at: workspace_model::Timestamp::from_timestamp(
                clock.utc_now().timestamp() + 600,
                0,
            )
            .unwrap(),
        })
    };

    let other_machine: MachineId = "01890f2e-7b4c-7cc0-98c4-0000000000bb".parse().unwrap();

    let other = client.receive(&attempt, welcome_naming(other_machine.clone()));
    let own = client.receive(&attempt, welcome_naming(client.machine.clone()));

    assert_eq!(
        other,
        Err(ClientError::WelcomeForAnotherMachine {
            expected: client.machine.clone(),
            actual: other_machine,
        })
    );
    assert_eq!(own, Err(ClientError::InvalidWelcome));
    client.closed(&attempt).unwrap();
    assert_eq!(client.status(), LinkStatus::Connecting);
    assert_eq!(
        client.next_deadline(),
        Some(clock.monotonic_now().after(Duration::from_secs(1)))
    );
}

#[test]
fn only_the_current_connections_exact_receipt_answer_retires_its_positions() {
    use central_link_protocol::{Receipt, ReceiptBatch, ReceiptKind, ReceiptResult};
    let (mut client, clock) = fixture();
    let first = connect(&mut client);
    let batch = ReceiptBatch::new(vec![Receipt {
        message_id: "01890f2e-7b4c-7cc0-98c4-000000000009".parse().unwrap(),
        kind: ReceiptKind::Seen {
            completed_at: client.clock.utc_now(),
        },
    }])
    .unwrap();
    let frame = client.begin_receipts(batch.clone()).unwrap();
    let LinkFrame::Data(DataFrame::Receipts { correlation_id, .. }) = frame else {
        panic!("receipt frame");
    };
    let results = |correlation_id, results| {
        LinkFrame::Data(DataFrame::ReceiptResults {
            correlation_id,
            results,
        })
    };
    assert!(matches!(
        client.receive(&first, results(correlation_id.clone(), vec![])),
        Err(ClientError::InvalidReceiptResults)
    ));
    assert!(matches!(
        client.begin_receipts(batch.clone()),
        Err(ClientError::ReceiptFramePending)
    ));
    clock.advance(Duration::from_secs(30)).unwrap();
    assert!(matches!(
        client.poll_time(),
        Some(ClientAction::Reconnect { .. })
    ));
    clock.advance(Duration::from_secs(1)).unwrap();
    let second = connect(&mut client);
    let frame = client.begin_receipts(batch).unwrap();
    let LinkFrame::Data(DataFrame::Receipts {
        correlation_id: current,
        ..
    }) = frame
    else {
        panic!("retry receipt frame");
    };
    assert!(
        client
            .receive(
                &first,
                results(correlation_id.clone(), vec![ReceiptResult::Recorded])
            )
            .is_err()
    );
    assert!(matches!(
        client.receive(
            &second,
            results(correlation_id, vec![ReceiptResult::Recorded])
        ),
        Err(ClientError::InvalidReceiptResults)
    ));
    assert_eq!(
        client
            .receive(
                &second,
                results(current.clone(), vec![ReceiptResult::Recorded])
            )
            .unwrap(),
        ClientEvent::ReceiptResults {
            correlation_id: current,
            results: vec![ReceiptResult::Recorded]
        },
    );
}

#[test]
fn a_full_request_table_refuses_before_producing_a_frame_and_reuses_a_freed_slot() {
    let (mut client, _) = fixture();
    let attempt = connect(&mut client);
    let pending = (0..64).map(|_| request(&mut client)).collect::<Vec<_>>();

    assert_eq!(
        client.begin_request(caller(), payload()),
        Err(ClientError::NeverReached)
    );
    assert!(matches!(
        client.receive(&attempt, reply(pending[0].clone())),
        Ok(ClientEvent::Reply { .. })
    ));
    let replacement = request(&mut client);
    assert!(!pending.contains(&replacement));
    assert_eq!(client.closed(&attempt).unwrap().len(), 64);
}

#[test]
fn an_open_connection_keeps_only_its_recent_legacy_replies() {
    let (mut client, _) = fixture();
    let attempt = connect(&mut client);
    let completed = (0..129)
        .map(|_| {
            let correlation = request(&mut client);
            assert!(matches!(
                client.receive(&attempt, reply(correlation.clone())),
                Ok(ClientEvent::Reply { .. })
            ));
            correlation
        })
        .collect::<Vec<_>>();

    assert_eq!(
        client.receive(&attempt, reply(completed[128].clone())),
        Ok(ClientEvent::LateReply {
            correlation_id: completed[128].clone(),
        })
    );
    assert_eq!(
        client.receive(&attempt, reply(completed[0].clone())),
        Err(ClientError::UnknownCorrelation)
    );
}

#[test]
fn welcomed_link_serves_transport_frames_and_holds_ordinary_requests_until_ready() {
    let (mut client, clock) = fixture();
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("connect action required");
    };
    let central_time = clock.utc_now();
    let effective_expires_at =
        workspace_model::Timestamp::from_timestamp(central_time.timestamp() + 300, 0).unwrap();
    let welcome = LinkFrame::Data(DataFrame::Welcome {
        machine_id: client.machine.clone(),
        central_time,
        effective_expires_at,
    });
    client.dial_started(&attempt).unwrap();
    assert_eq!(
        client.receive(&attempt, welcome),
        Ok(ClientEvent::Welcomed {
            central_time,
            effective_expires_at
        }),
    );
    assert_eq!(
        client.begin_request(caller(), payload()),
        Err(ClientError::CentralUnavailable)
    );
    assert!(matches!(client.status(), LinkStatus::Welcomed { .. }));
    let delivery = decode_text(
        r#"{"kind":"deliver","timeLeft":1,"sender":{"kind":"central"},"message":{"messageId":"01890f2e-7b4c-7cc0-98c4-000000000009","sender":{"kind":"central"},"target":{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000002","sessionId":"s"},"body":{"kind":"direct","body":"hello","delivery":"auto","replyTo":null},"state":{"kind":"queued"},"source":null}}"#,
        FrameDirection::ToRouter,
    ).unwrap();
    assert!(matches!(
        client.receive(&attempt, delivery),
        Ok(ClientEvent::Deliver { .. })
    ));
    let correlation_id: CorrelationId = "01890f2e-7b4c-7cc0-98c4-000000000010".parse().unwrap();
    let result = central_link_protocol::TokenRenewalResult::Renewed {
        central_time,
        effective_expires_at,
    };
    assert_eq!(
        client.receive(
            &attempt,
            LinkFrame::Data(DataFrame::MachineTokenRenewal {
                correlation_id: correlation_id.clone(),
                result: result.clone()
            })
        ),
        Ok(ClientEvent::TokenRenewal {
            correlation_id,
            result
        }),
    );
    clock.advance(Duration::from_secs(20)).unwrap();
    assert_eq!(client.poll_time(), Some(ClientAction::SendPing));
    report_ping(&mut client, &attempt, &clock);
    assert_eq!(
        client.receive(&attempt, LinkFrame::Pong),
        Ok(ClientEvent::Pong)
    );
    client.ready(&attempt).unwrap();
    assert!(client.begin_request(caller(), payload()).is_ok());
}

#[test]
fn typed_close_before_welcome_distinguishes_final_enrollment_loss_from_expiry() {
    for (code, expected) in [
        (
            MachineLinkCloseCode::Revoked,
            LinkStatus::EnrollmentEnded {
                cause: EnrollmentEnd::Revoked,
            },
        ),
        (
            MachineLinkCloseCode::Inactive,
            LinkStatus::EnrollmentEnded {
                cause: EnrollmentEnd::Inactive,
            },
        ),
        (MachineLinkCloseCode::Expired, LinkStatus::Connecting),
    ] {
        let (mut client, clock) = fixture();
        let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
            panic!("connect action required");
        };
        client.dial_started(&attempt).unwrap();
        assert!(
            client
                .close_received(&attempt, Some(code))
                .unwrap()
                .is_empty()
        );
        assert_eq!(client.status(), expected);
        clock.advance(Duration::from_secs(60)).unwrap();
        assert_eq!(
            matches!(client.poll_time(), Some(ClientAction::Connect { .. })),
            code == MachineLinkCloseCode::Expired,
        );
    }
}

#[test]
fn an_explicit_fault_parks_the_attempt_until_the_owner_retries() {
    let (mut client, clock) = fixture();
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("connect action required");
    };
    let kind = LinkFault::MalformedAddress;
    assert!(
        client
            .finish_attempt(&attempt, AttemptOutcome::Fault { kind })
            .unwrap()
            .is_empty()
    );
    assert_eq!(client.status(), LinkStatus::Fault { kind });
    assert_eq!(client.next_deadline(), None);
    clock.advance(Duration::from_secs(60)).unwrap();
    assert_eq!(client.poll_time(), None);
    assert!(client.retry_after_fault());
    assert!(matches!(
        client.poll_time(),
        Some(ClientAction::Connect { .. })
    ));
}
