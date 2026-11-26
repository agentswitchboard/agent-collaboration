//! The binding receives the real reader's end while its ordered writer stays live.

use super::*;
use acp_client_runtime::{
    acp_transport::TransportRun,
    owner_wire::{OpeningPausePoint, OpeningReaderEnd},
};
use provider_session_machines::harness_lifecycle::EventRefusal;

#[derive(Clone, Copy)]
enum PortResponse {
    Accept,
    EndAfterLiveCatalogue,
    RejectInitialSkills,
}

struct RecordReaderEvents {
    facts: tokio::sync::mpsc::UnboundedSender<SessionLifecycleFact>,
    response: PortResponse,
}

impl EventPort<SessionLifecycleFact> for RecordReaderEvents {
    fn send<'a>(&'a self, fact: SessionLifecycleFact) -> EventSendFuture<'a>
    where
        SessionLifecycleFact: 'a,
    {
        Box::pin(async move {
            let reject = matches!(
                (&fact, self.response),
                (
                    SessionLifecycleFact::CatalogueObserved { .. },
                    PortResponse::EndAfterLiveCatalogue
                ) | (
                    SessionLifecycleFact::SkillView { .. },
                    PortResponse::RejectInitialSkills
                )
            );
            self.facts
                .send(fact)
                .expect("test still observes this reader");
            if reject {
                Err(EventRefusal::Retired)
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn natural_reader_finish_and_real_end_failures_reach_the_binding() {
    for end in [
        OpeningReaderEnd::Finished,
        OpeningReaderEnd::Failed,
        OpeningReaderEnd::IngressClosed,
        OpeningReaderEnd::HandoffClosed,
    ] {
        assert_live_writer_receives_the_exact_reader_end(end).await;
    }
}

async fn assert_live_writer_receives_the_exact_reader_end(expected: OpeningReaderEnd) {
    let behaviours: &[&str] = match expected {
        OpeningReaderEnd::Finished => &["hold-created-update"],
        OpeningReaderEnd::Failed => &["reader-projection-error"],
        OpeningReaderEnd::IngressClosed => &["restore-skill-view"],
        OpeningReaderEnd::HandoffClosed => &[],
        OpeningReaderEnd::Retired => panic!("run-local retirement has its own schedule"),
    };
    let fixture = hub_runtime_with(behaviours).await;
    let wire = fixture.runtime.client.owner_wire();
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let generation = SessionGeneration::from_number(1);
    let run = RunAttempt {
        generation,
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let (setup, query, accepted, port) = if expected == OpeningReaderEnd::Finished {
        let create = CreateAttempt(Uuid::now_v7());
        let accepted = prepare_created_reader(&fixture, owner, create, &mut binding).await;
        (
            ActivationSetup::Created {
                attempt: create,
                generation,
            },
            OpenAttempt::Create(create),
            accepted,
            PortResponse::EndAfterLiveCatalogue,
        )
    } else {
        let (readback, _) =
            load_fixture_binding(&fixture, &mut binding, owner, run, CancellationToken::new())
                .await;
        let accepted = AcceptedRunSettings {
            catalogue: readback.catalogue,
            router_auto_requested: false,
        };
        binding
            .accept_settings(OpenAttempt::Restore(run), &accepted)
            .expect("prepare the real restored SDK");
        let port = if expected == OpeningReaderEnd::IngressClosed {
            PortResponse::RejectInitialSkills
        } else {
            PortResponse::Accept
        };
        (
            ActivationSetup::Restored(run),
            OpenAttempt::Restore(run),
            accepted,
            port,
        )
    };
    if expected == OpeningReaderEnd::HandoffClosed {
        wire.close_next_restored_reader_start_for_test();
    }
    let observed = wire.observe_next_reader_ending_for_test();
    let after_step = wire.pause_opening_for_test(OpeningPausePoint::ReaderStoppedDelivered);
    let (send, mut facts) = tokio::sync::mpsc::unbounded_channel();
    binding
        .activate(
            setup,
            accepted,
            Arc::new(RecordReaderEvents {
                facts: send,
                response: port,
            }),
            Box::pin(std::future::pending()),
        )
        .expect("install the real reader before its ending");
    match expected {
        OpeningReaderEnd::Finished => {
            node_test_fixture::release_fixture(&fixture.receipt, ".release-created-update")
                .expect("release the real final catalogue update")
        }
        OpeningReaderEnd::Failed => {
            node_test_fixture::release_fixture(&fixture.receipt, ".release-reader-error")
                .expect("release the real projection failure")
        }
        OpeningReaderEnd::IngressClosed
        | OpeningReaderEnd::HandoffClosed
        | OpeningReaderEnd::Retired => {}
    }
    assert_eq!(
        observed
            .await
            .expect("the installed reader observed its ending"),
        (
            TransportRun {
                incarnation: owner.incarnation,
                generation
            },
            expected,
        )
    );
    after_step
        .reached
        .await
        .expect("the worker delivered its finding to the actual opening row");
    assert_eq!(
        binding.opening_is_held(query),
        Held::NotHeld(LifecycleRefusal::BoundSessionNotLoaded),
        "every real reader ending must end liveness before the next binding call"
    );
    assert!(
        !fixture.runtime.retirement().is_cancelled(),
        "the writer connection is still open"
    );
    after_step
        .release
        .send(())
        .expect("finish releasing the reader");

    let replacement = RunAttempt {
        generation: SessionGeneration::from_number(2),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    assert_eq!(
        load_fixture_binding(
            &fixture,
            &mut binding,
            owner,
            replacement,
            CancellationToken::new()
        )
        .await
        .1,
        ReplayCompleteness::Complete,
        "the same real writer still accepts a subsequent restore"
    );
    binding.retire(replacement.generation);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
    let mut reports = 0;
    let mut saw_catalogue = false;
    let mut saw_skills = false;
    while let Ok(fact) = facts.try_recv() {
        match fact {
            SessionLifecycleFact::RunObservationFailed { run } => {
                assert_eq!(run, generation);
                reports += 1;
            }
            SessionLifecycleFact::CatalogueObserved { run, .. } => {
                assert_eq!(run, generation);
                saw_catalogue = true;
            }
            SessionLifecycleFact::SkillView { run, .. } => {
                assert_eq!(run, generation);
                saw_skills = true;
            }
            _ => {}
        }
    }
    assert_eq!(
        reports,
        usize::from(matches!(
            expected,
            OpeningReaderEnd::Failed | OpeningReaderEnd::HandoffClosed
        )),
        "unexpected loss reporting for {expected:?}",
    );
    if expected == OpeningReaderEnd::Finished {
        assert!(
            saw_catalogue,
            "the real live update reached the ending event port"
        );
    }
    if expected == OpeningReaderEnd::IngressClosed {
        assert!(
            saw_skills,
            "the restored skill view reached the closed ingress"
        );
    }
}

pub(super) async fn prepare_created_reader(
    fixture: &HubRuntime,
    owner: OwnerAddress,
    create: CreateAttempt,
    binding: &mut SessionHarnessBinding,
) -> AcceptedRunSettings {
    let (reply, opened) = oneshot::channel();
    binding
        .open(
            OpenRequest::Create {
                owner,
                attempt: create,
                setup: OpenSetup {
                    cwd: fixture._root.path().to_owned(),
                    work_roots: Default::default(),
                    scratch_folder: None,
                },
                requested: Default::default(),
            },
            Box::new(Reply(reply)),
            Arc::new(AcceptEvents),
            Box::pin(std::future::pending()),
        )
        .expect("send real create to the live writer");
    assert!(matches!(opened.await.expect("the actual create reply"),
        OpeningFact::Created { attempt, .. } if attempt == create));
    let (reply, configured) = oneshot::channel();
    binding
        .configure_created(
            create,
            Box::new(Reply(reply)),
            Box::pin(std::future::pending()),
        )
        .expect("configure the real SDK");
    let CreatedSetupFact::Configured { attempt, report } =
        configured.await.expect("real setup reply")
    else {
        panic!("expected configured create for the real reader");
    };
    assert_eq!(attempt, create);
    AcceptedRunSettings {
        catalogue: report.catalogue,
        router_auto_requested: report.router_auto_requested,
    }
}
