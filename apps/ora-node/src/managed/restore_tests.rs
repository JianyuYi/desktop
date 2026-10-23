//! Fault-injected real Unix IPC plus the production stop monitor and durable session ledger.
use super::*;
use ora_node_db::{NodeDatabase, NodeIdentity, SessionJournal};
use ora_node_protocol::*;
use pretty_assertions::assert_eq;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Scoped TRACE keeps shared production callsites observable in every test ordering.
pub(super) fn with_trace<T>(action: impl FnOnce() -> T) -> T {
    let dispatcher = tracing::Dispatch::new(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .finish(),
    );
    tracing::dispatcher::with_default(&dispatcher, action)
}

/// A valid resumed session, independent of any plugin or model provider.
pub(super) fn session() -> StartAgentSessionMessage {
    StartAgentSessionMessage {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        operation_id: OperationId::new("restore-op"),
        execution_id: ExecutionId::new("restore-session"),
        payload: StartAgentSession {
            spec: AgentSessionSpec {
                workspace_directory: Vec::new(),
                agent_configuration: None,
                node_id: NodeId::new("node"),
                agent_plugin_id: PluginId::new("official/agent"),
                agent_plugin_version: PluginVersion::new("1.0.0"),
                checkout_execution_id: ExecutionId::new("clone"),
                model_binding_id: None,
                git_identity: GitIdentity {
                    name: "Fixture".into(),
                    email: "fixture@example.test".into(),
                },
                initial_turn: UserTurn {
                    turn_id: TurnId::new("initial"),
                    content: vec![ContentBlock::Text {
                        text: "fixture".into(),
                    }],
                },
                prior_revision: Some(PriorRevision {
                    revision_id: RevisionId::new("prior"),
                    final_commit: CommitId::new("0".repeat(40)),
                    git_repository_url: GitRepositoryUrl::parse(
                        "https://git.ora.test/tenant/project.git",
                    )
                    .expect("valid repository URL"),
                    delivery_ref: DeliveryRef::parse("refs/ora/deliveries/prior")
                        .expect("valid delivery ref"),
                    base_commit: CommitId::new("0".repeat(40)),
                }),
                trigger: None,
            },
        },
    }
}

/// An IPC responder can withhold each independent part of the required stop evidence.
pub(super) async fn answer(
    mut stream: tokio::net::UnixStream,
    mode: u8,
    attempt: &ora_node_db::ProcessAttempt,
    journal: &SessionJournal,
) -> std::io::Result<()> {
    let size = stream.read_u32().await? as usize;
    assert!(size <= GUARDIAN_MAX_FRAME);
    let mut bytes = vec![0; size];
    stream.read_exact(&mut bytes).await?;
    let request: HostRequest = decode_guardian_payload(&bytes)?;
    let closed = mode != 0;
    let scope = HostScopeView {
        scope: attempt.intent.scope,
        close_requested: true,
        last_observed: Some(if closed {
            ScopeState::Closed(CleanupEvidence::BestEffortComplete)
        } else {
            ScopeState::Closing
        }),
        coordination: HostCoordination::Observing,
    };
    let reply = match request.operation {
        HostOperation::CreateScope { scope: id } | HostOperation::QueryScope { scope: id } => {
            assert_eq!(id, attempt.intent.scope);
            HostReply::Scope(scope)
        }
        HostOperation::Close { scope: id } => {
            assert_eq!(id, attempt.intent.scope);
            assert!(
                journal
                    .pending_restore_attempts(&attempt.execution)
                    .map_err(std::io::Error::other)?[0]
                    .first_stop_at_ms
                    .is_some(),
                "Close preceded the durable stop anchor"
            );
            if mode == 3 {
                HostReply::Rejected(HostRejection::GuardianUnavailable)
            } else {
                HostReply::Scope(scope)
            }
        }
        HostOperation::QueryRun { run } => {
            assert_eq!(run, attempt.intent.run);
            if mode == 6 {
                HostReply::Rejected(HostRejection::UnknownIdentity)
            } else {
                HostReply::Run(HostRunView {
                    scope: if mode == 10 {
                        ScopeId::new()
                    } else {
                        attempt.intent.scope
                    },
                    run: if mode == 12 { RunId::new() } else { run },
                    stop_requested: true,
                    last_observed: Some(RunSnapshot {
                        id: if mode == 11 { RunId::new() } else { run },
                        launch: match mode {
                            5 | 8 => LaunchFact::NotStarted(
                                "cancelled before guardian acceptance".into(),
                            ),
                            9 => LaunchFact::Unknown("launch not observed".into()),
                            _ => LaunchFact::Started,
                        },
                        direct: match mode {
                            1 => DirectProcessState::Running,
                            2 => DirectProcessState::Unknown,
                            5 | 7 | 8 | 9 => DirectProcessState::NotStarted,
                            _ => DirectProcessState::Exited(ExitOutcome::Code(0)),
                        },
                        cleanup: if mode == 8 {
                            CleanupState::Pending
                        } else {
                            CleanupState::Complete(CleanupEvidence::BestEffortComplete)
                        },
                    }),
                    coordination: HostCoordination::Observing,
                })
            }
        }
        other => panic!("stop reconciliation invented execution work: {other:?}"),
    };
    stream.write_all(&encode_guardian_frame(&reply)?).await
}

/// Unknown evidence cannot authorize terminal, a new restore, delivery, or checkout reuse.
#[test]
fn unknown_stop_matrix_expires_without_terminal_then_original_proof_settles_once() -> TestResult {
    with_trace(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                ora_logging::initialize_test_clock();
                for initial in [0, 1, 2, 3, 6] {
                    let directory = tempfile::tempdir()?;
                    let input = session();
                    let mut db = NodeDatabase::open(
                        &directory.path().join("db"),
                        NodeIdentity::Require(NodeId::new("node")),
                    )?;
                    db.accept_session(&input)?;
                    db.start_session(&input, &NodeIncarnationId::new("original"))?;
                    let journal = db.session_journal()?;
                    let attempt = ora_node_db::ProcessAttempt {
                        execution: input.execution_id.clone(),
                        host_directory: directory.path().to_path_buf(),
                        expected_uid: unsafe { libc::geteuid() },
                        intent: HostRunIntent {
                            scope: ScopeId::new(),
                            run: RunId::new(),
                            spec: RunSpec::new(
                                "git",
                                directory.path(),
                                DescendantPolicy::WaitForAll,
                            ),
                            host_disconnect: GuardianHostDisconnect::KeepRunning,
                        },
                    };
                    journal.record_restore_attempt(&attempt, StopContainment::BestEffort)?;
                    let ended = AgentSessionEnded {
                        node: NodeRuntimeIdentity {
                            node_id: NodeId::new("node"),
                            incarnation_id: NodeIncarnationId::new("original"),
                        },
                        reason: AgentSessionEndReason::AgentFailed,
                        detail: Some("prior_revision_unavailable".into()),
                        scheduled_result: None,
                        artifact_manifest: None,
                    };
                    journal.defer_restore_end(&input.execution_id, &ended)?;
                    let config = ProcessConfig {
                        workload_uid: None,
                        host_directory: directory.path().to_path_buf(),
                        expected_uid: unsafe { libc::geteuid() },
                        git_program: "/usr/bin/git".into(),
                        environment: BTreeMap::new(),
                        command_timeout_ms: 1000,
                        network_timeout_ms: 1000,
                        cleanup_timeout_ms: 40,
                        shutdown_grace_ms: 100,
                    };
                    let stops =
                        RestoreStops::new(journal.clone(), config, Duration::from_secs(/*secs*/ 2));
                    let mode = Arc::new(AtomicU8::new(initial));
                    let calls = Arc::new(AtomicUsize::new(/*v*/ 0));
                    let listener =
                        tokio::net::UnixListener::bind(directory.path().join("host.sock"))?;
                    let task = {
                        let mode = Arc::clone(&mode);
                        let calls = Arc::clone(&calls);
                        let attempt = attempt.clone();
                        let journal = journal.clone();
                        tokio::spawn(async move {
                            loop {
                                let (stream, _) = listener.accept().await?;
                                calls.fetch_add(1, Ordering::SeqCst);
                                if let Err(error) =
                                    answer(stream, mode.load(Ordering::SeqCst), &attempt, &journal)
                                        .await
                                    && !matches!(
                                        error.kind(),
                                        std::io::ErrorKind::BrokenPipe
                                            | std::io::ErrorKind::ConnectionReset
                                            | std::io::ErrorKind::UnexpectedEof
                                    )
                                {
                                    return Err(error);
                                }
                            }
                            #[allow(unreachable_code)]
                            Ok::<(), std::io::Error>(())
                        })
                    };
                    assert!(
                        !stops
                            .poll(&input.execution_id, StopCheck::WithinBound)
                            .await?
                    );
                    assert!(
                        journal
                            .end_session(&input.execution_id, ended.clone())
                            .is_err()
                    );
                    assert!(db.pending_events()?.is_empty());
                    assert!(
                        !tokio::time::timeout(
                            Duration::from_secs(/*secs*/ 3),
                            stops.wait(&input.execution_id)
                        )
                        .await?
                    );
                    let before = journal.pending_restore_attempts(&input.execution_id)?;
                    assert_eq!(before.len(), 1);
                    assert!(before[0].stop_unconfirmed);
                    let count = calls.load(Ordering::SeqCst);
                    assert!(
                        !stops
                            .poll(&input.execution_id, StopCheck::WithinBound)
                            .await?
                    );
                    assert_eq!(
                        calls.load(Ordering::SeqCst),
                        count,
                        "expired polling continued"
                    );
                    assert_eq!(
                        journal.pending_restore_attempts(&input.execution_id)?,
                        before
                    );
                    // The same identities supply late facts. No Start or replacement Scope is accepted above.
                    mode.store(4, Ordering::SeqCst);
                    assert!(stops.poll(&input.execution_id, StopCheck::Handover).await?);
                    journal.end_session(
                        &input.execution_id,
                        journal
                            .deferred_restore_end(&input.execution_id)?
                            .ok_or("missing deferred end")?,
                    )?;
                    assert!(journal.end_session(&input.execution_id, ended).is_err());
                    assert_eq!(db.pending_events()?.len(), 1);
                    task.abort();
                    let _ = task.await;
                }
                Ok(())
            })
    })
}

/// Host loss crosses a real guardian lifetime; restart settles only the recorded original Run.
#[test]
fn original_restore_run_survives_host_loss_and_is_settled_after_bound() -> TestResult {
    with_trace(|| {
        ora_logging::initialize_test_clock();
        let mut fixture = super::deadline_tests::Fixture::new()?;
        let root = fixture.directory.path();
        let config = fixture.config();
        let input = session();
        let mut db = NodeDatabase::open(
            &root.join("node.sqlite"),
            NodeIdentity::Require(NodeId::new("node")),
        )?;
        db.accept_session(&input)?;
        db.start_session(&input, &NodeIncarnationId::new("original"))?;
        let journal = db.session_journal()?;
        let stops = RestoreStops::new(
            journal.clone(),
            config.clone(),
            Duration::from_millis(/*millis*/ 200),
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let client = ProcessHost::new(config.host_directory.clone(), config.expected_uid);
        let mut spec = RunSpec::new(
            "/usr/bin/git",
            root,
            DescendantPolicy::Cleanup {
                grace: Duration::ZERO,
            },
        );
        spec.args = [
            "-c",
            "alias.pause=!printf started > restore-started; exec sleep 60",
            "pause",
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        let attempt = ora_node_db::ProcessAttempt {
            execution: input.execution_id.clone(),
            host_directory: config.host_directory.clone(),
            expected_uid: config.expected_uid,
            intent: HostRunIntent {
                scope: ScopeId::new(),
                run: RunId::new(),
                spec,
                host_disconnect: GuardianHostDisconnect::KeepRunning,
            },
        };
        journal.record_restore_attempt(&attempt, StopContainment::BestEffort)?;
        runtime.block_on(async {
            let deadline = Instant::now() + Duration::from_secs(/*secs*/ 15);
            while !matches!(
                client.execute(HostOperation::Inspect).await,
                Ok(HostReply::Ready(_))
            ) {
                assert!(Instant::now() < deadline, "host startup timed out");
                tokio::time::sleep(Duration::from_millis(/*millis*/ 10)).await;
            }
            client
                .execute(HostOperation::CreateScope {
                    scope: attempt.intent.scope,
                })
                .await?;
            client
                .execute(HostOperation::Start {
                    intent: attempt.intent.clone(),
                })
                .await?;
            while !root.join("restore-started").is_file() {
                assert!(Instant::now() < deadline, "original Git never started");
                tokio::time::sleep(Duration::from_millis(/*millis*/ 10)).await;
            }
            Ok::<(), std::io::Error>(())
        })?;
        fixture.host.kill()?;
        fixture.host.wait()?;
        // Persisted origin predates this recovered monitor; no wall-clock sleep is needed for expiry.
        let now = RestoreStops::now_ms();
        journal.request_restore_stop(attempt.intent.run, now - 1000, 200)?;
        assert!(!runtime.block_on(stops.wait(&input.execution_id)));
        let unknown = journal.pending_restore_attempts(&input.execution_id)?;
        assert!(unknown[0].stop_unconfirmed);
        let binaries = std::env::current_exe()?
            .parent()
            .and_then(std::path::Path::parent)
            .ok_or("missing executable parent")?
            .to_path_buf();
        fixture.host = std::process::Command::new(binaries.join("ora-process-host"))
            .arg("recover")
            .arg(&config.host_directory)
            .arg(root.join("guardian"))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .spawn()?;
        runtime.block_on(async {
        let deadline = Instant::now() + Duration::from_secs(/*secs*/ 15);
        loop {
            if stops.poll(&input.execution_id, StopCheck::Handover).await? { break; }
            if Instant::now() >= deadline {
                let scope = client.execute(HostOperation::QueryScope { scope: attempt.intent.scope }).await;
                let run = client.execute(HostOperation::QueryRun { run: attempt.intent.run }).await;
                panic!("original stop proof did not arrive after host recovery: scope={scope:?}; run={run:?}");
            }
            tokio::time::sleep(Duration::from_millis(/*millis*/ 25)).await;
        }
        assert!(matches!(client.execute(HostOperation::QueryRun { run: attempt.intent.run }).await?, HostReply::Run(view) if view.last_observed.as_ref().is_some_and(|snapshot| matches!(snapshot.direct, DirectProcessState::Exited(_)))));
        Ok::<(), std::io::Error>(())
    })?;
        assert!(
            journal
                .pending_restore_attempts(&input.execution_id)?
                .is_empty()
        );
        let host_db = rusqlite::Connection::open_with_flags(
            config.host_directory.join("host.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let count: i64 = host_db.query_row("SELECT count(*) FROM run_intents", [], |row| {
            row.get(/*idx*/ 0)
        })?;
        assert_eq!(count, 1, "host recovery created a replacement Run");
        Ok(())
    })
}
