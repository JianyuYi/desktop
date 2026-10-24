//! Stop confirmation accepts only consistent, identity-matched host facts after Scope closure.
use super::restore_tests::{answer, session, with_trace};
use super::*;
use crate::session::PriorRevisionRestore;
use ora_node_db::{NodeDatabase, NodeIdentity};
use ora_node_protocol::*;
use pretty_assertions::assert_eq;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Confirmation and private-input cleanup must never execute another Git command.
struct NoRestoreGit;
impl gitlancer::GitRunner for NoRestoreGit {
    /// Any attempt to inspect or mutate a checkout during confirmation is a test failure.
    fn run(
        &self,
        _command: &gitlancer::GitCommand,
    ) -> Result<gitlancer::GitOutput, gitlancer::GitExecError> {
        panic!("stop confirmation executed Git")
    }
}
impl crate::revision::RestoreGitRunner for NoRestoreGit {
    /// No restore step is admitted while collecting original stop evidence.
    fn run_bounded_before(
        &self,
        _command: &gitlancer::GitCommand,
        _stdout: usize,
        _stderr: usize,
        _deadline: Instant,
    ) -> Result<gitlancer::GitOutput, gitlancer::GitExecError> {
        panic!("stop confirmation restarted restoration")
    }
}

/// The real IPC/SQLite boundary must not turn partial or contradictory non-launch facts into release.
#[test]
fn not_started_requires_closed_scope_consistent_facts_and_original_identity() -> TestResult {
    with_trace(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                ora_logging::initialize_test_clock();
                // Existing responder modes cover an open Scope, exit, authoritative non-launch,
                // unknown identity, contradictory launch, incomplete cleanup, and foreign identities.
                for (mode, confirmed) in [
                    (0, false),
                    (4, true),
                    (5, true),
                    (6, false),
                    (7, false),
                    (8, false),
                    (9, false),
                    (10, false),
                    (11, false),
                    (12, false),
                ] {
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
                        // SAFETY: geteuid only reads this test's identity.
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
                    let stops = RestoreStops::new(
                        journal.clone(),
                        ProcessConfig {
                            workload_uid: None,
                            host_directory: directory.path().to_path_buf(),
                            // SAFETY: geteuid only reads this test's identity.
                            expected_uid: unsafe { libc::geteuid() },
                            git_program: "/usr/bin/git".into(),
                            environment: BTreeMap::new(),
                            command_timeout_ms: 1000,
                            network_timeout_ms: 1000,
                            cleanup_timeout_ms: 100,
                            shutdown_grace_ms: 100,
                        },
                        Duration::from_secs(/*secs*/ 2),
                    );
                    let transport = crate::git_server::TransportRepository::location(
                        directory.path(),
                        crate::revision::RESTORE_TRANSPORT_KIND,
                        &input.execution_id,
                    );
                    std::fs::create_dir_all(&transport)?;
                    let restorer = crate::revision::RevisionRestorer::new(
                        crate::revision::DeliveryGit::new(
                            NoRestoreGit,
                            crate::revision::GitPolicy {
                                env: gitlancer::GitEnv::automation_defaults(),
                                config_args: Vec::new(),
                            },
                        ),
                        NoRestoreGit,
                        crate::git_server::GitServerAccess::new(
                            crate::GitAccessConfig {
                                ca_cert: directory.path().join("unused-ca"),
                                client_cert: directory.path().join("unused-cert"),
                                client_key: directory.path().join("unused-key"),
                            },
                            String::new(),
                            crate::git_server::RetryPolicy::DEFAULT,
                        ),
                        directory.path().to_path_buf(),
                        /*owner*/ None,
                        Shutdown::default(),
                        crate::revision::RestorePolicy {
                            deadline: Duration::from_secs(/*secs*/ 1),
                            stop_confirmation: Duration::from_secs(/*secs*/ 2),
                        },
                    )
                    .with_stops(stops.clone());
                    let listener =
                        tokio::net::UnixListener::bind(directory.path().join("host.sock"))?;
                    let task = {
                        let attempt = attempt.clone();
                        let journal = journal.clone();
                        tokio::spawn(async move {
                            loop {
                                let (stream, _) = listener.accept().await?;
                                // A failed Close can end the client's read before our reply is written.
                                if let Err(error) = answer(stream, mode, &attempt, &journal).await
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
                    assert_eq!(
                        stops
                            .poll(&input.execution_id, StopCheck::WithinBound)
                            .await?,
                        confirmed,
                        "incorrect stop confirmation for responder mode {mode}"
                    );
                    assert_eq!(stops.retains_inputs(&input.execution_id), !confirmed);
                    assert_eq!(restorer.confirm_stop(&input.execution_id).await, confirmed);
                    assert_eq!(transport.exists(), !confirmed);
                    if confirmed {
                        assert!(
                            journal
                                .pending_restore_attempts(&input.execution_id)?
                                .is_empty()
                        );
                        journal.end_session(&input.execution_id, ended.clone())?;
                        assert!(journal.end_session(&input.execution_id, ended).is_err());
                        assert_eq!(db.pending_events()?.len(), 1);
                    } else {
                        assert!(journal.end_session(&input.execution_id, ended).is_err());
                        assert!(db.pending_events()?.is_empty());
                        let pending = journal.pending_restore_attempts(&input.execution_id)?;
                        assert_eq!(pending.len(), 1);
                        assert_eq!(pending[0].process, attempt);
                    }
                    task.abort();
                    let _ = task.await;
                }
                Ok(())
            })
    })
}
