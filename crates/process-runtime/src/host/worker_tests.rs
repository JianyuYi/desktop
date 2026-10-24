//! Exercise non-launch fencing through the production worker and real typed Unix exchanges.
use super::*;
use ora_process_protocol::*;
use pretty_assertions::assert_eq;
use std::num::NonZeroU64;
use std::os::unix::fs::PermissionsExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Case {
    StaleBinding,
    StoppedUnknownRun,
    RunningUnknownRun,
    StaleStop,
}

/// Reads one bounded frame using the same typed protocol that the real guardian decodes.
async fn read_request<T: serde::de::DeserializeOwned>(
    stream: &mut UnixStream,
) -> std::io::Result<T> {
    let length = stream.read_u32().await? as usize;
    assert!(length <= GUARDIAN_MAX_FRAME);
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).await?;
    decode_guardian_payload(&payload)
}

/// Only a bound Stop/UnknownRun permits synthesis; stale authority and Start/UnknownRun stay unknown.
#[tokio::test]
async fn nonlaunch_requires_bound_stop_and_never_sends_start_after_stop() -> TestResult {
    for case in [
        Case::StaleBinding,
        Case::StoppedUnknownRun,
        Case::RunningUnknownRun,
        Case::StaleStop,
    ] {
        let home = std::env::var_os("HOME").ok_or("fixture HOME missing")?;
        let directory = tempfile::Builder::new()
            .prefix("wf")
            .permissions(std::fs::Permissions::from_mode(/*mode*/ 0o700))
            .tempdir_in(home)?;
        let binding = HostBinding {
            epoch: NonZeroU64::MIN,
            instance: HostInstanceId::new(),
        };
        let access = GuardianAccess {
            scope_dir: directory.path().to_path_buf(),
            intent: ScopeCreationIntent {
                scope: ScopeId::new(),
                guardian: GuardianInstanceId::new(),
                created_by: binding,
            },
        };
        let intent = HostRunIntent {
            scope: access.intent.scope,
            run: RunId::new(),
            spec: RunSpec::new(
                "/usr/bin/true",
                directory.path(),
                DescendantPolicy::WaitForAll,
            ),
            host_disconnect: GuardianHostDisconnect::KeepRunning,
        };
        let stop = case != Case::RunningUnknownRun;
        let listener = UnixListener::bind(
            directory
                .path()
                .join(GuardianChannel::Control.socket_name()),
        )?;
        let guardian_intent = access.intent.clone();
        let expected_start = intent.start_operation();
        let run = intent.run;
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let request: GuardianManagementRequest = read_request(&mut stream).await?;
            assert_eq!(request.intent, guardian_intent);
            assert_eq!(request.channel, GuardianChannel::Control);
            assert!(
                matches!(request.operation, GuardianManagementOperation::Bind { host } if host == binding)
            );
            let session = GuardianHostSession { host: binding };
            let reply = if case == Case::StaleBinding {
                GuardianManagementReply::Rejected(GuardianManagementRejection::StaleHost)
            } else {
                GuardianManagementReply::Bound {
                    intent: guardian_intent.clone(),
                    session: session.clone(),
                }
            };
            stream.write_all(&encode_guardian_frame(&reply)?).await?;
            if case == Case::StaleBinding {
                return Ok::<_, std::io::Error>(vec![]);
            }
            let mut operations = Vec::new();
            for index in 0..2 {
                let (mut stream, _) = listener.accept().await?;
                let request: GuardianRunRequest = read_request(&mut stream).await?;
                assert_eq!(request.intent, guardian_intent);
                assert_eq!(request.session, session);
                assert_eq!(request.channel, GuardianChannel::Control);
                let result = if index == 0 {
                    assert_eq!(
                        request.operation,
                        if stop {
                            GuardianRunOperation::Close
                        } else {
                            GuardianRunOperation::Scope
                        }
                    );
                    GuardianRunResult::Scope(ScopeState::Closed(
                        CleanupEvidence::BestEffortComplete,
                    ))
                } else {
                    assert_eq!(
                        request.operation,
                        if stop {
                            GuardianRunOperation::Stop { run }
                        } else {
                            expected_start.clone()
                        }
                    );
                    GuardianRunResult::Rejected(if case == Case::StaleStop {
                        GuardianRunRejection::Management(GuardianManagementRejection::StaleSession)
                    } else {
                        GuardianRunRejection::UnknownRun
                    })
                };
                operations.push(request.operation);
                stream
                    .write_all(&encode_guardian_frame(&GuardianRunReply {
                        intent: guardian_intent.clone(),
                        session: session.clone(),
                        result,
                    })?)
                    .await?;
            }
            Ok(operations)
        });
        let observation = tokio::time::timeout(
            std::time::Duration::from_secs(/*secs*/ 2),
            Work {
                access: Some(access),
                launch: None,
                binding,
                // SAFETY: geteuid only reads the current process identity.
                owner: unsafe { libc::geteuid() },
                close: stop,
                runs: vec![(intent, stop)],
            }
            .observe(),
        )
        .await?;
        let operations = peer.await??;
        if case == Case::StoppedUnknownRun {
            assert_eq!(
                operations,
                vec![
                    GuardianRunOperation::Close,
                    GuardianRunOperation::Stop { run }
                ]
            );
            assert_eq!(
                observation.runs,
                vec![RunSnapshot {
                    id: run,
                    launch: LaunchFact::NotStarted("cancelled before guardian acceptance".into()),
                    direct: DirectProcessState::NotStarted,
                    cleanup: CleanupState::Complete(CleanupEvidence::BestEffortComplete),
                }]
            );
            assert_eq!(observation.status, HostCoordination::Observing);
        } else {
            assert!(
                observation.runs.is_empty(),
                "missing fencing manufactured NotStarted"
            );
            let rejection = match case {
                Case::StaleBinding => {
                    GuardianRunRejection::Management(GuardianManagementRejection::StaleHost)
                }
                Case::RunningUnknownRun => GuardianRunRejection::UnknownRun,
                Case::StaleStop => {
                    GuardianRunRejection::Management(GuardianManagementRejection::StaleSession)
                }
                Case::StoppedUnknownRun => unreachable!(),
            };
            assert_eq!(observation.status, HostCoordination::Rejected(rejection));
        }
    }
    Ok(())
}
