//! Reproducible samples at the exact durable-intent/first-dispatch crash boundary.
use super::*;
use ora_process_protocol::*;
use pretty_assertions::assert_eq;
use std::os::unix::fs::PermissionsExt;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// A recovered host reports a positive non-launch fact only for a Run it actually accepted.
#[tokio::test]
async fn restart_before_first_dispatch_distinguishes_recorded_and_unknown_runs() -> TestResult {
    let home = std::env::var_os("HOME").ok_or("fixture HOME missing")?;
    let directory = tempfile::Builder::new()
        .prefix("nw")
        .permissions(std::fs::Permissions::from_mode(/*mode*/ 0o700))
        .tempdir_in(home)?;
    let mut recorded_nonlaunch = 0;
    let mut unknown_identity = 0;
    // Equal sampling is intentional injection, not an estimate of production frequency.
    for sample in 0..20 {
        for registered in [false, true] {
            let root = directory
                .path()
                .join(format!("{sample}-{}", usize::from(registered)));
            let scope = ScopeId::new();
            let intent = HostRunIntent {
                scope,
                run: RunId::new(),
                spec: RunSpec::new(
                    "/usr/bin/true",
                    directory.path(),
                    DescendantPolicy::WaitForAll,
                ),
                host_disconnect: GuardianHostDisconnect::KeepRunning,
            };
            let state = HostState::create(&root)?;
            let mut host = HostCoordinator::new(state, directory.path().join("unused"));
            host.create_scope(scope)?;
            if registered {
                host.start(intent.clone())?;
            }
            // Drop before tick: the journal is real but no guardian/Start can have been dispatched.
            let old_binding = host.binding();
            drop(host);
            let mut host =
                HostCoordinator::new(HostState::recover(&root)?, directory.path().join("unused"));
            assert!(host.binding().epoch > old_binding.epoch);
            host.close(scope)?;
            let deadline = Instant::now() + Duration::from_secs(/*secs*/ 2);
            loop {
                host.tick()?;
                if matches!(
                    host.query_scope(scope)?.last_observed,
                    Some(ScopeState::Closed(_))
                ) {
                    break;
                }
                assert!(Instant::now() < deadline, "recovered Scope did not close");
                tokio::time::sleep(Duration::from_millis(/*millis*/ 5)).await;
            }
            if registered {
                assert_eq!(
                    host.query_run(intent.run)?.last_observed,
                    Some(RunSnapshot {
                        id: intent.run,
                        launch: LaunchFact::NotStarted(
                            "cancelled before guardian acceptance".into()
                        ),
                        direct: DirectProcessState::NotStarted,
                        cleanup: CleanupState::Complete(CleanupEvidence::BestEffortComplete),
                    })
                );
                assert!(
                    host.start(HostRunIntent {
                        run: RunId::new(),
                        ..intent
                    })
                    .is_err(),
                    "closed recovered Scope admitted another Start"
                );
                recorded_nonlaunch += 1;
            } else {
                assert!(matches!(
                    host.query_run(intent.run),
                    Err(ProcessStateError::Rejected("unknown host Run"))
                ));
                unknown_identity += 1;
            }
        }
    }
    assert_eq!((recorded_nonlaunch, unknown_identity), (20, 20));
    println!(
        "host restart window: registered-before-dispatch NotStarted={recorded_nonlaunch}/20; unregistered UnknownIdentity={unknown_identity}/20; controlled samples only"
    );
    Ok(())
}
