use ora_process_protocol::{
    ContainmentGuarantee, ContainmentRequest, DescendantPolicy, LaunchFact, RunId, RunSpec,
};
use ora_process_runtime::{Platform, PlatformCapabilities, ScopeRuntime, SpawnError, StartError};
use ora_process_runtime::{PlatformError, PlatformObservation, StopSignal};
use pretty_assertions::assert_eq;

#[derive(Default)]
struct UncertainPlatform {
    launched: bool,
}

impl Platform for UncertainPlatform {
    /// Models a deployment that only promises best-effort cleanup.
    fn capabilities(&self) -> PlatformCapabilities {
        PlatformCapabilities::BestEffortOnly
    }

    /// A second launch would produce different facts, exposing accidental retries to the test.
    fn spawn(
        &mut self,
        _run: RunId,
        _spec: &RunSpec,
        _guarantee: ContainmentGuarantee,
    ) -> Result<(), SpawnError> {
        if self.launched {
            Ok(())
        } else {
            self.launched = true;
            Err(SpawnError::Unknown("launch reply lost".into()))
        }
    }

    /// Models a temporarily unavailable observation path.
    fn observe(&mut self, _run: RunId) -> Result<PlatformObservation, PlatformError> {
        Err(PlatformError("observation unavailable".into()))
    }

    /// Models a temporarily unavailable cleanup path.
    fn signal(&mut self, _run: RunId, _signal: StopSignal) -> Result<(), PlatformError> {
        Err(PlatformError("cleanup unavailable".into()))
    }
}

/// specs/test-cases/node/process/lifecycle/attempts-and-closure.md#replaying-a-run-never-creates-a-second-launch-attempt
#[test]
fn unknown_launch_replays_original_facts_and_rejects_changed_parameters() {
    let mut scope = ScopeRuntime::new(
        ContainmentRequest::PreferStrong,
        UncertainPlatform::default(),
    )
    .unwrap_or_else(|error| panic!("create scope: {error}"));
    let id = RunId::new();
    let spec = RunSpec::new("git", ".", DescendantPolicy::WaitForAll);
    let first = scope
        .start(id, spec.clone())
        .unwrap_or_else(|error| panic!("start: {error}"));
    assert_eq!(
        first.launch,
        LaunchFact::Unknown("launch reply lost".into())
    );
    assert_eq!(scope.start(id, spec), Ok(first.clone()));
    assert_eq!(scope.run(id), Some(first));
    assert_eq!(
        scope.start(
            id,
            RunSpec::new("plugin", ".", DescendantPolicy::WaitForAll)
        ),
        Err(StartError::ConflictingRun(id))
    );
}

struct NonLaunchingPlatform;

impl Platform for NonLaunchingPlatform {
    /// Both guarantees are available so the test verifies evidence rather than admission fallback.
    fn capabilities(&self) -> PlatformCapabilities {
        PlatformCapabilities::StrongAndBestEffort
    }
    /// Explicit platform non-execution is a positive fact, not an observation timeout.
    fn spawn(
        &mut self,
        _run: RunId,
        _spec: &RunSpec,
        _guarantee: ContainmentGuarantee,
    ) -> Result<(), SpawnError> {
        Err(SpawnError::NotStarted(
            "platform refused before spawn".into(),
        ))
    }
    /// A proven non-launch needs no process observation to manufacture exit.
    fn observe(&mut self, _run: RunId) -> Result<PlatformObservation, PlatformError> {
        panic!("proven non-launch must not be observed as a process")
    }
    /// No process exists to signal after this platform-confirmed non-launch.
    fn signal(&mut self, _run: RunId, _signal: StopSignal) -> Result<(), PlatformError> {
        panic!("proven non-launch must not receive process signals")
    }
}

/// Platform non-launch reports the actual selected guarantee; BestEffort never claims quiescence.
#[test]
fn platform_nonlaunch_preserves_cleanup_evidence_for_each_guarantee()
-> Result<(), Box<dyn std::error::Error>> {
    use ora_process_protocol::{
        CleanupEvidence, CleanupState, DirectProcessState, RunSnapshot, ScopeState, StopRequest,
    };
    use std::time::{Duration, Instant};
    for (request, evidence) in [
        (
            ContainmentRequest::BestEffort,
            CleanupEvidence::BestEffortComplete,
        ),
        (
            ContainmentRequest::RequireStrong,
            CleanupEvidence::ConfirmedQuiescence,
        ),
    ] {
        let mut scope = ScopeRuntime::new(request, NonLaunchingPlatform)?;
        let run = RunId::new();
        let snapshot = scope.start(run, RunSpec::new("git", ".", DescendantPolicy::WaitForAll))?;
        assert_eq!(
            snapshot,
            RunSnapshot {
                id: run,
                launch: LaunchFact::NotStarted("platform refused before spawn".into()),
                direct: DirectProcessState::NotStarted,
                cleanup: CleanupState::Complete(evidence)
            }
        );
        let now = Instant::now();
        scope.close(StopRequest::Force, now)?;
        assert_eq!(
            scope.reconcile(now + Duration::from_millis(/*millis*/ 1)),
            vec![]
        );
        assert_eq!(scope.state(), ScopeState::Closed(evidence));
    }
    Ok(())
}
