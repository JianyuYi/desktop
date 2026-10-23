use ora_domain::{
    ScheduledArtifactRejection, ScheduledMissedOccurrence, ScheduledOccurrence, ScheduledRun,
    ScheduledRunArtifact, ScheduledRunId, ScheduledRunTransition, ScheduledRunVerdict,
    ScheduledTask, ScheduledTaskHistoryEntry, ScheduledTaskId,
};
use std::path::Path;

use crate::{RepositoryError, TaskWorktreeProvisionerError};

/// Defines persistence operations required by scheduled-task definition use cases.
pub trait ScheduledTaskRepository {
    /// Persists a newly created task definition.
    fn create_scheduled_task(&self, task: ScheduledTask) -> Result<ScheduledTask, RepositoryError>;

    /// Loads one visible task definition by identifier.
    fn find_scheduled_task(
        &self,
        task_id: &ScheduledTaskId,
    ) -> Result<Option<ScheduledTask>, RepositoryError>;

    /// Lists visible task definitions in deterministic storage order.
    fn list_scheduled_tasks(&self) -> Result<Vec<ScheduledTask>, RepositoryError>;

    /// Replaces one visible task definition.
    fn update_scheduled_task(
        &self,
        task: ScheduledTask,
        expected_updated_at: i64,
    ) -> Result<ScheduledTaskUpdateResolution, RepositoryError>;

    /// Marks one visible task definition deleted without removing its history.
    fn soft_delete_scheduled_task(
        &self,
        task_id: &ScheduledTaskId,
        deleted_at: i64,
    ) -> Result<bool, RepositoryError>;
}

/// Distinguishes a committed replacement from a missing or concurrently changed definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduledTaskUpdateResolution {
    Updated(Box<ScheduledTask>),
    NotFound,
    Conflict,
}

/// Supplies stable identifiers for newly created scheduled-task definitions.
pub trait ScheduledTaskIdGenerator {
    /// Produces one new scheduled-task identifier.
    fn generate_scheduled_task_id(&self) -> ScheduledTaskId;
}

/// Verifies that a local Scheduled Task target can use the isolated-worktree execution mode.
pub trait ScheduledTaskTargetValidator {
    /// Checks the configured project directory without creating a branch or worktree.
    fn validate_git_worktree_target(
        &self,
        project_root: &Path,
    ) -> Result<(), TaskWorktreeProvisionerError>;

    /// Resolves the project's current HEAD branch, or `None` when HEAD is detached. Callers use this
    /// as a fallback when the persisted `default_branch` is empty.
    fn resolve_default_branch(
        &self,
        project_root: &Path,
    ) -> Result<Option<String>, TaskWorktreeProvisionerError>;
}

/// Supplies the fact a scheduler wants to persist for one planned occurrence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduledOccurrenceCandidate {
    Run(Box<ScheduledRun>),
    Missed(ScheduledMissedOccurrence),
}

impl ScheduledOccurrenceCandidate {
    /// Stores a scheduled run behind one pointer so the short missed fact stays compact.
    pub fn run(run: ScheduledRun) -> Self {
        Self::Run(Box::new(run))
    }

    /// Returns the stable planned identity shared by run and missed facts.
    pub fn occurrence(&self) -> &ScheduledOccurrence {
        match self {
            Self::Run(run) => &run.occurrence,
            Self::Missed(missed) => &missed.occurrence,
        }
    }
}

/// Distinguishes newly persisted occurrence facts from idempotent replays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduledOccurrenceResolution {
    NotDue,
    Admitted(ScheduledRun),
    ExistingRun(ScheduledRun),
    Missed(ScheduledMissedOccurrence),
    ExistingMissed(ScheduledMissedOccurrence),
}

/// Defines the atomic persistence boundary shared by admitted and missed occurrences.
pub trait ScheduledOccurrenceRepository {
    /// Inserts one outcome at most once and returns the existing outcome on replay.
    fn resolve_scheduled_occurrence(
        &self,
        candidate: ScheduledOccurrenceCandidate,
    ) -> Result<ScheduledOccurrenceResolution, RepositoryError>;
}

/// Defines newest-first history reads independently from occurrence admission writes.
pub trait ScheduledTaskHistoryRepository {
    fn list_scheduled_task_history(
        &self,
        task_id: &ScheduledTaskId,
    ) -> Result<Vec<ScheduledTaskHistoryEntry>, RepositoryError>;
}

/// Distinguishes a committed lifecycle write from missing or stale execution callbacks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduledRunTransitionResolution {
    Updated(ScheduledRun),
    NotFound,
    InvalidState(ScheduledRun),
}

/// Persists forward-only Scheduled Run lifecycle changes atomically.
pub trait ScheduledRunLifecycleRepository {
    /// Marks active runs from an earlier Desktop process as failed during startup recovery.
    fn fail_interrupted_scheduled_runs(
        &self,
        interrupted_at: i64,
    ) -> Result<usize, RepositoryError>;

    fn transition_scheduled_run(
        &self,
        run_id: &ScheduledRunId,
        transition: ScheduledRunTransition,
        transitioned_at: i64,
    ) -> Result<ScheduledRunTransitionResolution, RepositoryError>;

    /// Lists finished runs that still reference a task-owned worktree, ordered oldest
    /// terminal transition first, so retention can delete the oldest kept worktree.
    fn list_scheduled_runs_with_retained_worktrees(
        &self,
    ) -> Result<Vec<ScheduledRun>, RepositoryError>;

    /// Clears one run's retained-worktree link after its worktree task was discarded, so
    /// retention sweeps and history pruning stop tracking the discarded worktree.
    fn clear_retained_worktree_link(&self, run_id: &ScheduledRunId) -> Result<(), RepositoryError>;

    /// Persists one independent business verdict and its detail onto a run row, leaving the
    /// execution-layer status and failure reason untouched. Returns `false` when the run no
    /// longer exists or has not reached `succeeded`, the only annotatable state.
    fn annotate_scheduled_run_verdict(
        &self,
        run_id: &ScheduledRunId,
        verdict: ScheduledRunVerdict,
        detail: Option<&str>,
        annotated_at: i64,
    ) -> Result<bool, RepositoryError>;
}

/// Supplies stable identifiers for newly admitted Scheduled Runs.
pub trait ScheduledRunIdGenerator {
    /// Produces one new Scheduled Run identifier.
    fn generate_scheduled_run_id(&self) -> ScheduledRunId;
}

/// One run that still owns artifact rows, resolved against its definition for sweep decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledArtifactRunRecord {
    pub run_id: ScheduledRunId,
    /// `None` when the run row was already pruned by history retention.
    pub task_id: Option<ScheduledTaskId>,
    /// The run's last transition time, used as the oldest-first artifact retention order.
    pub updated_at: Option<i64>,
    /// Whether the run definition is gone even though its artifact rows remain.
    pub run_missing: bool,
    /// Whether the owning task was soft-deleted or removed entirely.
    pub task_deleted: bool,
    /// Whether the run still keeps its worktree, which exempts it from the per-task bound.
    pub retains_worktree: bool,
}

/// Persists a run's collected artifact manifest and rejected-candidate log.
pub trait ScheduledRunArtifactRepository {
    /// Records collected artifacts and rejections for one run in a single transaction.
    fn record_scheduled_run_artifacts(
        &self,
        run_id: &ScheduledRunId,
        artifacts: &[ScheduledRunArtifact],
        rejections: &[ScheduledArtifactRejection],
        recorded_at: i64,
    ) -> Result<(), RepositoryError>;

    /// Lists one run's collected artifacts in declaration order.
    fn list_scheduled_run_artifacts(
        &self,
        run_id: &ScheduledRunId,
    ) -> Result<Vec<ScheduledRunArtifact>, RepositoryError>;

    /// Lists one run's rejected candidates in declaration order.
    fn list_scheduled_run_artifact_rejections(
        &self,
        run_id: &ScheduledRunId,
    ) -> Result<Vec<ScheduledArtifactRejection>, RepositoryError>;

    /// Loads one registered artifact by its run-relative path.
    fn find_scheduled_run_artifact(
        &self,
        run_id: &ScheduledRunId,
        relative_path: &str,
    ) -> Result<Option<ScheduledRunArtifact>, RepositoryError>;

    /// Lists every run that still owns artifact rows, with the retention metadata the sweep needs.
    fn list_scheduled_artifact_runs(
        &self,
    ) -> Result<Vec<ScheduledArtifactRunRecord>, RepositoryError>;

    /// Loads one run's artifact subject, whether or not it still owns manifest rows.
    ///
    /// Returns `None` only when neither the run nor any artifact row exists. A pruned run
    /// with orphan rows reports `run_missing`, and a soft-deleted task reports `task_deleted`,
    /// so callers can reject access without leaking the fact that artifacts survived.
    fn find_scheduled_artifact_run(
        &self,
        run_id: &ScheduledRunId,
    ) -> Result<Option<ScheduledArtifactRunRecord>, RepositoryError>;

    /// Deletes one registered artifact row, returning whether it existed.
    fn delete_scheduled_run_artifact(
        &self,
        run_id: &ScheduledRunId,
        relative_path: &str,
    ) -> Result<bool, RepositoryError>;

    /// Deletes every artifact and rejection row for one run.
    fn delete_scheduled_run_artifact_rows(
        &self,
        run_id: &ScheduledRunId,
    ) -> Result<usize, RepositoryError>;
}
