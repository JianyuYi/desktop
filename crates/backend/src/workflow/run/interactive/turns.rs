//! Workflow-owned admission and cleanup around a session's human prompt stream.

use super::CompletingNodeRuns;
use super::session::{HumanTurnAdmission, restore_human_turn};
use crate::agent_runtime::{AgentRuntimeManager, SessionEventStream};
use crate::error::BackendError;
use crate::git_cleanup::KeyedResourceLocks;
use crate::workflow::run::transitions::WorkflowRunTransitions;
use ora_contracts::{PromptSessionEvent, PromptSessionRequest};
use ora_db::RepositoryPool;
use ora_domain::PromptInactivityPolicy;
use std::path::PathBuf;
use std::sync::Arc;

/// Grants only human-turn coordination to Sessions, not workflow scheduling or completion control.
pub(crate) struct WorkflowSessionTurns {
    pool: RepositoryPool,
    run_locks: Arc<KeyedResourceLocks>,
    completing_node_runs: Arc<CompletingNodeRuns>,
    transitions: Arc<WorkflowRunTransitions>,
    sessions_root: PathBuf,
}

impl WorkflowSessionTurns {
    /// Shares the completion and callback gates owned by the run module.
    ///
    /// `sessions_root` lets the drop cleanup derive the settled turn's snapshot from the same
    /// durable history the completion path reads.
    pub(in crate::workflow::run) fn new(
        pool: RepositoryPool,
        run_locks: Arc<KeyedResourceLocks>,
        completing_node_runs: Arc<CompletingNodeRuns>,
        transitions: Arc<WorkflowRunTransitions>,
        sessions_root: PathBuf,
    ) -> Self {
        Self {
            pool,
            run_locks,
            completing_node_runs,
            transitions,
            sessions_root,
        }
    }

    /// Streams one structured ACP prompt turn for a running session.
    ///
    /// When the session belongs to an awaiting interactive workflow node, the node flips to
    /// `Running` for the duration of the turn and back to `Pending` when the turn ends or the
    /// stream is dropped, so the node's awaiting status tracks the agent's generating state.
    pub(crate) async fn prompt(
        &self,
        agent_runtime: &AgentRuntimeManager,
        request: PromptSessionRequest,
    ) -> Result<SessionEventStream<PromptSessionEvent>, BackendError> {
        let admission = crate::workflow::run::interactive::begin_human_turn(
            &self.pool,
            &self.run_locks,
            &self.completing_node_runs,
            &self.transitions,
            &request.session_id,
        )
        .await?;
        let prompt_inactivity = match &admission {
            HumanTurnAdmission::Ordinary => PromptInactivityPolicy::Timeout,
            HumanTurnAdmission::Workflow {
                prompt_inactivity, ..
            } => *prompt_inactivity,
        };
        let stream = match agent_runtime
            .prompt_session_with_inactivity_policy(request, prompt_inactivity)
            .await
        {
            Ok(stream) => SessionEventStream::from(stream),
            Err(error) => {
                // The turn never started; put the awaiting node back where it was without
                // overwriting the snapshot of the turn that previously settled.
                if let HumanTurnAdmission::Workflow { node_run_id, .. } = &admission {
                    restore_human_turn(&self.transitions, node_run_id).await?;
                }
                return Err(error.into());
            }
        };
        let HumanTurnAdmission::Workflow { node_run_id, .. } = admission else {
            return Ok(stream);
        };
        let pool = self.pool.clone();
        let sessions_root = self.sessions_root.clone();
        let transitions = self.transitions.clone();
        Ok(stream.attach_cleanup(move || async move {
            crate::workflow::run::interactive::end_human_turn(
                &pool,
                &sessions_root,
                &transitions,
                &node_run_id,
            )
            .await
        }))
    }
}
