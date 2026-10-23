//! Scheduled-artifact upload responsibility: the immutable input, the declaration it reads, and the
//! one terminal event retained until the Controller acknowledges it.
//!
//! The upload lives in its own table rather than in `revision_deliveries`: it shares neither a
//! frozen plan nor Git work with a Revision delivery, and a restart fails it as `interrupted`
//! instead of resuming it, because Cloud gives a retried upload a new execution. Upload grants never
//! reach this module.

use crate::*;
use ora_node_protocol::*;
use rusqlite::{OptionalExtension, params};

/// One accepted upload with its immutable input and retained terminal state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduledArtifactsDelivery {
    pub command: DeliverScheduledArtifactsMessage,
    pub progress: ScheduledArtifactsProgress,
}

/// A running upload never freezes anything; only the terminal event exists besides the input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScheduledArtifactsProgress {
    Running,
    Completed(Box<ScheduledArtifactsEvidence>),
}

impl ScheduledArtifactsProgress {
    /// Preparation and upload are both `Running`; the Controller waits on the terminal event.
    pub fn state(&self) -> ExecutionState {
        match self {
            Self::Running => ExecutionState::Running,
            Self::Completed(evidence) => {
                ExecutionState::Completed(ExecutionResult::ScheduledArtifacts(evidence.clone()))
            }
        }
    }
}

/// The artifacts one ended scheduled session declared, as its stored terminal result recorded them.
///
/// The session input is kept separate from the checkout: the caller resolves the checkout through
/// `cloned_checkout`, so "no declaration" and "declaration but no Workspace" stay distinguishable
/// terminal reasons.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduledArtifactDeclaration {
    /// The clone execution whose checkout held the declared files.
    pub checkout_execution_id: ExecutionId,
    pub manifest: ScheduledArtifactManifest,
}

impl<G: WriteGuard> NodeDatabase<G> {
    /// Reads the declaration of one ended scheduled session, by execution alone.
    ///
    /// A session of another execution family, a session that has not ended, an end that reported no
    /// declaration, and a scheduled run without the reserved directory all read as `None`: the
    /// upload reports `declaration_unavailable` for every one of them.
    pub fn scheduled_artifact_declaration(
        &self,
        session: &ExecutionId,
    ) -> Result<Option<ScheduledArtifactDeclaration>, Error> {
        let Some(record) = self.delivery_session(session)? else {
            return Ok(None);
        };
        let ExecutionState::Completed(ExecutionResult::AgentSession(result)) = &record.state else {
            return Ok(None);
        };
        let AgentSessionResult::AgentSessionEnded(ended) = result;
        let Some(manifest) = &ended.artifact_manifest else {
            return Ok(None);
        };
        Ok(Some(ScheduledArtifactDeclaration {
            checkout_execution_id: record.command.payload.spec.checkout_execution_id,
            manifest: manifest.clone(),
        }))
    }

    /// Reads by both identity keys so another execution family cannot adopt the identity.
    pub fn find_scheduled_artifacts(
        &self,
        operation: &OperationId,
        execution: &ExecutionId,
    ) -> Result<Option<ScheduledArtifactsDelivery>, Error> {
        if self
            .identity_kind(operation, execution)?
            .is_some_and(|kind| kind != "deliver_scheduled_artifacts")
        {
            return Err(Error::IdentityConflict);
        }
        read(&self.connection, execution)
    }

    /// Admits a bare input only in a home that has never enabled runtime control.
    pub fn accept_scheduled_artifacts(
        &mut self,
        command: &DeliverScheduledArtifactsMessage,
    ) -> Result<ScheduledArtifactsDelivery, Error> {
        let fenced: bool = self.connection.query_row("SELECT EXISTS(SELECT 1 FROM runtime_enforcement) OR EXISTS(SELECT 1 FROM runtime_binding)", [], |r| r.get(/*idx*/ 0))?;
        if fenced {
            return Err(Error::InvalidTransition);
        }
        self.accept_scheduled_artifacts_input(command, /*permit*/ None)
    }

    /// Stores Cloud's exact execution responsibility together with the immutable input.
    pub fn accept_controlled_scheduled_artifacts(
        &mut self,
        envelope: &ControlledDeliverScheduledArtifacts,
    ) -> Result<ScheduledArtifactsDelivery, Error> {
        envelope.validate()?;
        self.validate_runtime_permit(&envelope.binding)?;
        self.accept_scheduled_artifacts_input(&envelope.command, Some(&envelope.binding))
    }

    /// Deduplicates the full input, then records the started upload and its control atomically.
    ///
    /// The permit was just checked live, so the upload is recorded as started: a later restart
    /// settles it as interrupted instead of re-qualifying it.
    fn accept_scheduled_artifacts_input(
        &mut self,
        command: &DeliverScheduledArtifactsMessage,
        permit: Option<&RuntimeBinding>,
    ) -> Result<ScheduledArtifactsDelivery, Error> {
        command.validate()?;
        if let Some(record) =
            self.find_scheduled_artifacts(&command.operation_id, &command.execution_id)?
        {
            return if record.command == *command {
                Ok(record)
            } else {
                Err(Error::IdentityConflict)
            };
        }
        if permit.is_some() && !self.unfinished_runtime_executions()?.is_empty() {
            return Err(Error::ResourceConflict);
        }
        self.guard.before_write(WritePoint::Accept)?;
        let tx = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO execution_identities VALUES(?1,?2,'deliver_scheduled_artifacts')",
            params![command.operation_id.as_str(), command.execution_id.as_str()],
        )?;
        tx.execute(
            "INSERT INTO scheduled_artifact_deliveries(execution,input,state) VALUES(?1,?2,'running')",
            params![
                command.execution_id.as_str(),
                serde_json::to_string(command)?
            ],
        )?;
        if let Some(permit) = permit {
            tx.execute(
                "INSERT INTO execution_control(execution,permit,started) VALUES(?1,?2,1)",
                params![
                    command.execution_id.as_str(),
                    serde_json::to_string(permit)?
                ],
            )?;
        }
        tx.commit()?;
        Ok(ScheduledArtifactsDelivery {
            command: command.clone(),
            progress: ScheduledArtifactsProgress::Running,
        })
    }

    /// Commits the one terminal result and its replay envelope together.
    ///
    /// `node` is the incarnation that wrote the result; a restarted Node settling an interrupted
    /// attempt reports itself, since no artifact object identifies the original writer.
    pub fn complete_scheduled_artifacts(
        &mut self,
        execution: &ExecutionId,
        node: &NodeRuntimeIdentity,
        result: ScheduledArtifactsExecutionResult,
    ) -> Result<(), Error> {
        let record = read(&self.connection, execution)?.ok_or(Error::IdentityConflict)?;
        if matches!(record.progress, ScheduledArtifactsProgress::Completed(_)) {
            return Err(Error::InvalidTransition);
        }
        if node.node_id != self.node_id {
            return Err(Error::NodeMismatch);
        }
        let evidence = ScheduledArtifactsEvidence {
            node: node.clone(),
            result: result.clone(),
        };
        let message = event(&record.command, result);
        message.validate()?;
        self.guard.before_write(WritePoint::Complete)?;
        let tx = self.connection.transaction()?;
        tx.execute(
            "UPDATE scheduled_artifact_deliveries SET state='completed',result=?1 WHERE execution=?2",
            params![
                serde_json::to_string(&evidence)?,
                execution.as_str()
            ],
        )?;
        self.guard.before_write(WritePoint::Outbox)?;
        tx.execute(
            "INSERT INTO revision_outbox VALUES(?1,?2)",
            params![
                execution.as_str(),
                serde_json::to_string(&NodeToControllerMessage::ScheduledArtifactsResult(message))?
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Lists unfinished uploads in admission order; startup settles each as interrupted.
    pub fn recoverable_scheduled_artifacts(
        &self,
    ) -> Result<Vec<ScheduledArtifactsDelivery>, Error> {
        self.connection
            .prepare("SELECT input,state,result FROM scheduled_artifact_deliveries WHERE state<>'completed' ORDER BY rowid")?
            .query_map([], row)?
            .map(|r| decode(r?))
            .collect()
    }

    /// Fails every upload still running when this process takes over as interrupted.
    ///
    /// The previous attempt may already have uploaded objects, but nothing can prove which: Cloud
    /// retries with a new execution and settles the run's delivery deadline by itself.
    pub fn interrupt_scheduled_artifacts(
        &mut self,
        node: &NodeRuntimeIdentity,
    ) -> Result<usize, Error> {
        let running = self.recoverable_scheduled_artifacts()?;
        let count = running.len();
        for record in running {
            self.complete_scheduled_artifacts(
                &record.command.execution_id,
                node,
                ScheduledArtifactsExecutionResult::Failed(ScheduledArtifactsFailed {
                    reason: ScheduledArtifactsFailureReason::Interrupted,
                }),
            )?;
        }
        Ok(count)
    }

    /// Only the exact terminal sequence of a completed upload releases its event.
    pub(crate) fn acknowledge_scheduled_artifacts(
        &mut self,
        ack: &EventAckMessage,
    ) -> Result<(), Error> {
        let record = self
            .find_scheduled_artifacts(&ack.operation_id, &ack.execution_id)?
            .ok_or(Error::InvalidAck)?;
        if ack.sequence != Sequence::new(/*value*/ 1)
            || !matches!(record.progress, ScheduledArtifactsProgress::Completed(_))
        {
            return Err(Error::InvalidAck);
        }
        self.guard.before_write(WritePoint::Acknowledge)?;
        self.connection.execute(
            "DELETE FROM revision_outbox WHERE execution=?1",
            [ack.execution_id.as_str()],
        )?;
        Ok(())
    }
}

/// Builds the single terminal envelope; replay reuses the stored copy and never rebuilds it.
fn event(
    command: &DeliverScheduledArtifactsMessage,
    payload: ScheduledArtifactsExecutionResult,
) -> ScheduledArtifactsResultMessage {
    ScheduledArtifactsResultMessage {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        operation_id: command.operation_id.clone(),
        execution_id: command.execution_id.clone(),
        sequence: Sequence::new(/*value*/ 1),
        payload,
    }
}

/// Loads one upload while the caller owns the connection.
fn read(
    connection: &rusqlite::Connection,
    execution: &ExecutionId,
) -> Result<Option<ScheduledArtifactsDelivery>, Error> {
    connection
        .query_row(
            "SELECT input,state,result FROM scheduled_artifact_deliveries WHERE execution=?1",
            [execution.as_str()],
            row,
        )
        .optional()?
        .map(decode)
        .transpose()
}

type DeliveryRow = (String, String, Option<String>);

/// Shares SQL row extraction between lookups and recovery scans.
fn row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeliveryRow> {
    Ok((
        row.get(/*idx*/ 0)?,
        row.get(/*idx*/ 1)?,
        row.get(/*idx*/ 2)?,
    ))
}

/// Refuses malformed rows rather than guessing whether a result was written.
fn decode((input, state, result): DeliveryRow) -> Result<ScheduledArtifactsDelivery, Error> {
    let progress = match (state.as_str(), result) {
        ("running", None) => ScheduledArtifactsProgress::Running,
        ("completed", Some(result)) => {
            ScheduledArtifactsProgress::Completed(Box::new(serde_json::from_str(&result)?))
        }
        _ => return Err(Error::InvalidSchema),
    };
    Ok(ScheduledArtifactsDelivery {
        command: serde_json::from_str(&input)?,
        progress,
    })
}
