//! Uploading the artifacts one scheduled run declared: its command, its terminal event and the
//! memory-only grant exchange it reuses from the Revision delivery.
//!
//! The execution exists because the artifacts must leave the one-shot Workspace before it is
//! released, while Cloud never scans it: the Node reads the declaration out of the ended session's
//! stored terminal result, uploads each declared file to the key Cloud grants, and reports what it
//! measured. Grants are not part of the input - they expire, while the same execution must be
//! resendable with identical input.

use super::validation::{validate_execution_ids, validate_protocol_version};
use crate::{
    DeliverScheduledArtifactsSpec, ExecutionId, MessageValidationError, OperationId,
    ProtocolVersion, ScheduledArtifactsExecutionResult, Sequence, ValidateMessage,
};
use serde::{Deserialize, Serialize};

/// Uploads the artifacts of one ended scheduled session.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliverScheduledArtifacts {
    pub spec: DeliverScheduledArtifactsSpec,
}

/// Correlates the upload with its IssueRun and durable execution identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeliverScheduledArtifactsMessage {
    pub protocol_version: ProtocolVersion,
    pub operation_id: OperationId,
    pub execution_id: ExecutionId,
    pub payload: DeliverScheduledArtifacts,
}

/// The terminal event of an artifact upload; no event of the execution follows it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScheduledArtifactsResultMessage {
    pub protocol_version: ProtocolVersion,
    pub operation_id: OperationId,
    pub execution_id: ExecutionId,
    pub sequence: Sequence,
    pub payload: ScheduledArtifactsExecutionResult,
}

impl ValidateMessage for DeliverScheduledArtifactsMessage {
    /// Requires the identity of the session whose declaration this upload carries.
    fn validate(&self) -> Result<(), MessageValidationError> {
        validate_protocol_version(self.protocol_version)?;
        validate_execution_ids(&self.operation_id, &self.execution_id)?;
        self.payload.spec.validate()
    }
}

impl ValidateMessage for ScheduledArtifactsResultMessage {
    /// Bounds the terminal facts: every registered object needs a valid key and canonical digest, and
    /// a failure names one of the reasons Cloud recognises.
    fn validate(&self) -> Result<(), MessageValidationError> {
        validate_protocol_version(self.protocol_version)?;
        validate_execution_ids(&self.operation_id, &self.execution_id)?;
        self.payload.validate()
    }
}
