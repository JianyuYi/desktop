//! Uploading the artifacts one scheduled run declared: its command, its terminal event and the
//! memory-only grant exchange it reuses from the Revision delivery.
//!
//! The execution exists because the artifacts must leave the one-shot Workspace before it is
//! released, while Cloud never scans it: the Node reads the declaration out of the ended session's
//! stored terminal result, uploads each declared file to the key Cloud grants, and reports what it
//! measured. Grants are not part of the input - they expire, while the same execution must be
//! resendable with identical input.

use super::validation::{validate_execution_ids, validate_identity, validate_protocol_version};
use crate::{
    DeliverScheduledArtifactsSpec, ExecutionId, MessageValidationError, NodeId, OperationId,
    ProtocolVersion, ScheduledArtifactUpload, ScheduledArtifactUploadGrant,
    ScheduledArtifactsExecutionResult, Sequence, ValidateMessage,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Uploads the artifacts of one ended scheduled session.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliverScheduledArtifacts {
    pub spec: DeliverScheduledArtifactsSpec,
}

/// The Node has declared artifacts Cloud has not granted yet; one grant per declaration index.
///
/// This cannot reuse `UploadGrantNeeded`: a scheduled artifact has no object key until Cloud picks
/// one, so the request must name the declaration entry instead of a key.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledArtifactGrantNeeded {
    pub node_id: NodeId,
    pub artifacts: Vec<ScheduledArtifactUpload>,
}

/// Grants Cloud signed for the artifacts a running upload asked about.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledArtifactGrants {
    pub grants: Vec<ScheduledArtifactUploadGrant>,
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

/// Asks the Controller for grants covering every declared artifact still to upload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScheduledArtifactGrantNeededMessage {
    pub protocol_version: ProtocolVersion,
    pub operation_id: OperationId,
    pub execution_id: ExecutionId,
    pub payload: ScheduledArtifactGrantNeeded,
}

/// Hands a running upload the grants Cloud signed for the artifacts it named.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScheduledArtifactGrantsMessage {
    pub protocol_version: ProtocolVersion,
    pub operation_id: OperationId,
    pub execution_id: ExecutionId,
    pub payload: ScheduledArtifactGrants,
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

impl ValidateMessage for ScheduledArtifactGrantNeededMessage {
    /// Requires the asking Node and at least one declaration entry, each uniquely and validly named;
    /// an upload only asks for artifacts it still has to store.
    fn validate(&self) -> Result<(), MessageValidationError> {
        validate_protocol_version(self.protocol_version)?;
        validate_execution_ids(&self.operation_id, &self.execution_id)?;
        validate_identity(self.payload.node_id.is_empty(), "node_id")?;
        let artifacts = &self.payload.artifacts;
        if artifacts.is_empty() || artifacts.len() > crate::MAX_SCHEDULED_ARTIFACTS_PER_RUN {
            return Err(MessageValidationError::InvalidScheduledArtifactManifest);
        }
        let mut indexes = HashSet::new();
        for artifact in artifacts {
            if !indexes.insert(artifact.index) {
                return Err(MessageValidationError::InvalidScheduledArtifactManifest);
            }
            artifact.validate()?;
        }
        Ok(())
    }
}

impl ValidateMessage for ScheduledArtifactGrantsMessage {
    /// Requires at least one grant, each for a distinct declaration entry with a usable key and URL.
    fn validate(&self) -> Result<(), MessageValidationError> {
        validate_protocol_version(self.protocol_version)?;
        validate_execution_ids(&self.operation_id, &self.execution_id)?;
        let grants = &self.payload.grants;
        if grants.is_empty() || grants.len() > crate::MAX_SCHEDULED_ARTIFACTS_PER_RUN {
            return Err(MessageValidationError::EmptyUploadGrant);
        }
        let mut indexes = HashSet::new();
        for grant in grants {
            if !indexes.insert(grant.index) {
                return Err(MessageValidationError::InvalidScheduledArtifactManifest);
            }
            grant.validate()?;
        }
        Ok(())
    }
}
