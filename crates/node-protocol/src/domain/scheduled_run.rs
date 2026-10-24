//! Business facts a Cloud scheduled run reports in addition to its execution result: the verdict
//! its agent stated in the final Assistant turn, and the artifacts it declared under the reserved
//! `.ora-scheduled-artifacts/` directory.
//!
//! Cloud admits a run from a scheduled occurrence, but it never parses the Thread, a message or a
//! tool field: the executor is the only writer of these values. So both are reported here as
//! structured, bounded wire values, and only for a scheduled session that ended normally. A manual
//! IssueRun and every failed end carry neither.
//!
//! The verdict vocabulary, the artifact directory name, the artifact types and the three per-run
//! artifact bounds are the ones the approved Cloud ADR froze and shared with local scheduled runs;
//! the crate keeps its own copies because it must stay free of Ora application-domain crates.

use crate::domain::revision::{ObjectUploadGrant, StoredObject};
use crate::{ExecutionId, MessageValidationError, NodeRuntimeIdentity, Sha256Digest};
use ora_utils::path::StrictRelativePath;
use serde::{Deserialize, Serialize};

/// The fixed worktree directory an agent declares its artifacts in.
///
/// Cloud never scans a Workspace: the executor collects from this one directory, and every
/// snapshot excludes it so artifacts never enter a delivery ref.
pub const SCHEDULED_ARTIFACT_DIRECTORY: &str = ".ora-scheduled-artifacts";

/// Largest free-text verdict detail Cloud stores, in Unicode characters.
pub const MAX_SCHEDULED_VERDICT_DETAIL_CHARS: usize = 2_000;

/// Largest single artifact, in bytes (10 MiB).
pub const MAX_SCHEDULED_ARTIFACT_BYTES: u64 = 10 * 1024 * 1024;

/// Most artifacts one run may declare.
pub const MAX_SCHEDULED_ARTIFACTS_PER_RUN: usize = 64;

/// Most refused candidates one manifest lists.
///
/// The bound exists because a refusal is a declaration too: a directory with tens of thousands of
/// unusable names would otherwise build a terminal result larger than the frame that carries it.
/// Refusals beyond this count are summarised by `omitted_rejections` instead.
pub const MAX_SCHEDULED_ARTIFACT_REJECTIONS: usize = 64;

/// Largest display name one rejection may carry, in UTF-8 bytes.
///
/// The collector already bounds an accepted path; a refused candidate only needs to stay
/// identifiable, so the same bound applies without requiring it to be a valid path.
pub const MAX_SCHEDULED_ARTIFACT_REJECTION_NAME_BYTES: usize = 1024;

/// Most bytes one run may declare across all artifacts (64 MiB).
pub const MAX_SCHEDULED_ARTIFACT_TOTAL_BYTES: u64 = 64 * 1024 * 1024;

/// What admitted a session execution.
///
/// An absent trigger on the wire is a manual IssueRun, which is the only other admission. Only a
/// scheduled occurrence may report a business verdict or artifacts.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionTrigger {
    /// Cloud admitted the run from a scheduled-task occurrence.
    Scheduled,
}

/// The frozen business verdict vocabulary shared with local scheduled runs.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduledVerdict {
    Pass,
    Warn,
    Fail,
    Blocked,
    Idle,
    RangeUnresolved,
}

/// The business result the executor parsed out of the run's final Assistant turn.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledRunResult {
    pub verdict: ScheduledVerdict,
    /// Free text the agent attached to its verdict, never a second machine-readable channel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict_detail: Option<String>,
}

impl ScheduledRunResult {
    /// Checks the value against the bounds the wire enforces on both send and receive.
    ///
    /// The executor parses the annotation out of Agent output, so it validates the parsed value
    /// here before it becomes part of a terminal result: a detail the wire refuses must degrade to
    /// "not reported" instead of failing the transaction that settles the run.
    pub fn validate(&self) -> Result<(), MessageValidationError> {
        match &self.verdict_detail {
            Some(detail)
                if detail.chars().count() > MAX_SCHEDULED_VERDICT_DETAIL_CHARS
                    || detail.contains('\0') =>
            {
                Err(MessageValidationError::InvalidScheduledResult)
            }
            Some(_) | None => Ok(()),
        }
    }
}

/// The closed set of artifact types an agent may declare.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduledArtifactKind {
    Markdown,
    Json,
    Text,
    Log,
    Xml,
    Html,
}

/// One artifact the run declares and will upload as its own object.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledArtifactItem {
    /// Zero-based position that identifies the artifact in the upload grant Cloud issues; it is
    /// never part of an object key.
    pub index: u32,
    /// Forward-slash path relative to [`SCHEDULED_ARTIFACT_DIRECTORY`], for display only.
    pub display_name: String,
    pub kind: ScheduledArtifactKind,
    pub size_bytes: u64,
    /// Lowercase hexadecimal SHA-256 the executor measured while collecting the file.
    pub sha256: String,
}

/// Why one candidate was refused instead of declared; the rejection stays visible to the reader.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduledArtifactRejectionReason {
    /// The file name's extension is outside the closed whitelist.
    UnsupportedType,
    /// The relative path is unsafe: traversal, absolute, drive, alternate-data-stream or control
    /// characters, or the same path in another case.
    PathUnsafe,
    /// The candidate is a symbolic link rather than a regular file.
    Symlink,
    /// The candidate is a hard link with more than one name, so its inode may be reachable
    /// outside the artifact directory.
    HardLink,
    /// The file exceeds the single-artifact size bound.
    TooLarge,
    /// The run already declared the per-run artifact count bound.
    TooMany,
    /// The run already declared the per-run total byte bound.
    TotalTooLarge,
    /// The file could not be read while collecting or hashing it.
    ReadFailed,
    /// The walk stopped at the executor's candidate bound before examining every entry, so names
    /// below the named directory may be missing from this declaration. The manifest states the same
    /// fact in `walk_truncated`, which stays visible even when this list has no room for the detail.
    CandidateLimit,
}

/// One refused candidate and the reason the reader sees.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledArtifactRejection {
    /// The candidate's relative path, or the artifact directory itself when it was refused whole.
    pub display_name: String,
    pub reason: ScheduledArtifactRejectionReason,
}

/// Everything a scheduled run declared about its artifacts, in one message.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledArtifactManifest {
    pub items: Vec<ScheduledArtifactItem>,
    pub rejections: Vec<ScheduledArtifactRejection>,
    /// Refused candidates whose detail is not listed because [`MAX_SCHEDULED_ARTIFACT_REJECTIONS`]
    /// was already reached. The count keeps the omission visible instead of hiding refusals behind
    /// a silent cut.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub omitted_rejections: u32,
    /// Whether the walk stopped at its candidate bound before reading every entry, so candidates the
    /// executor never examined may exist.
    ///
    /// This is separate from `omitted_rejections`, which counts refused candidates the walk did
    /// enumerate: here no count is known, and the refusal list may already be full, so a reader
    /// could neither see the cut nor tell it from a complete list of refusals without this fact.
    #[serde(default, skip_serializing_if = "is_false")]
    pub walk_truncated: bool,
}

impl ScheduledArtifactManifest {
    /// Bounds the manifest both for its own readers and against a Node that mis-declared what it
    /// collected, using the same rules the collector applies while walking the directory.
    pub fn validate(&self) -> Result<(), MessageValidationError> {
        let invalid = || MessageValidationError::InvalidScheduledArtifactManifest;
        if self.items.len() > MAX_SCHEDULED_ARTIFACTS_PER_RUN
            || self.rejections.len() > MAX_SCHEDULED_ARTIFACT_REJECTIONS
        {
            return Err(invalid());
        }
        let mut indexes = std::collections::HashSet::new();
        let mut names = std::collections::HashSet::new();
        let mut total = 0u64;
        for item in &self.items {
            if !indexes.insert(item.index) || item.index as usize >= MAX_SCHEDULED_ARTIFACTS_PER_RUN
            {
                return Err(invalid());
            }
            // The collector folds names the same way, so two spellings one filesystem would treat
            // as one path cannot both be declared.
            let Some(path) = declared_artifact_path(&item.display_name) else {
                return Err(invalid());
            };
            if !names.insert(path.fold_case_key()) {
                return Err(invalid());
            }
            if item.size_bytes > MAX_SCHEDULED_ARTIFACT_BYTES || !is_lower_hex_sha256(&item.sha256)
            {
                return Err(invalid());
            }
            total = total.saturating_add(item.size_bytes);
        }
        if total > MAX_SCHEDULED_ARTIFACT_TOTAL_BYTES {
            return Err(invalid());
        }
        for rejection in &self.rejections {
            if rejection.display_name.is_empty()
                || rejection.display_name.len() > MAX_SCHEDULED_ARTIFACT_REJECTION_NAME_BYTES
                || rejection.display_name.contains('\0')
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

/// Omits the refusal summary from the wire while no refusal was dropped.
// `skip_serializing_if` gives the predicate a reference; taking the `u32` by value would not fit it.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(value: &u32) -> bool {
    *value == 0
}

/// Omits the walk fact from the wire while the walk read every entry it may examine.
// `skip_serializing_if` gives the predicate a reference; taking the `bool` by value would not fit it.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(value: &bool) -> bool {
    !*value
}

/// Parses a declared artifact path into the identity rules the collector applies while walking.
///
/// A Windows alternate data stream is refused separately because `:` is legal on Unix but would
/// name another file stream on Windows.
fn declared_artifact_path(value: &str) -> Option<StrictRelativePath> {
    if value.contains(':') {
        return None;
    }
    StrictRelativePath::parse(value).ok()
}

/// Accepts the digest spelling Cloud verifies uploads against.
fn is_lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// Cloud's frozen input for uploading the artifacts of one ended scheduled session.
///
/// The declaration itself is not repeated here: the Node reads it from the durable terminal result of
/// the session it names, so a resent execution cannot smuggle a different set of files past what the
/// run actually reported.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliverScheduledArtifactsSpec {
    /// The ended session execution whose declaration Cloud persisted.
    pub session_execution_id: ExecutionId,
}

impl DeliverScheduledArtifactsSpec {
    /// Requires the session identity this upload is about.
    pub(crate) fn validate(&self) -> Result<(), MessageValidationError> {
        if self.session_execution_id.is_empty() {
            return Err(MessageValidationError::EmptyField {
                field: "session_execution_id",
            });
        }
        Ok(())
    }
}

/// One declared artifact the Node still has to store, addressed the way Cloud grants it.
///
/// A scheduled artifact has no object key before Cloud issues the grant: Cloud owns every key, and
/// the declaration index is what identifies the artifact to Cloud. The request therefore names the
/// declaration entry together with the size and digest Cloud binds into the PUT signature; the Node
/// uploads only to the key the grant names.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledArtifactUpload {
    pub index: u32,
    pub size_bytes: u64,
    pub sha256: Sha256Digest,
}

impl ScheduledArtifactUpload {
    /// Requires a declaration index, size and canonical digest Cloud could have persisted.
    pub(crate) fn validate(&self) -> Result<(), MessageValidationError> {
        if self.index as usize >= MAX_SCHEDULED_ARTIFACTS_PER_RUN
            || self.size_bytes > MAX_SCHEDULED_ARTIFACT_BYTES
        {
            return Err(MessageValidationError::InvalidScheduledArtifactManifest);
        }
        self.sha256.validate()
    }
}

/// The upload grant Cloud issued for one declared artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledArtifactUploadGrant {
    /// The declaration entry Cloud signed the grant for.
    pub index: u32,
    pub grant: ObjectUploadGrant,
}

impl ScheduledArtifactUploadGrant {
    /// Requires a declaration index the manifest bound and a usable grant.
    pub(crate) fn validate(&self) -> Result<(), MessageValidationError> {
        if self.index as usize >= MAX_SCHEDULED_ARTIFACTS_PER_RUN {
            return Err(MessageValidationError::InvalidScheduledArtifactManifest);
        }
        self.grant.validate()
    }
}

/// The artifacts the Node uploaded, as it measured them; Cloud verifies each before registering it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledArtifactsDelivered {
    pub artifacts: Vec<StoredObject>,
}

/// Why an upload ended without leaving a registrable object.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduledArtifactsFailureReason {
    /// The checkout, or the reserved directory inside it, could not be read.
    WorkspaceUnavailable,
    /// An upload or its grant failed after the retries ran out.
    UploadFailed,
    /// The Node holds no declaration for the session, for example after a restart.
    DeclarationUnavailable,
    /// The execution was terminated before it reached its own verdict.
    Interrupted,
}

/// The terminal failure of an artifact upload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledArtifactsFailed {
    pub reason: ScheduledArtifactsFailureReason,
}

/// The only terminal result of an artifact upload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
pub enum ScheduledArtifactsExecutionResult {
    Delivered(ScheduledArtifactsDelivered),
    Failed(ScheduledArtifactsFailed),
}

impl ScheduledArtifactsExecutionResult {
    /// Bounds the terminal facts: a delivered result carries at most one object per declared artifact,
    /// each with a valid key and canonical digest.
    pub(crate) fn validate(&self) -> Result<(), MessageValidationError> {
        match self {
            Self::Delivered(delivered) => {
                if delivered.artifacts.len() > MAX_SCHEDULED_ARTIFACTS_PER_RUN {
                    return Err(MessageValidationError::InvalidScheduledArtifactManifest);
                }
                delivered
                    .artifacts
                    .iter()
                    .try_for_each(StoredObject::validate)
            }
            Self::Failed(_) => Ok(()),
        }
    }
}

/// Stored terminal evidence of one artifact upload, reported only by status queries.
///
/// The sequenced `ScheduledArtifactsResultMessage` remains the only settlement path, exactly as for
/// a Revision delivery: a query cannot acknowledge anything, so this value exists only to answer
/// what happened to an execution after a reconnect or restart. It repeats the origin because a
/// restarted Node reports retained evidence without becoming its author.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledArtifactsEvidence {
    pub node: NodeRuntimeIdentity,
    pub result: ScheduledArtifactsExecutionResult,
}

impl ScheduledArtifactsEvidence {
    /// Keeps the origin identifiable and the retained terminal facts bounded.
    pub(crate) fn validate(&self) -> Result<(), MessageValidationError> {
        self.node
            .validate()
            .map_err(|field| MessageValidationError::EmptyField { field })?;
        self.result.validate()
    }
}
