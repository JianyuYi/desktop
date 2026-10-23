//! Uploading the artifacts one scheduled run declared, before its one-shot Workspace is released.
//!
//! The declaration is not part of the execution input: Cloud persists the manifest it received in
//! the session's terminal result, and the Node reads it back from its own ledger. That is why this
//! executor has no frozen plan. Cloud owns every object key, so the executor asks for a grant per
//! declaration index and uploads to exactly the key the grant names; it never constructs a key.
//!
//! One failing artifact does not hide the others: every declared item is attempted, and the result
//! reports exactly what was stored, because Cloud registers this family item by item (see
//! [`upload`]).
use crate::ManagedNode;
use crate::revision::{GrantStore, HttpUploader, ObjectUploader, Registration, RetryPolicy};
use ora_node_protocol::*;
use ora_utils::hash::sha256_file;
use ora_utils::http::{ProxyConfig, ReqwestUploader, UploadOptions};
use ora_utils::path::StrictRelativePath;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Generous enough for 64 artifacts on a slow link; a stuck request still ends and is retried.
const UPLOAD_OPTIONS: UploadOptions = UploadOptions {
    connect_timeout: Duration::from_secs(/*secs*/ 30),
    total_timeout: Duration::from_secs(/*secs*/ 30 * 60),
};

/// Everything the worker resolved from the ledger for one upload.
#[derive(Clone, Debug)]
pub(super) struct Job {
    pub(crate) operation: OperationId,
    pub(crate) execution: ExecutionId,
    pub(crate) node_id: NodeId,
    pub(crate) checkout: PathBuf,
    pub(crate) manifest: ScheduledArtifactManifest,
}

/// What a step reported; an interrupted step still settles the execution, as Cloud retries it.
struct Finished {
    execution: ExecutionId,
    result: ScheduledArtifactsExecutionResult,
}

/// The worker's view of scheduled-artifact uploads: one outstanding step and its executor.
pub(super) struct ScheduledArtifacts {
    steps: Option<mpsc::Sender<Box<Job>>>,
    results: mpsc::Receiver<Finished>,
    stop: tokio::sync::watch::Sender<bool>,
    thread: Option<JoinHandle<()>>,
    in_flight: Option<ExecutionId>,
}

impl ScheduledArtifacts {
    /// Settles every upload the previous process left running, then starts the executor thread.
    ///
    /// A retried upload always arrives as a new execution, so a running row can never be resumed
    /// here; Cloud's delivery deadline decides what the run does with the interrupted attempt.
    pub(super) fn start(node: &mut ManagedNode, grants: GrantStore) -> Result<Self, crate::Error> {
        let identity = node.identity().clone();
        node.database.interrupt_scheduled_artifacts(&identity)?;
        let uploader =
            HttpUploader::new(ReqwestUploader::new(ProxyConfig::default()), UPLOAD_OPTIONS);
        Self::spawn(uploader, grants, RetryPolicy::DEFAULT)
            .map_err(|error| ora_node_db::Error::from(error).into())
    }

    /// Runs steps on a dedicated thread with its own runtime for uploads.
    fn spawn<U: ObjectUploader>(
        uploader: U,
        grants: GrantStore,
        policy: RetryPolicy,
    ) -> std::io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (steps, requests) = mpsc::channel::<Box<Job>>();
        let (reply, results) = mpsc::channel();
        let (stop, stopping) = tokio::sync::watch::channel(/*init*/ false);
        let thread = thread::Builder::new()
            .name("ora-node-scheduled-artifacts".into())
            .spawn(move || {
                for job in requests {
                    let mut stopping = stopping.clone();
                    let result = runtime.block_on(async {
                        tokio::select! {
                            biased;
                            _ = stopping.wait_for(|stop| *stop) => interrupted(),
                            result = upload(&uploader, &grants, &job, policy) => result,
                        }
                    });
                    if reply
                        .send(Finished {
                            execution: job.execution.clone(),
                            result,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            })?;
        Ok(Self {
            steps: Some(steps),
            results,
            stop,
            thread: Some(thread),
            in_flight: None,
        })
    }

    /// Persists finished steps, then hands the oldest runnable upload to an idle executor.
    pub(super) fn advance(&mut self, node: &mut ManagedNode) -> Result<(), crate::Error> {
        while let Ok(finished) = self.results.try_recv() {
            self.in_flight = None;
            self.settle(node, finished)?;
        }
        if self.thread.as_ref().is_some_and(JoinHandle::is_finished) {
            return Err(crate::Error::Configuration(
                "scheduled-artifact executor stopped unexpectedly".into(),
            ));
        }
        if self.in_flight.is_some() {
            return Ok(());
        }
        for record in node.database.recoverable_scheduled_artifacts()? {
            let job = match resolve(&node.database, &record.command) {
                Ok(job) => job,
                Err(reason) => {
                    settle_failure(node, &record.command.execution_id, reason)?;
                    continue;
                }
            };
            ora_logging::ora_info!(
                execution_id = %job.execution.as_str(),
                session_execution_id = %record.command.payload.spec.session_execution_id.as_str(),
                artifacts = job.manifest.items.len(),
                "Scheduled-artifact upload started"
            );
            self.in_flight = Some(job.execution.clone());
            self.steps
                .as_ref()
                .and_then(|steps| steps.send(Box::new(job)).ok())
                .ok_or_else(|| {
                    crate::Error::Configuration("scheduled-artifact executor is unavailable".into())
                })?;
            break;
        }
        Ok(())
    }

    /// Stops the executor and settles whatever finished before the stop.
    ///
    /// An upload the stop cancelled is reported as interrupted instead of left running: the process
    /// may never start again, and Cloud retries a new execution once the run's deadline passes.
    pub(super) fn finish(mut self, node: &mut ManagedNode) -> Result<(), crate::Error> {
        self.stop.send_replace(/*value*/ true);
        self.steps.take();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| {
                crate::Error::Shutdown("scheduled-artifact executor panicked".into())
            })?;
        }
        while let Ok(finished) = self.results.try_recv() {
            self.in_flight = None;
            self.settle(node, finished)?;
        }
        if let Some(execution) = self.in_flight.take() {
            settle_failure(
                node,
                &execution,
                ScheduledArtifactsFailureReason::Interrupted,
            )?;
        }
        Ok(())
    }

    /// Makes one step's outcome durable before anything else may follow from it.
    fn settle(&self, node: &mut ManagedNode, finished: Finished) -> Result<(), crate::Error> {
        let node_identity = node.identity().clone();
        Ok(node.database.complete_scheduled_artifacts(
            &finished.execution,
            &node_identity,
            finished.result,
        )?)
    }
}

impl Drop for ScheduledArtifacts {
    /// Error paths must not leave an uploader running after the database lease is released.
    fn drop(&mut self) {
        self.stop.send_replace(/*value*/ true);
        self.steps.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Resolves the ended session and its checkout before anything is read from the Workspace.
///
/// The session must carry a stored declaration and its clone checkout must still be reachable;
/// every other outcome is one of the protocol's closed failure reasons.
fn resolve<G: ora_node_db::WriteGuard>(
    database: &ora_node_db::NodeDatabase<G>,
    command: &DeliverScheduledArtifactsMessage,
) -> Result<Job, ScheduledArtifactsFailureReason> {
    let spec = &command.payload.spec;
    let declaration = database
        .scheduled_artifact_declaration(&spec.session_execution_id)
        .map_err(|_| ScheduledArtifactsFailureReason::DeclarationUnavailable)?
        .ok_or(ScheduledArtifactsFailureReason::DeclarationUnavailable)?;
    let checkout = database
        .cloned_checkout(&declaration.checkout_execution_id)
        .map_err(|_| ScheduledArtifactsFailureReason::WorkspaceUnavailable)?
        .ok_or(ScheduledArtifactsFailureReason::WorkspaceUnavailable)?;
    Ok(Job {
        operation: command.operation_id.clone(),
        execution: command.execution_id.clone(),
        node_id: database.node_id().clone(),
        checkout: checkout.path,
        manifest: declaration.manifest,
    })
}

/// Writes a definitive failure for an upload that never reached the executor.
fn settle_failure(
    node: &mut ManagedNode,
    execution: &ExecutionId,
    reason: ScheduledArtifactsFailureReason,
) -> Result<(), crate::Error> {
    ora_logging::ora_warn!(execution_id = %execution.as_str(), reason = ?reason, "Scheduled-artifact upload failed");
    let identity = node.identity().clone();
    let result = ScheduledArtifactsExecutionResult::Failed(ScheduledArtifactsFailed { reason });
    Ok(node
        .database
        .complete_scheduled_artifacts(execution, &identity, result)?)
}

/// Uploads every declared artifact, measuring each file it actually sends.
///
/// Grants are requested per declaration index because Cloud chooses every object key; the executor
/// never derives one. A failed item does not stop the remaining ones, so one bad file cannot hide
/// the rest of the run's output.
async fn upload<U: ObjectUploader>(
    uploader: &U,
    grants: &GrantStore,
    job: &Job,
    policy: RetryPolicy,
) -> ScheduledArtifactsExecutionResult {
    if job.manifest.items.is_empty() {
        return delivered(Vec::new());
    }
    let directory = job.checkout.join(SCHEDULED_ARTIFACT_DIRECTORY);
    if !readable_directory(&directory) {
        ora_logging::ora_warn!(execution_id = %job.execution.as_str(), "declared artifact directory is unreadable");
        return failed(ScheduledArtifactsFailureReason::WorkspaceUnavailable);
    }
    let registration = grants.register_artifacts(
        job.operation.clone(),
        job.execution.clone(),
        job.node_id.clone(),
        job.manifest
            .items
            .iter()
            .map(|item| {
                (
                    item.index,
                    ScheduledArtifactUpload {
                        index: item.index,
                        size_bytes: item.size_bytes,
                        sha256: Sha256Digest::new(item.sha256.clone()),
                    },
                )
            })
            .collect(),
    );
    let context = UploadContext {
        uploader,
        grants,
        registration: &registration,
        job,
        policy,
    };
    let mut stored = Vec::new();
    let mut any_failed = false;
    for item in &job.manifest.items {
        let Some(path) = artifact_path(&directory, item, &job.execution) else {
            any_failed = true;
            continue;
        };
        let (size, sha256) = match measure(&path) {
            Ok(measured) => measured,
            Err(error) => {
                ora_logging::ora_warn!(execution_id = %job.execution.as_str(), index = item.index, error = %error, "declared artifact could not be measured");
                any_failed = true;
                continue;
            }
        };
        // Cloud signed the declared size and digest into every grant, so bytes that no longer match
        // their declaration could only be refused by the object store after a wasted round trip.
        if size != item.size_bytes || sha256.as_str() != item.sha256.as_str() {
            ora_logging::ora_warn!(execution_id = %job.execution.as_str(), index = item.index, "declared artifact no longer matches its declaration");
            any_failed = true;
            continue;
        }
        let Some(grant) = wait_for_artifact_grant(grants, &registration, item.index, policy).await
        else {
            ora_logging::ora_warn!(execution_id = %job.execution.as_str(), index = item.index, "Cloud will not grant this upload any more");
            any_failed = true;
            break;
        };
        match put_artifact(&context, item.index, &grant, &path).await {
            PutOutcome::Stored => {
                registration.uploaded_artifact(item.index);
                stored.push(StoredObject {
                    key: grant.object_key.clone(),
                    size,
                    sha256,
                });
            }
            PutOutcome::Failed => any_failed = true,
            // Cloud withdrew the grants of this execution: no remaining artifact could be stored
            // either, so the ones already stored are still reported for per-item registration.
            PutOutcome::Refused => {
                ora_logging::ora_warn!(execution_id = %job.execution.as_str(), index = item.index, "Cloud will not grant this upload any more");
                any_failed = true;
                break;
            }
        }
    }
    if any_failed && stored.is_empty() {
        // Cloud settles this family per item: a delivered result registers exactly the objects it
        // names and records every remaining declared artifact as a visible rejection (the run
        // becomes `partial`), while a failure verdict registers nothing. So a partial upload is
        // reported as delivered with the objects that were stored, and only an upload that stored
        // nothing at all is failed.
        return failed(ScheduledArtifactsFailureReason::UploadFailed);
    }
    delivered(stored)
}

/// The absolute path of one declared artifact, refusing anything the manifest could not have named.
fn artifact_path(
    directory: &Path,
    item: &ScheduledArtifactItem,
    execution: &ExecutionId,
) -> Option<PathBuf> {
    match StrictRelativePath::parse(&item.display_name) {
        Ok(relative) => Some(relative.to_path(directory)),
        Err(error) => {
            ora_logging::ora_warn!(execution_id = %execution.as_str(), index = item.index, error = ?error, "declared artifact path is unusable");
            None
        }
    }
}

/// Whether the reserved directory is a real directory the executor may read.
///
/// A link or a plain file under that name is not a declaration to follow: following it could read
/// files from outside the Workspace.
fn readable_directory(directory: &Path) -> bool {
    fs::symlink_metadata(directory)
        .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
}

/// Measures one artifact as the executor will upload it.
///
/// The metadata is read again after hashing so a file that changed in between is refused instead of
/// being reported with a size that does not describe the digest.
fn measure(path: &Path) -> io::Result<(u64, Sha256Digest)> {
    let before = fs::symlink_metadata(path)?;
    if before.file_type().is_symlink() || !before.is_file() {
        return Err(io::Error::other("declared artifact is not a regular file"));
    }
    if before.len() > MAX_SCHEDULED_ARTIFACT_BYTES {
        return Err(io::Error::other("declared artifact exceeds the size bound"));
    }
    let sha256 = Sha256Digest::new(sha256_file(path)?);
    let after = fs::symlink_metadata(path)?;
    if !after.is_file() || after.len() != before.len() {
        return Err(io::Error::other(
            "declared artifact changed while measuring",
        ));
    }
    Ok((after.len(), sha256))
}

/// Waits for the grant Cloud issued for one declaration index, repeating the request while none
/// arrives. Waiting has no time limit while the Controller is merely disconnected, because a
/// reconnected one can still grant; only Cloud's refusal, which no later reply can undo, ends it.
async fn wait_for_artifact_grant(
    grants: &GrantStore,
    registration: &Registration,
    index: u32,
    policy: RetryPolicy,
) -> Option<ObjectUploadGrant> {
    let mut offers = grants.offers();
    let mut requested: Option<tokio::time::Instant> = None;
    loop {
        offers.borrow_and_update();
        if let Some(grant) = registration.valid_artifact(index, policy.expiry_margin) {
            return Some(grant);
        }
        if registration.refused() {
            return None;
        }
        if requested.is_none_or(|at| at.elapsed() >= policy.resend) {
            registration.request();
            requested = Some(tokio::time::Instant::now());
        }
        let _ = tokio::time::timeout(policy.resend, offers.changed()).await;
    }
}

/// Everything one artifact upload needs besides its index and bytes.
struct UploadContext<'a, U: ObjectUploader> {
    uploader: &'a U,
    grants: &'a GrantStore,
    registration: &'a Registration,
    job: &'a Job,
    policy: RetryPolicy,
}

/// How one artifact's upload ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PutOutcome {
    /// The object store accepted the bytes: a conditional create stored them, or they were already
    /// stored by an earlier attempt whose reply was lost.
    Stored,
    /// Every attempt for this artifact ran out.
    Failed,
    /// Cloud withdrew the upload's grants, so no remaining artifact can be stored either.
    Refused,
}

/// Sends one artifact with one grant; a refused grant ends the whole upload, a failed PUT does not.
async fn put_artifact<U: ObjectUploader>(
    context: &UploadContext<'_, U>,
    index: u32,
    grant: &ObjectUploadGrant,
    path: &Path,
) -> PutOutcome {
    let mut failures = 0;
    let mut current = grant.clone();
    while failures < context.policy.attempts {
        match context.uploader.put(&current, path).await {
            // A repeated PUT after Cloud re-issued the execution finds its own object and is told
            // not to overwrite it; that is success, not a conflict.
            Ok(200..=299 | 412) => return PutOutcome::Stored,
            Ok(403) => {
                // An expired or revoked signature: only a new grant can succeed.
                context.registration.discard_artifact(index);
                ora_logging::ora_warn!(execution_id = %context.job.execution.as_str(), index, "object store refused the upload grant");
                let Some(fresh) = wait_for_artifact_grant(
                    context.grants,
                    context.registration,
                    index,
                    context.policy,
                )
                .await
                else {
                    return PutOutcome::Refused;
                };
                current = fresh;
            }
            Ok(status) => {
                ora_logging::ora_warn!(execution_id = %context.job.execution.as_str(), index, status, "artifact upload failed");
                tokio::time::sleep(context.policy.backoff).await;
            }
            Err(error) => {
                ora_logging::ora_warn!(execution_id = %context.job.execution.as_str(), index, error = %error, "artifact upload failed");
                tokio::time::sleep(context.policy.backoff).await;
            }
        }
        failures += 1;
    }
    PutOutcome::Failed
}

/// A delivered result with the objects the Node measured.
fn delivered(artifacts: Vec<StoredObject>) -> ScheduledArtifactsExecutionResult {
    ScheduledArtifactsExecutionResult::Delivered(ScheduledArtifactsDelivered { artifacts })
}

/// The terminal failure of an upload that could not deliver every declared artifact.
fn failed(reason: ScheduledArtifactsFailureReason) -> ScheduledArtifactsExecutionResult {
    ScheduledArtifactsExecutionResult::Failed(ScheduledArtifactsFailed { reason })
}

/// What a stop before the executor reaches its own verdict reports.
fn interrupted() -> ScheduledArtifactsExecutionResult {
    failed(ScheduledArtifactsFailureReason::Interrupted)
}

/// Reports one accepted upload's durable state; the executor picks the execution up on its own.
pub(super) fn status(
    node: &ManagedNode,
    record: ora_node_db::ScheduledArtifactsDelivery,
) -> Vec<NodeToControllerMessage> {
    vec![NodeToControllerMessage::ExecutionStatus(
        ExecutionStatusMessage {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            operation_id: record.command.operation_id.clone(),
            execution_id: record.command.execution_id.clone(),
            payload: ExecutionStatus {
                stop_unconfirmed: Vec::new(),
                node: node.identity().clone(),
                state: record.progress.state(),
            },
        },
    )]
}

#[cfg(test)]
mod tests;
