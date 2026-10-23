//! Upload grants held only in memory, and the requests for them.
//!
//! A grant is a bearer credential that expires, so it never enters the ledger, a log line or a
//! `Debug` print: a restarted Node asks again instead (protocol D5). The control session hands
//! every inbound `UploadGrant` and `ScheduledArtifactGrants` to [`GrantStore`] without queueing it
//! behind admission, and forwards every request the store emits to the connected Controller.
//!
//! Two families share the store. A Revision delivery names object keys up front; a scheduled-artifact
//! upload cannot, because Cloud chooses every key, so it asks by declaration index and only learns
//! the key from the grant it receives.
use ora_node_protocol::{
    CURRENT_PROTOCOL_VERSION, ExecutionId, NodeId, ObjectKey, ObjectUploadGrant, OperationId,
    ScheduledArtifactGrantNeededMessage, ScheduledArtifactGrantsMessage, ScheduledArtifactUpload,
    Sha256Digest, UploadGrantMessage, UploadGrantNeeded, UploadGrantNeededMessage,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::{broadcast, watch};

/// Requests not yet written by a slow session are superseded by the next periodic request.
const REQUEST_BUFFER: usize = 16;

/// Grants and grant requests shared by the upload executors and the control session.
#[derive(Clone)]
pub(crate) struct GrantStore {
    inner: Arc<Inner>,
}

struct Inner {
    waiting: Mutex<HashMap<ExecutionId, Waiting>>,
    /// Bumped on every accepted grant so an uploader blocked on a missing grant wakes up.
    offered: watch::Sender<u64>,
    requests: broadcast::Sender<GrantRequest>,
}

/// A request the session turns into one Controller message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum GrantRequest {
    /// A Revision delivery asks for the object keys it froze.
    Revisions(UploadGrantNeededMessage),
    /// A scheduled-artifact upload asks for the declaration indices it still has to store.
    Artifacts(ScheduledArtifactGrantNeededMessage),
}

/// Why a grant reply could not be accepted as a whole.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GrantMismatch {
    /// The reply did not name exactly the artifacts the last request named.
    RequestedSet,
    /// No request was outstanding, so the reply answers nothing this Node asked.
    Unexpected,
}

impl fmt::Display for GrantMismatch {
    /// Keeps the protocol violation the session closes with bounded and log-safe.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RequestedSet => "scheduled-artifact grants do not match the requested artifacts",
            Self::Unexpected => "scheduled-artifact grants answer no outstanding request",
        })
    }
}

impl std::error::Error for GrantMismatch {}

/// An uploading execution: what it still needs and the grants it holds.
struct Waiting {
    operation: OperationId,
    node: NodeId,
    keys: BTreeMap<ObjectKey, Sha256Digest>,
    grants: HashMap<ObjectKey, ObjectUploadGrant>,
    /// Declared artifacts still to store, by the index Cloud signs.
    artifacts: BTreeMap<u32, ScheduledArtifactUpload>,
    artifact_grants: HashMap<u32, ObjectUploadGrant>,
    /// Indices of the last artifact request; a reply must name exactly this set.
    requested_artifacts: Option<BTreeSet<u32>>,
    /// Set while the upload waits for a grant, so a new connection can repeat the request at once.
    requesting: bool,
}

impl GrantStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                waiting: Mutex::new(HashMap::new()),
                offered: watch::channel(/*init*/ 0).0,
                requests: broadcast::channel(REQUEST_BUFFER).0,
            }),
        }
    }

    /// Delivers grant requests to one connection; requests sent while nobody listens are lost,
    /// which is harmless because a waiting upload repeats its request.
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<GrantRequest> {
        self.inner.requests.subscribe()
    }

    /// Keeps the grants for keys the execution is currently uploading and drops the rest: a grant
    /// for an unknown execution or key could only make the Node write somewhere it was not asked to.
    pub(crate) fn offer(&self, message: UploadGrantMessage) {
        let mut waiting = self.lock();
        let Some(execution) = waiting.get_mut(&message.execution_id) else {
            ora_logging::ora_debug!(execution_id = %message.execution_id.as_str(), "ignored upload grants for an execution that is not uploading");
            return;
        };
        if execution.operation != message.operation_id {
            ora_logging::ora_warn!(execution_id = %message.execution_id.as_str(), "ignored upload grants naming another operation");
            return;
        }
        for grant in message.payload.grants {
            if execution.keys.contains_key(&grant.object_key) {
                execution.grants.insert(grant.object_key.clone(), grant);
            } else {
                ora_logging::ora_warn!(execution_id = %message.execution_id.as_str(), object_key = %grant.object_key.as_str(), "ignored an upload grant for a key the delivery does not upload");
            }
        }
        drop(waiting);
        self.wake();
    }

    /// Accepts artifact grants only when they answer the last request exactly.
    ///
    /// Cloud returns one grant per requested artifact in the same order, but the Node cannot trust
    /// order alone; the signed indices are the contract. A reply naming another set is refused whole
    /// instead of being partly applied, so a mistaken grant can never be paired with the wrong file.
    pub(crate) fn offer_artifacts(
        &self,
        message: ScheduledArtifactGrantsMessage,
    ) -> Result<(), GrantMismatch> {
        let mut waiting = self.lock();
        let Some(execution) = waiting.get_mut(&message.execution_id) else {
            ora_logging::ora_warn!(execution_id = %message.execution_id.as_str(), "ignored scheduled-artifact grants for an execution that is not uploading");
            return Ok(());
        };
        if execution.operation != message.operation_id {
            ora_logging::ora_warn!(execution_id = %message.execution_id.as_str(), "ignored scheduled-artifact grants naming another operation");
            return Ok(());
        }
        let Some(requested) = execution.requested_artifacts.clone() else {
            return Err(GrantMismatch::Unexpected);
        };
        let received: BTreeSet<u32> = message.payload.grants.iter().map(|g| g.index).collect();
        if received != requested {
            return Err(GrantMismatch::RequestedSet);
        }
        for grant in message.payload.grants {
            if execution.artifacts.contains_key(&grant.index) {
                execution.artifact_grants.insert(grant.index, grant.grant);
            }
        }
        drop(waiting);
        self.wake();
        Ok(())
    }

    /// Starts accepting grants for `keys`; dropping the registration forgets them and any grant.
    pub(crate) fn register(
        &self,
        operation: OperationId,
        execution: ExecutionId,
        node: NodeId,
        keys: BTreeMap<ObjectKey, Sha256Digest>,
    ) -> Registration {
        self.lock().insert(
            execution.clone(),
            Waiting {
                operation,
                node,
                keys,
                grants: HashMap::new(),
                artifacts: BTreeMap::new(),
                artifact_grants: HashMap::new(),
                requested_artifacts: None,
                requesting: false,
            },
        );
        Registration {
            store: self.clone(),
            execution,
        }
    }

    /// Starts accepting grants for the artifacts Cloud has not granted yet.
    ///
    /// The artifacts themselves are the request: their indexes, declared sizes and digests are what
    /// Cloud binds into each signature and into the object key it chooses.
    pub(crate) fn register_artifacts(
        &self,
        operation: OperationId,
        execution: ExecutionId,
        node: NodeId,
        artifacts: BTreeMap<u32, ScheduledArtifactUpload>,
    ) -> Registration {
        self.lock().insert(
            execution.clone(),
            Waiting {
                operation,
                node,
                keys: BTreeMap::new(),
                grants: HashMap::new(),
                artifacts,
                artifact_grants: HashMap::new(),
                requested_artifacts: None,
                requesting: false,
            },
        );
        Registration {
            store: self.clone(),
            execution,
        }
    }

    /// The requests of every upload currently waiting for a grant. A new Controller connection
    /// sends these first: a restarted Controller remembers no earlier request.
    pub(crate) fn pending(&self) -> Vec<GrantRequest> {
        self.lock()
            .iter()
            .filter(|(_, waiting)| waiting.requesting)
            .flat_map(|(execution, waiting)| waiting.requests(execution))
            .collect()
    }

    /// Observes grant arrivals; mark the receiver seen before checking for a grant.
    pub(crate) fn offers(&self) -> watch::Receiver<u64> {
        self.inner.offered.subscribe()
    }

    /// Wakes every uploader waiting on a grant; the store ignores the send failure because the
    /// receiver list is only a wake-up notification.
    fn wake(&self) {
        self.inner
            .offered
            .send_modify(|count| *count = count.wrapping_add(1));
    }

    /// Mutex poisoning only means another thread panicked mid-update of plain maps; grants are
    /// re-requestable, so the data is still safe to use.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ExecutionId, Waiting>> {
        self.inner
            .waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// One execution's live claim on the store.
pub(crate) struct Registration {
    store: GrantStore,
    execution: ExecutionId,
}

impl Registration {
    /// Returns a grant for `key` that stays valid for at least `margin`, dropping an expired one.
    pub(crate) fn valid(&self, key: &ObjectKey, margin: Duration) -> Option<ObjectUploadGrant> {
        let mut waiting = self.store.lock();
        let execution = waiting.get_mut(&self.execution)?;
        let usable = execution
            .grants
            .get(key)
            .is_some_and(|grant| grant.expires_at > ora_logging::clock::now_local() + margin);
        if usable {
            execution.requesting = false;
            return execution.grants.get(key).cloned();
        }
        execution.grants.remove(key);
        None
    }

    /// Forgets a grant the store refused, so the next attempt asks for a fresh one.
    pub(crate) fn discard(&self, key: &ObjectKey) {
        if let Some(execution) = self.store.lock().get_mut(&self.execution) {
            execution.grants.remove(key);
        }
    }

    /// Marks an object uploaded: later requests no longer ask for it, and its grant is dropped.
    pub(crate) fn uploaded(&self, key: &ObjectKey) {
        if let Some(execution) = self.store.lock().get_mut(&self.execution) {
            execution.keys.remove(key);
            execution.grants.remove(key);
        }
    }

    /// Returns the grant Cloud issued for `index` when it is still valid, dropping an expired one.
    pub(crate) fn valid_artifact(&self, index: u32, margin: Duration) -> Option<ObjectUploadGrant> {
        let mut waiting = self.store.lock();
        let execution = waiting.get_mut(&self.execution)?;
        let usable = execution
            .artifact_grants
            .get(&index)
            .is_some_and(|grant| grant.expires_at > ora_logging::clock::now_local() + margin);
        if usable {
            return execution.artifact_grants.get(&index).cloned();
        }
        execution.artifact_grants.remove(&index);
        None
    }

    /// Forgets an artifact grant the store refused, so the next attempt asks for a fresh one.
    pub(crate) fn discard_artifact(&self, index: u32) {
        if let Some(execution) = self.store.lock().get_mut(&self.execution) {
            execution.artifact_grants.remove(&index);
        }
    }

    /// Marks an artifact uploaded: later requests no longer ask for it, and its grant is dropped.
    pub(crate) fn uploaded_artifact(&self, index: u32) {
        if let Some(execution) = self.store.lock().get_mut(&self.execution) {
            execution.artifacts.remove(&index);
            execution.artifact_grants.remove(&index);
        }
    }

    /// Asks the Controller for grants covering every object still to upload, with the digests
    /// frozen before the first PUT so Cloud can bind each grant to exactly those bytes. Artifact
    /// grants are asked for by declaration index because Cloud chooses their keys.
    pub(crate) fn request(&self) {
        let messages = {
            let mut waiting = self.store.lock();
            let Some(execution) = waiting.get_mut(&self.execution) else {
                return;
            };
            if execution.keys.is_empty() && execution.artifacts.is_empty() {
                return;
            }
            execution.requesting = true;
            if !execution.artifacts.is_empty() {
                execution.requested_artifacts = Some(execution.artifacts.keys().copied().collect());
            }
            execution.requests(&self.execution)
        };
        // No connected session is not an error: the request is repeated while the upload waits.
        for message in messages {
            let _ = self.store.inner.requests.send(message);
        }
    }
}

impl Drop for Registration {
    /// A finished or interrupted upload must not keep credentials in memory.
    fn drop(&mut self) {
        self.store.lock().remove(&self.execution);
    }
}

impl Waiting {
    /// The grant requests for everything this upload still has to store.
    fn requests(&self, execution: &ExecutionId) -> Vec<GrantRequest> {
        let mut requests = Vec::new();
        if !self.keys.is_empty() {
            requests.push(GrantRequest::Revisions(UploadGrantNeededMessage {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                operation_id: self.operation.clone(),
                execution_id: execution.clone(),
                payload: UploadGrantNeeded {
                    node_id: self.node.clone(),
                    checksums: self.keys.clone(),
                },
            }));
        }
        if !self.artifacts.is_empty() {
            requests.push(GrantRequest::Artifacts(
                ScheduledArtifactGrantNeededMessage {
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                    operation_id: self.operation.clone(),
                    execution_id: execution.clone(),
                    payload: ora_node_protocol::ScheduledArtifactGrantNeeded {
                        node_id: self.node.clone(),
                        artifacts: self.artifacts.values().cloned().collect(),
                    },
                },
            ));
        }
        requests
    }
}
