//! Uploads against a local HTTP object-store double: Cloud's granted keys, measured digests, the
//! per-index grant rule, a refused upload settling instead of waiting, and the aggregate failure a
//! partial upload reports.
use super::*;
use crate::revision::{GrantRequest, GrantStore, RetryPolicy};
use ora_utils::http::{ProxyConfig, ReqwestUploader, UploadOptions};
use pretty_assertions::assert_eq;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// One PUT the object store received.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Put {
    path: String,
    body: Vec<u8>,
}

/// Answers each request with the next scripted status for its path (201 once the script ends).
#[derive(Clone)]
struct ObjectStore {
    address: String,
    received: Arc<Mutex<Vec<Put>>>,
    script: Arc<Mutex<BTreeMap<String, VecDeque<u16>>>>,
}

impl ObjectStore {
    async fn start() -> Self {
        ora_logging::initialize_test_clock();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let store = Self {
            address: listener.local_addr().unwrap().to_string(),
            received: Arc::default(),
            script: Arc::default(),
        };
        let serving = store.clone();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let store = serving.clone();
                tokio::spawn(async move {
                    let mut buffer = Vec::new();
                    let end = loop {
                        let mut chunk = [0_u8; 4096];
                        let count = stream.read(&mut chunk).await.unwrap();
                        if count == 0 {
                            return;
                        }
                        buffer.extend_from_slice(&chunk[..count]);
                        if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                            break end;
                        }
                    };
                    let head = String::from_utf8(buffer[..end].to_vec()).unwrap();
                    let mut lines = head.split("\r\n");
                    let path = lines.next().unwrap().split(' ').nth(1).unwrap().to_owned();
                    let headers: BTreeMap<String, String> = lines
                        .map(|line| {
                            let (name, value) = line.split_once(':').unwrap();
                            (name.to_ascii_lowercase(), value.trim().to_owned())
                        })
                        .collect();
                    let length: usize = headers["content-length"].parse().unwrap();
                    let mut body = buffer[end + 4..].to_vec();
                    while body.len() < length {
                        let mut chunk = [0_u8; 4096];
                        let count = stream.read(&mut chunk).await.unwrap();
                        body.extend_from_slice(&chunk[..count]);
                    }
                    let key = path
                        .split('?')
                        .next()
                        .unwrap()
                        .trim_start_matches('/')
                        .to_owned();
                    let status = store
                        .script
                        .lock()
                        .unwrap()
                        .get_mut(&key)
                        .and_then(VecDeque::pop_front)
                        .unwrap_or(201);
                    store.received.lock().unwrap().push(Put { path: key, body });
                    let _ = stream
                        .write_all(
                            format!("HTTP/1.1 {status} X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                                .as_bytes(),
                        )
                        .await;
                });
            }
        });
        store
    }

    /// Queues statuses for the next requests to `key`.
    fn answer(&self, key: &str, statuses: &[u16]) {
        self.script
            .lock()
            .unwrap()
            .entry(key.to_owned())
            .or_default()
            .extend(statuses);
    }

    fn received(&self) -> Vec<Put> {
        self.received.lock().unwrap().clone()
    }

    /// A grant shaped like Cloud's: signed conditional create on the key Cloud chose.
    fn grant(&self, key: &str, valid_for: Duration) -> ObjectUploadGrant {
        ObjectUploadGrant {
            object_key: ObjectKey::new(key),
            url: PresignedUrl::new(format!(
                "http://{}/{key}?X-Amz-Signature=secret",
                self.address
            )),
            method: UploadMethod::Put,
            headers: [("If-None-Match".to_owned(), "*".to_owned())].into(),
            expires_at: ora_logging::clock::now_local() + valid_for,
        }
    }
}

/// Short timings keep the tests fast; three attempts per artifact as in production.
const POLICY: RetryPolicy = RetryPolicy {
    attempts: 3,
    resend: Duration::from_millis(/*millis*/ 50),
    backoff: Duration::from_millis(/*millis*/ 10),
    expiry_margin: Duration::from_secs(/*secs*/ 5),
};

fn uploader() -> HttpUploader {
    HttpUploader::new(
        ReqwestUploader::new(ProxyConfig::default()),
        UploadOptions {
            connect_timeout: Duration::from_secs(/*secs*/ 5),
            total_timeout: Duration::from_secs(/*secs*/ 10),
        },
    )
}

/// Writes the reserved artifact directory and returns the declaration of those files.
fn declared_files(root: &Path, files: &[(&str, &[u8])]) -> ScheduledArtifactManifest {
    let directory = root.join(SCHEDULED_ARTIFACT_DIRECTORY);
    let mut items = Vec::new();
    for (index, (name, bytes)) in files.iter().enumerate() {
        let path = directory.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        items.push(ScheduledArtifactItem {
            index: index as u32,
            display_name: (*name).to_owned(),
            kind: ScheduledArtifactKind::Markdown,
            size_bytes: bytes.len() as u64,
            sha256: ora_utils::hash::sha256_hex(bytes),
        });
    }
    ScheduledArtifactManifest {
        items,
        rejections: Vec::new(),
        omitted_rejections: 0,
        walk_truncated: false,
    }
}

/// A job over the declared checkout.
fn job(root: &Path, manifest: ScheduledArtifactManifest) -> Job {
    Job {
        operation: OperationId::new("run-1"),
        execution: ExecutionId::new("exec-upload"),
        node_id: NodeId::new("test-node"),
        checkout: root.to_path_buf(),
        manifest,
    }
}

/// Hands Cloud's grants to the store as the control session would.
fn offer_grants(grants: &GrantStore, job: &Job, issued: Vec<ScheduledArtifactUploadGrant>) {
    grants.offer_artifacts(ScheduledArtifactGrantsMessage {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        operation_id: job.operation.clone(),
        execution_id: job.execution.clone(),
        payload: ScheduledArtifactGrants { grants: issued },
    });
}

/// Withdraws the upload's grants as the control session would after Cloud's refusal.
fn refuse_upload(grants: &GrantStore, job: &Job) {
    grants.refuse_artifacts(ScheduledArtifactGrantsRefusedMessage {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        operation_id: job.operation.clone(),
        execution_id: job.execution.clone(),
    });
}

/// Waits for the next artifact grant request the upload sends.
async fn requested(
    requests: &mut tokio::sync::broadcast::Receiver<GrantRequest>,
) -> ScheduledArtifactGrantNeededMessage {
    let timeout = tokio::time::timeout(Duration::from_secs(/*secs*/ 10), requests.recv());
    match timeout.await.unwrap().unwrap() {
        GrantRequest::Artifacts(message) => message,
        GrantRequest::Revisions(message) => {
            panic!("a scheduled upload asked for Revision grants: {message:?}")
        }
    }
}

/// Both declared artifacts are uploaded to the keys Cloud granted and reported with the digests the
/// executor measured. A reply that arrives for a smaller set is applied to the index it names: the
/// later reply completes the set instead of the whole exchange failing.
#[tokio::test]
async fn declared_artifacts_upload_to_granted_keys_with_measured_digests() {
    let directory = tempfile::tempdir().unwrap();
    let manifest = declared_files(
        directory.path(),
        &[("reports/review.md", b"hello"), ("data.json", b"{\"a\":1}")],
    );
    let job = job(directory.path(), manifest.clone());
    let store = ObjectStore::start().await;
    let grants = GrantStore::new();
    let mut requests = grants.subscribe();
    let running = tokio::spawn({
        let (grants, job) = (grants.clone(), job.clone());
        async move { upload(&uploader(), &grants, &job, POLICY).await }
    });
    let request = requested(&mut requests).await;
    assert_eq!(
        request.payload.artifacts,
        vec![
            ScheduledArtifactUpload {
                index: 0,
                size_bytes: 5,
                sha256: Sha256Digest::new(ora_utils::hash::sha256_hex(b"hello")),
            },
            ScheduledArtifactUpload {
                index: 1,
                size_bytes: 7,
                sha256: Sha256Digest::new(ora_utils::hash::sha256_hex(b"{\"a\":1}")),
            },
        ]
    );
    // A reply for one artifact of the outstanding set is applied to that index.
    offer_grants(
        &grants,
        &job,
        vec![ScheduledArtifactUploadGrant {
            index: 0,
            grant: store.grant(
                "scheduled-runs/tenant/run/0",
                Duration::from_secs(/*secs*/ 3600),
            ),
        }],
    );
    assert_eq!(store.received(), vec![]);
    offer_grants(
        &grants,
        &job,
        vec![ScheduledArtifactUploadGrant {
            index: 1,
            grant: store.grant(
                "scheduled-runs/tenant/run/1",
                Duration::from_secs(/*secs*/ 3600),
            ),
        }],
    );
    assert_eq!(
        running.await.unwrap(),
        ScheduledArtifactsExecutionResult::Delivered(ScheduledArtifactsDelivered {
            artifacts: manifest
                .items
                .iter()
                .enumerate()
                .map(|(index, item)| StoredObject {
                    key: ObjectKey::new(format!("scheduled-runs/tenant/run/{index}")),
                    size: item.size_bytes,
                    sha256: Sha256Digest::new(item.sha256.clone()),
                })
                .collect(),
        })
    );
    assert_eq!(
        store.received(),
        vec![
            Put {
                path: "scheduled-runs/tenant/run/0".to_owned(),
                body: b"hello".to_vec(),
            },
            Put {
                path: "scheduled-runs/tenant/run/1".to_owned(),
                body: b"{\"a\":1}".to_vec(),
            },
        ]
    );
}

/// One artifact exhausting its attempts does not hide the other: Cloud registers this family item by
/// item, so the execution reports the artifact it stored and leaves the failed one for Cloud's
/// per-item rejection.
#[tokio::test]
async fn a_partial_upload_reports_the_stored_artifacts() {
    let directory = tempfile::tempdir().unwrap();
    let manifest = declared_files(
        directory.path(),
        &[("first.md", b"first"), ("second.md", b"second")],
    );
    let stored_digest = manifest.items[0].sha256.clone();
    let job = job(directory.path(), manifest);
    let store = ObjectStore::start().await;
    store.answer("scheduled-runs/tenant/run/1", &[500, 500, 500]);
    let grants = GrantStore::new();
    let mut requests = grants.subscribe();
    let running = tokio::spawn({
        let (grants, job) = (grants.clone(), job.clone());
        async move { upload(&uploader(), &grants, &job, POLICY).await }
    });
    requested(&mut requests).await;
    offer_grants(
        &grants,
        &job,
        vec![
            ScheduledArtifactUploadGrant {
                index: 0,
                grant: store.grant(
                    "scheduled-runs/tenant/run/0",
                    Duration::from_secs(/*secs*/ 3600),
                ),
            },
            ScheduledArtifactUploadGrant {
                index: 1,
                grant: store.grant(
                    "scheduled-runs/tenant/run/1",
                    Duration::from_secs(/*secs*/ 3600),
                ),
            },
        ],
    );
    assert_eq!(
        running.await.unwrap(),
        ScheduledArtifactsExecutionResult::Delivered(ScheduledArtifactsDelivered {
            artifacts: vec![StoredObject {
                key: ObjectKey::new("scheduled-runs/tenant/run/0"),
                size: b"first".len() as u64,
                sha256: Sha256Digest::new(stored_digest),
            }],
        })
    );
    assert_eq!(
        store
            .received()
            .into_iter()
            .map(|put| put.path)
            .collect::<Vec<_>>(),
        vec![
            "scheduled-runs/tenant/run/0".to_owned(),
            "scheduled-runs/tenant/run/1".to_owned(),
            "scheduled-runs/tenant/run/1".to_owned(),
            "scheduled-runs/tenant/run/1".to_owned(),
        ]
    );
}

/// An upload that stored nothing reports the single failure Cloud recognises: nothing is registered,
/// so there is no delivered subset to name.
#[tokio::test]
async fn an_upload_that_stored_nothing_reports_upload_failed() {
    let directory = tempfile::tempdir().unwrap();
    let manifest = declared_files(directory.path(), &[("only.md", b"only")]);
    let job = job(directory.path(), manifest);
    let store = ObjectStore::start().await;
    store.answer("scheduled-runs/tenant/run/0", &[500, 500, 500]);
    let grants = GrantStore::new();
    let mut requests = grants.subscribe();
    let running = tokio::spawn({
        let (grants, job) = (grants.clone(), job.clone());
        async move { upload(&uploader(), &grants, &job, POLICY).await }
    });
    requested(&mut requests).await;
    offer_grants(
        &grants,
        &job,
        vec![ScheduledArtifactUploadGrant {
            index: 0,
            grant: store.grant(
                "scheduled-runs/tenant/run/0",
                Duration::from_secs(/*secs*/ 3600),
            ),
        }],
    );
    assert_eq!(
        running.await.unwrap(),
        ScheduledArtifactsExecutionResult::Failed(ScheduledArtifactsFailed {
            reason: ScheduledArtifactsFailureReason::UploadFailed,
        })
    );
    assert_eq!(
        store
            .received()
            .into_iter()
            .map(|put| put.path)
            .collect::<Vec<_>>(),
        vec![
            "scheduled-runs/tenant/run/0".to_owned(),
            "scheduled-runs/tenant/run/0".to_owned(),
            "scheduled-runs/tenant/run/0".to_owned(),
        ]
    );
}

/// An execution whose session holds no declaration fails without touching the Workspace.
#[test]
fn a_missing_declaration_reports_declaration_unavailable() {
    let directory = tempfile::tempdir().unwrap();
    let db = ora_node_db::NodeDatabase::open(
        &directory.path().join("db"),
        ora_node_db::NodeIdentity::Discover,
    )
    .unwrap();
    let command = DeliverScheduledArtifactsMessage {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        operation_id: OperationId::new("run-1"),
        execution_id: ExecutionId::new("exec-upload"),
        payload: DeliverScheduledArtifacts {
            spec: DeliverScheduledArtifactsSpec {
                session_execution_id: ExecutionId::new("missing-session"),
            },
        },
    };
    assert_eq!(
        resolve(&db, &command).unwrap_err(),
        ScheduledArtifactsFailureReason::DeclarationUnavailable
    );
}

/// Waits until the object store received `count` PUTs, so a test can act between two uploads.
async fn stored_puts(store: &ObjectStore, count: usize) -> Vec<Put> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(/*secs*/ 10);
    loop {
        let received = store.received();
        if received.len() >= count {
            return received;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the upload stopped before storing {count} object(s)"
        );
        tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
    }
}

/// Cloud refuses the remaining grants once the run left `delivering`: the upload settles with what
/// it already stored instead of holding the Node's slot open for a grant that can never arrive.
#[tokio::test]
async fn a_refused_upload_reports_what_it_stored_and_stops() {
    let directory = tempfile::tempdir().unwrap();
    let manifest = declared_files(
        directory.path(),
        &[("first.md", b"first"), ("second.md", b"second")],
    );
    let stored_digest = manifest.items[0].sha256.clone();
    let job = job(directory.path(), manifest);
    let store = ObjectStore::start().await;
    let grants = GrantStore::new();
    let mut requests = grants.subscribe();
    let running = tokio::spawn({
        let (grants, job) = (grants.clone(), job.clone());
        async move { upload(&uploader(), &grants, &job, POLICY).await }
    });
    requested(&mut requests).await;
    offer_grants(
        &grants,
        &job,
        vec![ScheduledArtifactUploadGrant {
            index: 0,
            grant: store.grant(
                "scheduled-runs/tenant/run/0",
                Duration::from_secs(/*secs*/ 3600),
            ),
        }],
    );
    assert_eq!(stored_puts(&store, /*count*/ 1).await.len(), 1);
    refuse_upload(&grants, &job);
    assert_eq!(
        running.await.unwrap(),
        ScheduledArtifactsExecutionResult::Delivered(ScheduledArtifactsDelivered {
            artifacts: vec![StoredObject {
                key: ObjectKey::new("scheduled-runs/tenant/run/0"),
                size: b"first".len() as u64,
                sha256: Sha256Digest::new(stored_digest),
            }],
        })
    );
    assert_eq!(
        store
            .received()
            .into_iter()
            .map(|put| put.path)
            .collect::<Vec<_>>(),
        vec!["scheduled-runs/tenant/run/0".to_owned()]
    );
}

/// A refusal before anything was stored reports the one failure Cloud recognises; nothing was
/// registered, so there is no delivered subset to name.
#[tokio::test]
async fn a_refused_upload_that_stored_nothing_reports_upload_failed() {
    let directory = tempfile::tempdir().unwrap();
    let manifest = declared_files(directory.path(), &[("only.md", b"only")]);
    let job = job(directory.path(), manifest);
    let store = ObjectStore::start().await;
    let grants = GrantStore::new();
    let mut requests = grants.subscribe();
    let running = tokio::spawn({
        let (grants, job) = (grants.clone(), job.clone());
        async move { upload(&uploader(), &grants, &job, POLICY).await }
    });
    requested(&mut requests).await;
    refuse_upload(&grants, &job);
    assert_eq!(
        running.await.unwrap(),
        ScheduledArtifactsExecutionResult::Failed(ScheduledArtifactsFailed {
            reason: ScheduledArtifactsFailureReason::UploadFailed,
        })
    );
    assert_eq!(store.received(), vec![]);
}

/// An expired signature sends the executor back for a fresh grant; Cloud's refusal ends that wait,
/// so an upload crossing its deadline writes a terminal result instead of running forever.
#[tokio::test]
async fn a_refusal_after_an_expired_grant_settles_the_upload() {
    let directory = tempfile::tempdir().unwrap();
    let manifest = declared_files(directory.path(), &[("only.md", b"only")]);
    let job = job(directory.path(), manifest);
    let store = ObjectStore::start().await;
    store.answer("scheduled-runs/tenant/run/0", &[403]);
    let grants = GrantStore::new();
    let mut requests = grants.subscribe();
    let running = tokio::spawn({
        let (grants, job) = (grants.clone(), job.clone());
        async move { upload(&uploader(), &grants, &job, POLICY).await }
    });
    requested(&mut requests).await;
    offer_grants(
        &grants,
        &job,
        vec![ScheduledArtifactUploadGrant {
            index: 0,
            grant: store.grant(
                "scheduled-runs/tenant/run/0",
                Duration::from_secs(/*secs*/ 3600),
            ),
        }],
    );
    // The refused PUT discards the grant and waits again; the refusal ends that wait.
    assert_eq!(stored_puts(&store, /*count*/ 1).await.len(), 1);
    refuse_upload(&grants, &job);
    assert_eq!(
        running.await.unwrap(),
        ScheduledArtifactsExecutionResult::Failed(ScheduledArtifactsFailed {
            reason: ScheduledArtifactsFailureReason::UploadFailed,
        })
    );
    assert_eq!(store.received().len(), 1);
}

/// A file that changed after the declaration is refused before any PUT: Cloud signed the declared
/// size and digest into the grant, so uploading other bytes could only be rejected there.
#[tokio::test]
async fn an_artifact_that_no_longer_matches_its_declaration_is_not_uploaded() {
    let directory = tempfile::tempdir().unwrap();
    let manifest = declared_files(directory.path(), &[("only.md", b"only")]);
    std::fs::write(
        directory
            .path()
            .join(SCHEDULED_ARTIFACT_DIRECTORY)
            .join("only.md"),
        b"changed",
    )
    .unwrap();
    let job = job(directory.path(), manifest);
    let store = ObjectStore::start().await;
    let grants = GrantStore::new();
    let mut requests = grants.subscribe();
    assert_eq!(
        upload(&uploader(), &grants, &job, POLICY).await,
        ScheduledArtifactsExecutionResult::Failed(ScheduledArtifactsFailed {
            reason: ScheduledArtifactsFailureReason::UploadFailed,
        })
    );
    assert_eq!(store.received(), vec![]);
    assert!(matches!(
        requests.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

/// A grant for an artifact that is already stored is ignored instead of failing the exchange: a
/// reply can cross the retry that removed the index from the outstanding set.
#[tokio::test]
async fn a_grant_for_an_already_stored_artifact_is_ignored() {
    let directory = tempfile::tempdir().unwrap();
    let manifest = declared_files(
        directory.path(),
        &[("first.md", b"first"), ("second.md", b"second")],
    );
    let job = job(directory.path(), manifest);
    let store = ObjectStore::start().await;
    let grants = GrantStore::new();
    let mut requests = grants.subscribe();
    let running = tokio::spawn({
        let (grants, job) = (grants.clone(), job.clone());
        async move { upload(&uploader(), &grants, &job, POLICY).await }
    });
    requested(&mut requests).await;
    offer_grants(
        &grants,
        &job,
        vec![ScheduledArtifactUploadGrant {
            index: 0,
            grant: store.grant(
                "scheduled-runs/tenant/run/0",
                Duration::from_secs(/*secs*/ 3600),
            ),
        }],
    );
    stored_puts(&store, /*count*/ 1).await;
    // The stale reply repeats index 0, which is no longer outstanding, and adds the missing index.
    offer_grants(
        &grants,
        &job,
        vec![
            ScheduledArtifactUploadGrant {
                index: 0,
                grant: store.grant(
                    "scheduled-runs/tenant/run/0",
                    Duration::from_secs(/*secs*/ 3600),
                ),
            },
            ScheduledArtifactUploadGrant {
                index: 1,
                grant: store.grant(
                    "scheduled-runs/tenant/run/1",
                    Duration::from_secs(/*secs*/ 3600),
                ),
            },
        ],
    );
    assert!(matches!(
        running.await.unwrap(),
        ScheduledArtifactsExecutionResult::Delivered(ScheduledArtifactsDelivered { artifacts })
            if artifacts.len() == 2
    ));
    assert_eq!(
        store
            .received()
            .into_iter()
            .map(|put| put.path)
            .collect::<Vec<_>>(),
        vec![
            "scheduled-runs/tenant/run/0".to_owned(),
            "scheduled-runs/tenant/run/1".to_owned(),
        ]
    );
}
