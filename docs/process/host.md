# Process Host

English | [中文](host.zh.md)

`ora-process-host` (`apps/ora-process-host`) owns the durable Scope and Run intent of a Node's managed
programs and coordinates each Scope's [guardian](guardian.md). It accepts Run, Stop and Close intent over
local IPC; every guardian continues independently of the host and the requesting Node. No root, helper,
token or key is required: this is a cooperative same-user deployment. The overview is in
[Process runtime](process-runtime.md). The relevant decisions are the
[rootless guardian bootstrap](https://github.com/ora-space/specs/blob/main/decisions/node/process/recovery/20260917-rootless-guardian-bootstrap-and-reconnect.md)
and the [independent journals](https://github.com/ora-space/specs/blob/main/decisions/node/process/persistence/0-independent-journals-and-responsibility-retirement.md)
ADRs.

## Running

```text
ora-process-host create  /absolute/private/process-state /absolute/private/bin/ora-process-guardian
ora-process-host recover /absolute/private/process-state /absolute/private/bin/ora-process-guardian
```

`create` requires an absent state directory and `recover` the original complete journal and lock; a
failure never falls back to creation. The host runs in the foreground. The guardian executable must be
at a trusted absolute path (release builds reject group- or other-writable ancestors; both builds check
owners, symlinks and inode types). `SIGTERM`/`SIGINT` stop host coordination, not workloads: close Scopes
and observe closure before terminating work. Container or service-manager group termination is not the
same as stopping only the host, see [Container images](../operations/container-images.md).

`ora_process_client::ProcessHost::new(state_dir, expected_uid)` connects to that directory.
`execute(HostOperation)` supports `Inspect`, `CreateScope`, `Start`, `QueryRun`, `Stop`, `Close`,
`QueryScope` and bounded `Output`. Each exchange uses a new socket and callers keep their original ids
across reconnects. Start acceptance is not execution success, Stop acceptance is not completed cleanup,
and dropping a client or a wait never cancels an accepted operation.

## State directory

`HostState` takes an absolute, dedicated `state_dir`, never `HOME` or the business cwd, injected
separately from Node data.

- `HostState::create` requires an absent directory (an existing empty one is rejected too) and creates
  only `host.lock`, `host.sqlite`, its SQLite sidecars and `scopes/`.
- `HostState::recover` requires the existing lock, database and `scopes/`. Missing files, unknown root
  entries, malformed identities and incompatible journals fail without resetting anything.
- Directories are owner-private and files are private regular inodes with one hard link; symlinks and
  writable ancestors are rejected through `ora-utils::path`. Existing permissions are never changed.
- Only the ext family, XFS and Btrfs are accepted; network, memory, overlay and unknown filesystems are
  rejected.
- The canonical Scope id plus `control.sock` must fit Linux's socket path limit. Long paths are
  rejected, not shortened, so a group-writable checkout or `/tmp` is not a supported parent.

Creation fails closed: an interrupted initialization leaves a partial directory that recovery rejects.
Never delete lock files, replace the directory while owned or remove records to bypass a failure.

## Journal

The journal (`application_id` `0x4f524148`, `user_version = 6`) checks its exact schema, integrity and
persisted identities and enables WAL with `synchronous=FULL`. Transactions commit, and containing
directory entries are synced, before new facts are returned; a filesystem error after commit can
therefore leave an accepted record even when the call reports failure, so callers query the original
identity instead of inferring non-acceptance.

- Recovery takes the original `host.lock` without waiting, runs a read-only compatibility check and only
  then commits a new host incarnation (a positive epoch plus an instance id). Contention is an error, not
  permission to replace anything.
- `record_scope_intent` commits a Scope's original intent (or returns it unchanged). It creates no
  directory, guardian or process; a pre-existing Scope path without intent blocks registration.
- `start_guardian` commits a `launch_unknown` record, creates the private Scope directory, takes its lock
  and execs the guardian. Every later error or cancellation consumes that attempt: even a proven exec
  failure never authorizes a second launch for the Scope. `guardian_access` restores discovery material
  after recovery without launching anything.
- `record_run_intent` saves the immutable ScopeId, RunId, exact `RunSpec` and host-disconnect policy
  before Start is sent; the whole request must fit the wire frame. Environment values are stored verbatim
  (native non-UTF-8 values survive). The same RunId and intent return the original record; changed
  parameters conflict. `run_intents` enumerates every record in RunId order.
- `request_run_stop` records force-stop responsibility and `request_scope_close` permanently seals
  admission; both survive restart, and Close stops every Run of the Scope without rewriting its specs.

A confirmed launch is durable positive evidence: later unknown observations retain `LaunchFact::Started`.
A later `NotStarted` launch or direct-process report is rejected without replacing the saved observation,
including after host recovery. This prevents cancellation of a missing guardian Run from erasing a
known launch and becoming false stop confirmation.
The coordinator rejects only that snapshot, exposes `Rejected(ConflictingRun)` in the Scope's
coordination status, and continues persisting other Run and Scope observations. It retains retry
responsibility without stopping the host; journal and filesystem failures still propagate as errors.

Older exact journal layouts (versions 1 to 5) migrate transactionally. A version-2 journal with existing
Scope directories is refused before migration, because those guardians still speak the old token
protocol: keep a compatible old host or use a new state directory, and never delete a Scope to force an
upgrade. Removed token values are not promised to be securely erased from SQLite free pages or backups.

## Coordination

`HostCoordinator` composes the journal with the guardian client; its owner drives `tick()`, and accepted
Start, Stop and Close then progress without the requesting connection. Each Scope has at most one active
exchange, with up to 32 Scopes in flight and round-robin scheduling. Failed exchanges back off from
hundreds of milliseconds to five seconds plus Scope-specific jitter. Neither socket I/O nor bootstrap
delivery holds the journal while waiting for a peer.

A newly accepted Close wakes its Scope without waiting for the previous observation backoff. An
in-flight exchange keeps ownership and the close is dispatched after it finishes; duplicate Close
requests do not reset failure backoff. Waking coordination is not closure evidence: callers still
wait for the original guardian's Closed observation.

After this host reconciles and persists Closed for the Scope and terminal cleanup for every accepted
Run, the Scope stops occupying periodic exchange slots. Unknown or incomplete Runs continue to be
reconciled under their original identities. This terminal cache belongs only to the current host:
recovery first rebinds the original guardian, then derives quiescence again from the original facts.
Scope and Run journals, close intent, queries and captured output remain available.

Changed Run and Scope observations are persisted before queries expose them. Queries return the durable
`last_observed` facts and a separate `coordination` status (`Pending` after recovery, `Unavailable` when
a guardian is lost); a historical `Running` never proves a process is live, and no stored PID or snapshot
becomes launch or signal authority. A close before any guardian attempt completes locally; otherwise the
original guardian must answer. Dropping the coordinator stops its transport tasks, not guardians.

## Wire protocol

Host wire version 2 uses the bounded MessagePack codec (16 KiB frames, depth 16). Control and output use
separate `host.sock` and `host-io.sock` endpoints, each with 16 connection slots and a five-second
exchange deadline; output chunks are at most 4096 bytes, so a slow output client cannot occupy control
slots. Both peers check the kernel UID, and the host binding comes from the host's journal, never from an
epoch the Node supplies. Recovery replaces only recognized private single-link sockets that refuse
connections; live listeners, regular files, symlinks and incompatible journals are preserved. Shutdown
does not unlink endpoints.
