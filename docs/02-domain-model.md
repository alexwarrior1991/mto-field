# 02 · Domain model

The schema of `V1` holds every concept below; the rules live in `domain/model` and the services in
`application/service` (`01-architecture.md`).

## Possession (`possession`)

The blocking of a track for one night, grouping one or more shifts of `mto-maintenance`. `code`
`PO-000001` from a sequence; `status` `OPEN` → `CLOSED`; `shift_date`; `ends_at` (the possession
window, by default the earliest `plannedEnd` of its shifts); `opened_at`/`opened_by`;
`closed_at`/`closed_by`; `forced` with its `close_reason`; and `next_command_seq`, the counter of
the downstream sequence.

Rules (`PossessionRules` and `PossessionStateMachine`):

| Transition | Rules |
|---|---|
| `open` | at least one shift, none repeated, all on the same date, none `CLOSED` or `CANCELLED` in `mto-maintenance`; a shift cannot be in two open possessions (`uq_possession_shift_open`); shifts are read through `MaintenanceClient` (synthetic ones while the client is off) |
| `close` | needs `all_clear` (every shift clear of track) unless `force`; `force` needs a `reason` and leaves `forced=true` only if someone was still on the track; a closed possession cannot be closed again; the close takes the same row lock as the command counter, so a command being issued finishes first or sees the possession closed; after commit it ends every open stream of the possession with `onCompleted` and sends the last board |
| `ends_at` | a `WindowChanged` issued through `IssueCommand` also updates the possession's `ends_at`: the `Welcome` of a later `Join` and the board carry the new window |

## Shifts of a possession (`possession_shift`)

One row per `MaintenanceShift` grouped: the `shift_id` of `mto-maintenance` and a snapshot of what
the board and the acknowledgements name (`shift_code`, `team_code`, `team_name`, `planned_end`).
`team_code` is also what the token is checked against on a `Join`: a technician may only join a
shift whose team code is among the groups of their token (`app.field.team-binding`), a supervisor
joins any (`04-grpc-api.md`, *Security*).
`open` copies the possession's state only for the partial unique index. `clear_of_track_at`, `_by`
and `_device` record the team's clear-of-track, with `earthing_removed`; it is written once
(`update ... where clear_of_track_at is null`), and `all_clear` is every shift having it.

## Commands (`field_command`)

What the server sends down a `TeamChannel`, append-only. `sequence` is **gapless per possession
and committed in order**: `next_command_seq` is incremented with `update ... returning` inside the
command's transaction, so the row lock holds until commit and a rollback leaves no hole. `kind`
`WINDOW_CHANGED`, `EVACUATE_NOW`, `SUPERVISOR_MESSAGE` (issued by the supervisor through
`IssueCommand`) or `EVENT_RESULT` (the asynchronous answer to an uploaded event). `target_shift_id`
`NULL` is a broadcast to every shift of the possession; an `EventResult` targets the shift of the
device that uploaded the event. `requires_ack` marks the commands whose acknowledgement the board
tracks (the evacuation order above all); an `EventResult` never requires one. `idempotency_key`,
only on the supervisor's commands: a retry of `IssueCommand` with the same key returns the same
`command_id` and `sequence` without a new row. `payload` is the whole `FieldCommand` as JSON, and
resumption resends it as stored.

## Acknowledgements (`command_ack`)

One per command and **team** (`shift_id`): any device of the team counts, and the first one wins
(`on conflict do nothing`). `acked_by` is the username of the token, `device_id` where it came
from, `accepted` with an optional `reason`; an acknowledgement with `accepted=false` still counts as the
team having answered (the record keeps why). An acknowledgement of a command that is not of that
possession, or does not require one, is answered in-band with `EventResult{REJECTED}`; the time
from issue to the first acknowledgement feeds `field.command.ack.time`.

The board summarises them per command (`CommandAckSummary`): `acked_by`, `pending` (no
acknowledgement yet), `sent_to` (pending, but written to a live stream: enqueued, not necessarily
delivered; a device that resumed declaring `last_command_sequence` at or past the command counts
too) and `queued_for` (pending and without a stream: it will arrive on resumption). A targeted
command counts only its shift.

## Events (`field_event`)

What a device uploads, append-only, unique per `(device_id, sequence)`: a duplicate is answered
with the stored outcome and applies nothing. `sequence` is monotonic per device and `> 0`; a
heartbeat travels with `0` and is not stored. `kind` `TASK_STARTED`, `TASK_COMPLETED`,
`COMMAND_ACK`, `CLEAR_OF_TRACK`; `occurred_at` is the device's clock, `received_at` the server's;
`reported_by` the username of the token; `payload` the whole `TeamMessage`.

`sync_status` says what happens with it next:

| Status | Meaning |
|---|---|
| `NOT_REQUIRED` | applied here (an acknowledgement, a clear-of-track) |
| `PENDING` | a task event waiting for the per-device work queue to pass it on to `mto-maintenance` |
| `SYNCED` | `mto-maintenance` recorded it (`synced_at`) |
| `FAILED` | `mto-maintenance` did not answer; retried after `next_attempt_at` (`sync_attempts`, `last_error`) |
| `REJECTED` | `mto-maintenance` said no; not retried |

Each uploaded event gets an `EventResult` back on the stream: `APPLIED`, `REJECTED` (with the
reason) or `PENDING_SYNC`. A task event is stored `PENDING` and queued; the per-device work queue
passes it on to `mto-maintenance` and answers `APPLIED` or `REJECTED` (with `<code>: <message>` of
`mto-maintenance`), or `PENDING_SYNC` when `mto-maintenance` did not answer, in which case the
event is `FAILED` and the retry answers when it resolves. A lost answer (`start` and `complete`
are not idempotent there) is reconciled by reading the task: a start with the task already in
progress or completed, and a completion with the task already completed, count as applied
(`CLAUDE.md`, *Synchronization with `mto-maintenance`*). With the client off every task event
stays `PENDING` and is answered `PENDING_SYNC`. A resent task event is answered from its stored
status: `PENDING`/`FAILED` → `PENDING_SYNC`, `SYNCED` → `APPLIED`, `REJECTED` → `REJECTED` with its
reason. `Welcome.last_applied_sequence` is the
**contiguous** watermark of the device's events, so a device knows what to resend after a cut.

What a device holds above that watermark is its **backlog**. Acknowledgements and clear-of-track
are resent on the `TeamChannel`; work events (`TASK_STARTED`, `TASK_COMPLETED`) are uploaded
through `SyncBufferedEvents`, in order, and while the device has a backlog a work event on the
`TeamChannel` that leaves a gap over the watermark is answered `REJECTED` (`BACKLOG_PENDING`) and
not stored, so the server never records the end of a task before its start. The sync answers with
the counts (`SyncResult`); the `EventResult` of each event still travels on the command sequence.

## Liveness

Kept in memory, per device and per JVM (`LivenessRegistry` and `TeamLiveness`), and shared with
the other replicas over the replica bus (`RemoteDeviceStates`, `06-messaging.md`): the last
message seen, the kp, battery and signal of the last heartbeat, and whether the stream is open. A
device heartbeats every 10 s. A team is `CONNECTED` when a stream is open and the silence is under
30 s, `STALE` between 30 and 60 s with the stream open, `DISCONNECTED` otherwise
(`app.field.liveness.stale-after`, `disconnected-after`); a team with several devices shows the
best of them. Transport keepalive (`spring.grpc.server.keepalive.*`) is a different thing: it
detects a dead TCP connection (in keepalive time + timeout, 30 s by default; grpc-java allows no
keepalive time under 10 s), it does not decide liveness; a stream it closes makes its device
`DISCONNECTED` at once, like a stream closed because its token expired.

## Board (`PossessionBoard`)

A snapshot per possession, recomputed when something changes (coalesced: N heartbeats are not N
recomputes) and every `app.field.board.tick` so the liveness decays: `version` (increasing),
`ends_at`, `all_clear`, one `TeamState` per shift (`team_code`, `liveness`, `last_seen`, `kp`,
`battery_pct`, `clear_of_track`) and one `CommandState` per command that requires an
acknowledgement. A watcher only ever sees the latest version; the version is seeded with the
clock, so a restart of the replica does not make it go backwards, and it is per replica: a watcher
always talks to one. The teams and the commands are the same from any replica (the database and
the replica bus); with the bus off a replica only knows the presence of its own devices.
