# 04 · gRPC API

Service `mto.field.v1.FieldService`, contract `src/main/proto/mto/field/v1/field_service.proto`
(generated into `com.alejandro.mtofield.grpc.v1`). Plaintext HTTP/2 on `9094` (host) / `9090`
(container); `mto-gateway` does not take part. Every call but health and reflection carries
`Authorization: Bearer <JWT>` in the metadata, a token of the `mto` realm with audience
`mto-field-api`. Fields may be added to the contract; the semantics do not change. Every RPC is
implemented but `SyncBufferedEvents`, which answers `UNIMPLEMENTED` until Phase 3.

## RPCs

| RPC | Kind | Request → response | Who |
|---|---|---|---|
| `OpenPossession` | unary | `OpenPossessionRequest{shift_ids[], ends_at?}` → `Possession` | supervisor |
| `ClosePossession` | unary | `ClosePossessionRequest{possession_id, force, reason}` → `Possession` | supervisor |
| `IssueCommand` | unary | `IssueCommandRequest{possession_id, idempotency_key, evacuate_now \| supervisor_message \| window_changed}` → `IssueCommandResponse{command_id, sequence}` | supervisor |
| `WatchPossessionBoard` | server streaming | `WatchPossessionBoardRequest{possession_id}` → `stream PossessionBoard` | supervisor |
| `TeamChannel` | bidirectional | `stream TeamMessage` → `stream FieldCommand` | device |
| `SyncBufferedEvents` | client streaming | `stream TeamMessage` → `SyncResult{last_applied_sequence, applied, duplicates}` | device (Phase 3) |

`Possession{id, code, shift_ids[], ends_at, status OPEN|CLOSED}`. Without `ends_at`,
`OpenPossession` takes the earliest `plannedEnd` of the shifts. `ClosePossession` without `force`
needs every team clear of track; with `force` it needs a `reason`. A `window_changed` through
`IssueCommand` also moves the possession's `ends_at`.

## `TeamChannel`

One stream per device, for the whole night. The first message **must** be a `Join{shift_id,
last_command_sequence}`; the first command back is a `Welcome` (`sequence 0`) with
`possession_id`, `server_time` (the device computes its clock offset), `ends_at` (the device runs
the countdown) and `last_applied_sequence`, the **contiguous** watermark of what the server has of
this device. The principal of the token and its expiry are captured when the stream opens; what
the device uploads is recorded under that username.

Upstream, `TeamMessage{device_id, sequence, occurred_at, event}`:

| Event | `sequence` | What the server does |
|---|---|---|
| `Heartbeat{kp, battery_pct, signal_dbm}` | `0` | updates liveness; not stored, not deduplicated |
| `CommandAck{command_id, accepted, reason}` | `> 0` | one acknowledgement per command and team (`accepted=false` still counts as answered); answered `EventResult{APPLIED}`, or `{REJECTED}` for an unknown command, one of another possession or one that requires no acknowledgement |
| `ClearOfTrack{earthing_removed}` | `> 0` | marks the team clear; answered in-band |
| `TaskStarted{order_id, task_id}`, `TaskCompleted{…, task_type_codes[], notes, work_complete, defects[], photo_refs[]}` | `> 0` | stored `PENDING` and queued for `mto-maintenance` (`POST /orders/{order_id}/tasks/{task_id}/start` with the shift and the person of the token; `.../complete` with the types, the notes, the inline defects and the photos); answered `EventResult{APPLIED}`, `{REJECTED, "<code>: <message>"}` (ids that are not UUIDs, or what `mto-maintenance` refuses: `TRN-001`, `SHF-001`, `MO-404`...) or `{PENDING_SYNC}` when `mto-maintenance` does not answer, followed by the final outcome when the retry resolves it (on the stream if open, on resumption otherwise) |
| a second `Join`, or an empty message | any | `EventResult{REJECTED}` (`already joined`, `empty message`) |

`kp` is a decimal string (`"34.271"`), never a double. `sequence` is monotonic per device; a
repeated one is answered with the stored outcome and applies nothing; a non-heartbeat with `0` is
rejected in-band.

Downstream, `FieldCommand{command_id, sequence, issued_at, requires_ack, command}` with `Welcome`,
`WindowChanged{ends_at}`, `EvacuateNow{reason, clear_by}`, `SupervisorMessage{author, text}` or
`EventResult{sequence, outcome APPLIED|REJECTED|PENDING_SYNC, reason}`.

### Resumption

A device that reconnects sends `Join.last_command_sequence` = the last `FieldCommand` it applied;
the server replays, in order, every later command of the possession that is a broadcast or targets
its shift, then goes live. A command is never skipped and never sent twice on the same stream.
**The sequence is gapless per possession, but a device sees a subset of it**: the `EventResult`s
targeted at other shifts take numbers too, so a gap in what a device receives is not a loss, and a
device must not infer one from it. The server guarantees it by construction (`CLAUDE.md`,
*Streams*).

A second `TeamChannel` with the same `device_id` supersedes the first, which ends with `ABORTED`
(`superseded`). When the possession closes, every stream ends with `onCompleted`.

### In-band errors

Inside a stream a `Status` closes it, so a rule that rejects one message is answered as
`EventResult{REJECTED, reason}` and the stream goes on. What closes the stream: a first message
that is not a `Join` or has no `device_id` (`FAILED_PRECONDITION`, reason `JOIN_REQUIRED`), a
shift id that is not a UUID (`INVALID_ARGUMENT`, `INVALID_SHIFT_ID`), a shift that is in no open
possession (`FAILED_PRECONDITION`, `SHIFT_NOT_IN_OPEN_POSSESSION`), an outbound queue that
overflowed (`RESOURCE_EXHAUSTED`, `OUTBOUND_QUEUE_FULL`) or a device that does not read during
catch-up (`RESOURCE_EXHAUSTED`, `DEVICE_NOT_READING`: the commands are in the database and a
resumption recovers them), a full work queue (`RESOURCE_EXHAUSTED`, `WORK_QUEUE_FULL`: the event
is stored and the watermark covers it), a possession that closed while a message was being
processed (`FAILED_PRECONDITION`, `POSSESSION_CLOSED`; the normal case is `onCompleted`, below)
and a server shutting down during catch-up (`UNAVAILABLE`, `SHUTTING_DOWN`). The device's own
half-close is answered with `onCompleted`.

## `WatchPossessionBoard`

A conflated board: only the latest `PossessionBoard{version, ends_at, all_clear, teams[],
commands[]}` matters, a slow watcher skips versions and the server keeps no queue for it.
`TeamState{shift_id, team_code, liveness CONNECTED|STALE|DISCONNECTED, last_seen, kp,
battery_pct, clear_of_track}`; `CommandState{command_id, kind, issued_at, acked_by[], pending[],
sent_to[], queued_for[]}` (team codes; `sent_to` is written to a live stream, not necessarily
delivered, or declared applied by a resumed device; `queued_for` has no stream and will receive it
on resumption). The watcher is registered before the first board is computed and sent, so nothing
published in between is missed; a request on an unknown possession is `NOT_FOUND`. On a closed
possession the last board is sent and the stream completes.

## Idempotency

- Downstream: `IssueCommand` with an `idempotency_key` (unique per possession) returns, on a retry,
  the same `command_id` and `sequence` and creates nothing: a retried evacuation is not two.
- Upstream: `(device_id, sequence)` is unique; a duplicate gets the stored `EventResult`.

## Status codes

| Code | When |
|---|---|
| `UNAUTHENTICATED` | no token; expired, badly signed, wrong issuer or wrong audience |
| `PERMISSION_DENIED` | valid token without the role of the RPC |
| `INVALID_ARGUMENT` | a bad request: no shifts, a repeated shift, shifts on different dates, no shift with a planned end and no `ends_at` (`INVALID_POSSESSION_REQUEST`); an unparseable id, no command in `IssueCommand`, a `window_changed` without `ends_at`, a `force` without a reason (`INVALID_ARGUMENT`); `Join.shift_id` not a UUID (`INVALID_SHIFT_ID`) |
| `NOT_FOUND` | unknown possession (`POSSESSION_NOT_FOUND`) |
| `FAILED_PRECONDITION` | first message not a `Join` (`JOIN_REQUIRED`); shift in no open possession (`SHIFT_NOT_IN_OPEN_POSSESSION`); shift already in an open possession (`SHIFT_ALREADY_IN_OPEN_POSSESSION`); the possession is not open (`POSSESSION_NOT_OPEN`, also closing twice); close without `force` when not everyone is clear (`POSSESSION_NOT_ALL_CLEAR`, metadata `pending_teams`, comma-separated team codes); a shift `mto-maintenance` already finished (`SHIFT_NOT_WORKABLE`, metadata `shift_id` and `status`); `mto-maintenance` refused a request of the call with a business 4xx (`MAINTENANCE_REJECTED`, metadata `maintenance_status` and `maintenance_error_code`); the possession closed under a stream (`POSSESSION_CLOSED`) |
| `UNAVAILABLE` | the server shutting down during a catch-up (`SHUTTING_DOWN`); `mto-maintenance` not answering while opening a possession (`MAINTENANCE_UNAVAILABLE`: network, timeout, 5xx, circuit open, the service account refused) |
| `RESOURCE_EXHAUSTED` | a stream whose outbound queue overflowed (`OUTBOUND_QUEUE_FULL`), a device that does not read during catch-up (`DEVICE_NOT_READING`), a full work queue (`WORK_QUEUE_FULL`); resumption recovers |
| `ABORTED` | `SUPERSEDED`: a newer stream of the same device registered |
| `UNIMPLEMENTED` | `SyncBufferedEvents` until Phase 3 |

What closes a call carries a `google.rpc.ErrorInfo` (`reason` in capitals above, `domain`
`mto-field`, `metadata`) in the status details (`grpc-status-details-bin`), built with
`StatusProto` by `GrpcErrors` and, for the unary calls, by the `@GrpcAdvice`
`FieldGrpcExceptionAdvice`. `UNAUTHENTICATED` and `PERMISSION_DENIED` come from Spring Security's
interceptor and carry no `ErrorInfo`.

## Security

| RPC | Role (`@PreAuthorize`) |
|---|---|
| `TeamChannel`, `SyncBufferedEvents` | `FIELD_TEAM` (`field-team`) |
| `OpenPossession`, `ClosePossession`, `IssueCommand`, `WatchPossessionBoard` | `FIELD_SUPERVISE` (`field-supervise`) |
| `grpc.health.v1.Health/*`, `grpc.reflection.v1.ServerReflection/*`, `grpc.reflection.v1alpha.ServerReflection/*` | none (reflection off in `prod`) |

The permission of a stream is checked when it is opened, like a unary call. Who holds what:
`mto-field-technician` → `field-team`; `mto-field-supervisor` → `field-team` + `field-supervise`.
