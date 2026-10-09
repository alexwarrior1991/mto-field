# 04 · gRPC API

Service `mto.field.v1.FieldService`, contract `src/main/proto/mto/field/v1/field_service.proto`
(generated into `com.alejandro.mtofield.grpc.v1`). Plaintext HTTP/2 on `9094` (host) / `9090`
(container); `mto-gateway` does not take part. Every call but health and reflection carries
`Authorization: Bearer <JWT>` in the metadata, a token of the `mto` realm with audience
`mto-field-api`. Fields may be added to the contract; the semantics do not change. **Phase 0:
every RPC answers `UNIMPLEMENTED`**; what follows is the contract Phase 1 implements
(`SyncBufferedEvents` in Phase 3).

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
needs every team clear of track; with `force` it needs a `reason`.

## `TeamChannel`

One stream per device, for the whole night. The first message **must** be a `Join{shift_id,
last_command_sequence}`; the first command back is a `Welcome` (`sequence 0`) with
`possession_id`, `server_time` (the device computes its clock offset), `ends_at` (the device runs
the countdown) and `last_applied_sequence`, the **contiguous** watermark of what the server has of
this device.

Upstream, `TeamMessage{device_id, sequence, occurred_at, event}`:

| Event | `sequence` | What the server does |
|---|---|---|
| `Heartbeat{kp, battery_pct, signal_dbm}` | `0` | updates liveness; not stored, not deduplicated |
| `CommandAck{command_id, accepted, reason}` | `> 0` | one acknowledgement per command and team; answered `EventResult{APPLIED}` or `{REJECTED}` |
| `ClearOfTrack{earthing_removed}` | `> 0` | marks the team clear; answered in-band |
| `TaskStarted{order_id, task_id}`, `TaskCompleted{…, task_type_codes[], notes, work_complete, defects[], photo_refs[]}` | `> 0` | stored `PENDING` and queued for `mto-maintenance`; answered `EventResult{PENDING_SYNC}`, then the final outcome when it is known |

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
shift that is in no open possession (`FAILED_PRECONDITION`), an outbound queue that overflowed or
a device that does not read during catch-up (`RESOURCE_EXHAUSTED`: the commands are in the
database and a resumption recovers them), a full work queue (`RESOURCE_EXHAUSTED`: the event is
stored and the watermark covers it).

## `WatchPossessionBoard`

A conflated board: only the latest `PossessionBoard{version, ends_at, all_clear, teams[],
commands[]}` matters, a slow watcher skips versions and the server keeps no queue for it.
`TeamState{shift_id, team_code, liveness CONNECTED|STALE|DISCONNECTED, last_seen, kp,
battery_pct, clear_of_track}`; `CommandState{command_id, kind, issued_at, acked_by[], pending[],
sent_to[], queued_for[]}` (team codes; `sent_to` is written to a live stream, not necessarily
delivered; `queued_for` has no stream and will receive it on resumption). On a closed possession
the last board is sent and the stream completes.

## Idempotency

- Downstream: `IssueCommand` with an `idempotency_key` (unique per possession) returns, on a retry,
  the same `command_id` and `sequence` and creates nothing: a retried evacuation is not two.
- Upstream: `(device_id, sequence)` is unique; a duplicate gets the stored `EventResult`.

## Status codes

| Code | When |
|---|---|
| `UNAUTHENTICATED` | no token; expired, badly signed, wrong issuer or wrong audience |
| `PERMISSION_DENIED` | valid token without the role of the RPC |
| `INVALID_ARGUMENT` | a bad request (no shifts, an unparseable id, a `force` without a reason…) |
| `NOT_FOUND` | unknown possession |
| `FAILED_PRECONDITION` | first message not a `Join` (`JOIN_REQUIRED`); shift in no open possession; shift already in an open possession; the possession is not open; close without `force` when not everyone is clear (metadata `pending_teams`); `mto-maintenance` rejected the shift |
| `UNAVAILABLE` | `mto-maintenance` down (Phase 2) |
| `RESOURCE_EXHAUSTED` | a stream whose outbound queue overflowed, a device that does not read during catch-up, a full work queue; resumption recovers |
| `ABORTED` | `superseded`: a newer stream of the same device registered |
| `UNIMPLEMENTED` | everything in Phase 0; `SyncBufferedEvents` until Phase 3 |

What closes a call carries a `google.rpc.ErrorInfo` (`reason`, `domain` `mto-field`, `metadata`)
in the status details, built with `StatusProto` by a `@GrpcAdvice` (Phase 1).

## Security

| RPC | Role (`@PreAuthorize`) |
|---|---|
| `TeamChannel`, `SyncBufferedEvents` | `FIELD_TEAM` (`field-team`) |
| `OpenPossession`, `ClosePossession`, `IssueCommand`, `WatchPossessionBoard` | `FIELD_SUPERVISE` (`field-supervise`) |
| `grpc.health.v1.Health/*`, `grpc.reflection.v1.ServerReflection/*`, `grpc.reflection.v1alpha.ServerReflection/*` | none (reflection off in `prod`) |

The permission of a stream is checked when it is opened, like a unary call. Who holds what:
`mto-field-technician` → `field-team`; `mto-field-supervisor` → `field-team` + `field-supervise`.
