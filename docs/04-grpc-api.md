# 04 · gRPC API

Service `mto.field.v1.FieldService`, contract `src/main/proto/mto/field/v1/field_service.proto`
(generated into `com.alejandro.mtofield.grpc.v1`). Plaintext HTTP/2 on `9094` (host) / `9090`
(container); `mto-gateway` does not take part. Every call but health and reflection carries
`Authorization: Bearer <JWT>` in the metadata, a token of the `mto` realm with audience
`mto-field-api`. Fields may be added to the contract; the semantics do not change. Every RPC is
implemented.

## RPCs

| RPC | Kind | Request → response | Who |
|---|---|---|---|
| `OpenPossession` | unary | `OpenPossessionRequest{shift_ids[], ends_at?}` → `Possession` | supervisor |
| `ClosePossession` | unary | `ClosePossessionRequest{possession_id, force, reason}` → `Possession` | supervisor |
| `IssueCommand` | unary | `IssueCommandRequest{possession_id, idempotency_key, evacuate_now \| supervisor_message \| window_changed}` → `IssueCommandResponse{command_id, sequence}` | supervisor |
| `WatchPossessionBoard` | server streaming | `WatchPossessionBoardRequest{possession_id}` → `stream PossessionBoard` | supervisor |
| `TeamChannel` | bidirectional | `stream TeamMessage` → `stream FieldCommand` | device |
| `SyncBufferedEvents` | client streaming | `stream TeamMessage` → `SyncResult{last_applied_sequence, applied, duplicates, rejected}` | device |

`Possession{id, code, shift_ids[], ends_at, status OPEN|CLOSED}`. Without `ends_at`,
`OpenPossession` takes the earliest `plannedEnd` of the shifts. `ClosePossession` without `force`
needs every team clear of track; with `force` it needs a `reason`. A `window_changed` through
`IssueCommand` also moves the possession's `ends_at`.

## `TeamChannel`

One stream per device, for the whole night. The first message **must** be a `Join{shift_id,
last_command_sequence}`; the first command back is a `Welcome` (`sequence 0`) with
`possession_id`, `server_time` (the device computes its clock offset), `ends_at` (the device runs
the countdown) and `last_applied_sequence`, the **contiguous** watermark of what the server has of
this device. The principal of the token, its expiry and its groups are captured when the stream
opens; what the device uploads is recorded under that username. With `app.field.team-binding`
on (the default) the `Join` is refused with `PERMISSION_DENIED` (`TEAM_NOT_ALLOWED`, metadata
`team_code`) unless the shift's team code is among the token's groups or the token carries
`field-supervise` (below, *Security*).

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
(`superseded`). When the possession closes, every stream ends with `onCompleted`. A stream whose
token expires while open ends with `UNAUTHENTICATED` (`TOKEN_EXPIRED`, metadata `expired_at`): the
JWT is only validated when the call opens, so a sweep every `app.field.token-expiry.sweep` (30 s)
closes what has expired, and the device reconnects with a fresh token and its
`last_command_sequence`, losing nothing.

### The backlog

`Welcome.last_applied_sequence` is the contiguous watermark of the device's events. What the
device holds above it is its **backlog**, and a backlog of work goes through `SyncBufferedEvents`
(below), in order: a `TaskStarted` or `TaskCompleted` sent on the `TeamChannel` whose sequence
leaves a gap over the watermark (`sequence > watermark + 1`) is answered
`EventResult{REJECTED, "BACKLOG_PENDING: …"}` and not stored. Acknowledgements and clear-of-track
are applied whatever the gap: they are what the supervisor is waiting for, and they must not queue
behind a night of task events. A resend in order after a cut is `watermark + 1` and goes through.

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
processed (`FAILED_PRECONDITION`, `POSSESSION_CLOSED`; the normal case is `onCompleted`, below),
a server shutting down during catch-up (`UNAVAILABLE`, `SHUTTING_DOWN`), a `Join` of a team the
token does not belong to (`PERMISSION_DENIED`, `TEAM_NOT_ALLOWED`) and a token that expired under
the stream (`UNAUTHENTICATED`, `TOKEN_EXPIRED`). The device's own half-close is answered with
`onCompleted`.

## `SyncBufferedEvents`

The backlog a device accumulated without coverage, uploaded outside the live channel. The device
opens the client stream, sends a `Join{shift_id}` with its `device_id` first (the same checks as
the `TeamChannel`: `JOIN_REQUIRED`, `INVALID_SHIFT_ID`, `SHIFT_NOT_IN_OPEN_POSSESSION`,
`TEAM_NOT_ALLOWED`), then its events in **strictly increasing** `sequence` (a repeated or lower
number closes the stream with `INVALID_ARGUMENT`, `OUT_OF_ORDER`), and half-closes. The server
applies each event as the `TeamChannel` would: an acknowledgement or a clear-of-track inline, a
task event stored `PENDING` and queued for `mto-maintenance` (a full work queue closes the stream
with `RESOURCE_EXHAUSTED`, `WORK_QUEUE_FULL`; what was stored stays stored); a heartbeat is
ignored, and a second `Join`, an empty message, a `sequence 0` or a business rejection count as
`rejected`. The answer is one `SyncResult{last_applied_sequence, applied, duplicates, rejected}`:
the contiguous watermark after the upload, what was stored now, what was already there, what was
not stored. The `EventResult` of each event (the final outcome of a task event among them) still
travels on the possession's command sequence, to the `TeamChannel` if it is open and on resumption
otherwise: the `SyncResult` counts, it does not answer event by event. A device that keeps a
`TeamChannel` open while it syncs sends only heartbeats, acknowledgements and clear-of-track on it
and buffers new work events until the `SyncResult` arrives, so that the server never sees a gap.

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
| `UNAUTHENTICATED` | no token; expired, badly signed, wrong issuer or wrong audience; a stream or a board watcher whose token expired while open (`TOKEN_EXPIRED`, metadata `expired_at`, from the sweep every `app.field.token-expiry.sweep`) |
| `PERMISSION_DENIED` | valid token without the role of the RPC; a `Join` on a shift whose team is not among the token's groups (`TEAM_NOT_ALLOWED`, metadata `team_code`; `field-supervise` joins any team; off with `app.field.team-binding.enabled=false`) |
| `INVALID_ARGUMENT` | a bad request: no shifts, a repeated shift, shifts on different dates, no shift with a planned end and no `ends_at` (`INVALID_POSSESSION_REQUEST`); an unparseable id, no command in `IssueCommand`, a `window_changed` without `ends_at`, a `force` without a reason (`INVALID_ARGUMENT`); `Join.shift_id` not a UUID (`INVALID_SHIFT_ID`); a `SyncBufferedEvents` sequence not above the previous one of the stream (`OUT_OF_ORDER`) |
| `NOT_FOUND` | unknown possession (`POSSESSION_NOT_FOUND`) |
| `FAILED_PRECONDITION` | first message not a `Join` (`JOIN_REQUIRED`); shift in no open possession (`SHIFT_NOT_IN_OPEN_POSSESSION`); shift already in an open possession (`SHIFT_ALREADY_IN_OPEN_POSSESSION`); the possession is not open (`POSSESSION_NOT_OPEN`, also closing twice); close without `force` when not everyone is clear (`POSSESSION_NOT_ALL_CLEAR`, metadata `pending_teams`, comma-separated team codes); a shift `mto-maintenance` already finished (`SHIFT_NOT_WORKABLE`, metadata `shift_id` and `status`); `mto-maintenance` refused a request of the call with a business 4xx (`MAINTENANCE_REJECTED`, metadata `maintenance_status` and `maintenance_error_code`); the possession closed under a stream (`POSSESSION_CLOSED`) |
| `UNAVAILABLE` | the server shutting down during a catch-up (`SHUTTING_DOWN`); `mto-maintenance` not answering while opening a possession (`MAINTENANCE_UNAVAILABLE`: network, timeout, 5xx, circuit open, the service account refused) |
| `RESOURCE_EXHAUSTED` | a stream whose outbound queue overflowed (`OUTBOUND_QUEUE_FULL`), a device that does not read during catch-up (`DEVICE_NOT_READING`), a full work queue (`WORK_QUEUE_FULL`, on either stream); resumption recovers |
| `ABORTED` | `SUPERSEDED`: a newer stream of the same device registered |

What closes a call carries a `google.rpc.ErrorInfo` (`reason` in capitals above, `domain`
`mto-field`, `metadata`) in the status details (`grpc-status-details-bin`), built with
`StatusProto` by `GrpcErrors` and, for the unary calls, by the `@GrpcAdvice`
`FieldGrpcExceptionAdvice`. `UNAUTHENTICATED` and `PERMISSION_DENIED` come from Spring Security's
interceptor and carry no `ErrorInfo`, except `TOKEN_EXPIRED` and `TEAM_NOT_ALLOWED`, which the
service raises itself with theirs.

## Security

| RPC | Role (`@PreAuthorize`) |
|---|---|
| `TeamChannel`, `SyncBufferedEvents` | `FIELD_TEAM` (`field-team`) |
| `OpenPossession`, `ClosePossession`, `IssueCommand`, `WatchPossessionBoard` | `FIELD_SUPERVISE` (`field-supervise`) |
| `grpc.health.v1.Health/*`, `grpc.reflection.v1.ServerReflection/*`, `grpc.reflection.v1alpha.ServerReflection/*` | none (reflection off in `prod`) |

The permission of a stream is checked when it is opened, like a unary call. Who holds what:
`mto-field-technician` → `field-team`; `mto-field-supervisor` → `field-team` + `field-supervise`.

The role says what a token may do; the **team** says for whom. The realm's login client
(`mto-frontend` in `mto-platform`) carries a group-membership mapper that puts the person's
groups in the access token as `groups` (names only, no path), a group is named after the `code`
of the team in `mto-maintenance` (`EQ-NORTE`, `EQ-SUR` in the partial import, one development
technician in each), and a `Join` on either stream is refused with `PERMISSION_DENIED`
(`TEAM_NOT_ALLOWED`, metadata `team_code`) unless the shift's team code is among them or the token
carries `field-supervise` (the supervisor acts for any team). `app.field.team-binding.enabled`
(`true`) and `.claim` (`groups`) configure it; the `test` profile runs with it off.

## Transport: keepalive and limits

Transport keepalive detects a dead TCP connection; the application heartbeat (`Heartbeat` every
10 s) decides whether a team is `CONNECTED`, `STALE` or `DISCONNECTED`. The two are tuned
together (`spring.grpc.server.keepalive.*`, `SPRING_GRPC_SERVER_KEEPALIVE_*`):

| Setting | Value | Why |
|---|---|---|
| `keepalive.time` / `keepalive.timeout` | `20s` / `10s` | The server pings after 20 s without data and closes the connection if the ping is not answered in 10 s: a dead connection is noticed in 30 s, the stream ends, liveness decays and resumption recovers. grpc-java raises any keepalive time under 10 s to 10 s, on the server and on the client |
| `keepalive.permit.time` / `permit.without-calls` | `10s` / `true` | What the server tolerates from a pinging client; it must not exceed the client's interval (the simulator pings every 20 s, grpc-java clients never under 10 s) or the server answers `GOAWAY ENHANCE_YOUR_CALM` (`too_many_pings`) |
| `keepalive.connection.max-idle-time` | `5m` | A connection without calls (the console between two unary calls) is closed with `GOAWAY` and the client reconnects on its own |
| `app.field.liveness.stale-after` / `disconnected-after` | `30s` / `60s` | Three missed heartbeats make a team `STALE`, six `DISCONNECTED`; a closed stream is `DISCONNECTED` at once |
| `inbound.message.max-size` | `4 MiB` | Messages are chunked (photos travel as references), the limit is not raised |
| `inbound.metadata.max-size` | `8 KiB` | Where the JWT travels. The worst-case token of the realm (every client role of the seven APIs, every profile, the groups, Keycloak's standard claims) measures 3381 bytes of JWT and 3945 bytes of HTTP/2 metadata, under half the limit; `SecurityLayerTest` mints it and prints the figure, and a realm that grows its claims should watch it |

`NetworkResilienceIT` exercises them with a Toxiproxy between the device and the server: the cut
mid-evacuation, a slow and narrow network, a peer that goes mute without closing (detected in
keepalive time + timeout) and a client pinging within the permitted rate.
