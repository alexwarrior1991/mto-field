# `grpcurl` cookbook

How to talk to `mto-field` by hand against the local environment
(`cd ../mto-platform && docker compose --profile all up -d && ./keycloak/apply-partials.sh`, or
`./mvnw spring-boot:run` here). The gRPC port on the host is **9094**, plaintext (the server has
no TLS configured): `mto-gateway` does not proxy gRPC. Every RPC works.

`grpcurl` reads the contract through server reflection, which is on in `dev` and **off in `prod`**
(`SPRING_GRPC_SERVER_REFLECTION_ENABLED`). Against an environment without reflection, give it the
`.proto` instead: `-import-path src/main/proto -proto mto/field/v1/field_service.proto`.

## Health, `list` and `describe` (no token)

```bash
grpcurl -plaintext localhost:9094 grpc.health.v1.Health/Check          # {"status": "SERVING"}
grpcurl -plaintext localhost:9094 list                                  # grpc.health.v1.Health, grpc.reflection..., mto.field.v1.FieldService
grpcurl -plaintext localhost:9094 list mto.field.v1.FieldService        # the six RPCs
grpcurl -plaintext localhost:9094 describe mto.field.v1.FieldService
grpcurl -plaintext localhost:9094 describe mto.field.v1.TeamMessage
```

## A token

The local realm leaves the password grant of the client `mto-frontend` open, and its tokens carry
`aud: mto-field-api`. Dev users: `campo.tecnico1`, `campo.tecnico2` (`mto-field-technician`,
`field-team`) and `campo.responsable` (`mto-field-supervisor`, `field-team` + `field-supervise`),
password `local`.

```bash
token() {
  curl -s -X POST http://auth.mto.local:8082/realms/mto/protocol/openid-connect/token \
    -d grant_type=password -d client_id=mto-frontend \
    -d username="$1" -d password=local | jq -r .access_token
}
SUPERVISOR=$(token campo.responsable)
TEAM=$(token campo.tecnico1)
```

`auth.mto.local` must resolve to the host (`/etc/hosts`): the token `iss` carries that name. A
call without the header is `UNAUTHENTICATED`; with the technician's token on a supervisor RPC,
`PERMISSION_DENIED`.

```bash
grpcurl -plaintext localhost:9094 mto.field.v1.FieldService/OpenPossession -d '{}'
# ERROR: Code: Unauthenticated
```

Field names in `-d` can be the proto names (`shift_ids`) or their JSON names (`shiftIds`);
timestamps are RFC 3339 strings; a `oneof` is just the chosen field.

## Open a possession (supervisor)

The shifts are read from `mto-maintenance` (`GET /api/v1/maintenance/shifts/{id}` with the
service account): they must exist, be on the same date and not be `CLOSED` or `CANCELLED`, and the
team code of the board is the one of the shift's team. With `app.maintenance.enabled=false` any
UUID is a valid shift: the `NoOp` client answers a synthetic shift whose team is `T-` plus the
first four hex digits of the id. Without `ends_at` the possession takes the earliest planned end of
the shifts.

```bash
grpcurl -plaintext -H "Authorization: Bearer $SUPERVISOR" localhost:9094 \
  mto.field.v1.FieldService/OpenPossession \
  -d '{"shift_ids": ["11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"],
       "ends_at": "2026-10-10T05:00:00Z"}'
# {"id": "…", "code": "PO-000001", "shiftIds": [...], "endsAt": "…", "status": "OPEN"}
POSSESSION=<the id>
```

A shift already in another open possession is `FAILED_PRECONDITION`; no shifts, a bad id, a shift
`mto-maintenance` does not know or a different date between shifts, `INVALID_ARGUMENT`;
`mto-maintenance` not answering, `UNAVAILABLE` (`MAINTENANCE_UNAVAILABLE`).

## Join as a device (team channel)

`TeamChannel` is bidirectional: `grpcurl -d @` reads one JSON message after another from stdin and
prints every `FieldCommand` that comes back. The first message **must** be a `Join`; the first
command back is the `Welcome` (`sequence 0`), with the contiguous watermark of what the server
already holds of this device. Keep the stream open in a terminal of its own:

```bash
grpcurl -plaintext -H "Authorization: Bearer $TEAM" -d @ localhost:9094 \
  mto.field.v1.FieldService/TeamChannel <<EOF
{"device_id": "dev-1", "sequence": 0, "join": {"shift_id": "11111111-1111-4111-8111-111111111111", "last_command_sequence": 0}}
{"device_id": "dev-1", "sequence": 0, "occurred_at": "2026-10-09T22:00:00Z", "heartbeat": {"kp": "34.271", "battery_pct": 80, "signal_dbm": -70}}
{"device_id": "dev-1", "sequence": 1, "occurred_at": "2026-10-09T22:01:00Z", "task_started": {"order_id": "…", "task_id": "…"}}
EOF
```

A heartbeat travels with `sequence 0` and is neither stored nor deduplicated; every other event
takes the next number of the device, and a repeated number is answered with the stored outcome.
With stdin closed `grpcurl` half-closes the call; to keep it open and type the messages by hand,
run it without the heredoc and paste one JSON object per message. A first message that is not a
`Join` closes the stream with `FAILED_PRECONDITION` (`JOIN_REQUIRED`); a second stream with the
same `device_id` ends the first with `ABORTED` (`superseded`).

## Issue a command (supervisor)

```bash
grpcurl -plaintext -H "Authorization: Bearer $SUPERVISOR" localhost:9094 \
  mto.field.v1.FieldService/IssueCommand \
  -d '{"possession_id": "'"$POSSESSION"'", "idempotency_key": "evac-1",
       "evacuate_now": {"reason": "train approaching", "clear_by": "2026-10-09T23:10:00Z"}}'
# {"commandId": "…", "sequence": "1"}
```

Every open `TeamChannel` of the possession prints the `EvacuateNow` with `requiresAck: true`.
Repeating the call with the same `idempotency_key` returns the same `commandId` and `sequence` and
nobody receives a copy. A `supervisor_message{author, text}` or a `window_changed{ends_at}` go the
same way.

## Acknowledge, and clear the track (device)

On the device's stream, after the command arrived:

```json
{"device_id": "dev-1", "sequence": 2, "occurred_at": "2026-10-09T23:01:00Z", "command_ack": {"command_id": "<commandId>", "accepted": true}}
{"device_id": "dev-1", "sequence": 3, "occurred_at": "2026-10-09T23:05:00Z", "clear_of_track": {"earthing_removed": true}}
```

Each one is answered with an `EventResult{sequence, outcome}` on the same stream: `APPLIED`, or
`REJECTED` with a `reason` (an unknown command, a command of another possession, a non-heartbeat
with `sequence 0`). The stream stays open: inside it, errors travel in-band.

## Watch the board (supervisor)

```bash
grpcurl -plaintext -H "Authorization: Bearer $SUPERVISOR" localhost:9094 \
  mto.field.v1.FieldService/WatchPossessionBoard -d '{"possession_id": "'"$POSSESSION"'"}'
```

One `PossessionBoard` per change (conflated: only the latest version is ever sent), and one every
`app.field.board.tick` so the liveness decays: `teams[]` with `liveness` (`CONNECTED` under 30 s
of silence with the stream open, `STALE` up to 60 s, `DISCONNECTED` otherwise), `lastSeen`, `kp`,
`batteryPct`, `clearOfTrack`; `commands[]` with `ackedBy`, `pending`, `sentTo` and `queuedFor` by
team code; `allClear` once every team has cleared. On a closed possession the last board is sent
and the stream completes.

## Resume after a cut (device)

Reconnect with the last command applied; the server replays what the device missed (broadcasts
and the commands of its shift), in order and once, then goes live:

```json
{"device_id": "dev-1", "sequence": 0, "join": {"shift_id": "11111111-1111-4111-8111-111111111111", "last_command_sequence": 1}}
```

The `Welcome.lastAppliedSequence` says which of the device's own events the server has
contiguously, so the device knows what to resend. The numbers a device receives may have gaps:
the `EventResult`s of other shifts take numbers of the same possession sequence, and a gap is not
a loss.

With the token's team not matching the shift's (`app.field.team-binding`), the `Join` is refused
with `PERMISSION_DENIED` (`TEAM_NOT_ALLOWED`); `campo.tecnico1` belongs to `EQ-NORTE` and
`campo.tecnico2` to `EQ-SUR`, and `campo.responsable` joins any team. A token that expires under
the stream ends it with `UNAUTHENTICATED` (`TOKEN_EXPIRED`): ask for a new one and resume the
same way.

## Upload the backlog (device)

After a cut, resend on the `TeamChannel` the acks and clear-of-track the `Welcome` says the
server lacks, and upload the work events it lacks through `SyncBufferedEvents`, in strictly
increasing sequence, a `Join` first: a work event with a gap sent on the `TeamChannel` instead is
answered `EventResult{REJECTED, "BACKLOG_PENDING: …"}`. With the heredoc closed, `grpcurl`
half-closes the call and prints the `SyncResult`:

```bash
grpcurl -plaintext -H "Authorization: Bearer $TEAM" -d @ localhost:9094 \
  mto.field.v1.FieldService/SyncBufferedEvents <<EOF
{"device_id": "dev-1", "sequence": 0, "join": {"shift_id": "11111111-1111-4111-8111-111111111111"}}
{"device_id": "dev-1", "sequence": 2, "occurred_at": "2026-10-09T22:10:00Z", "task_started": {"order_id": "…", "task_id": "…"}}
{"device_id": "dev-1", "sequence": 3, "occurred_at": "2026-10-09T22:40:00Z", "task_completed": {"order_id": "…", "task_id": "…", "work_complete": true}}
EOF
```

```json
{"lastAppliedSequence": "3", "applied": 2}
```

`applied` is what was stored now, `duplicates` what the server already had, `rejected` what was
not stored (a repeated `Join`, an empty message, a `sequence 0`); the `EventResult` of each event
(the final outcome of a task once `mto-maintenance` answers) still arrives on the `TeamChannel`, or
on the next resumption. A sequence not above the previous one closes the stream with
`INVALID_ARGUMENT` (`OUT_OF_ORDER`).

## Close the possession (supervisor)

```bash
grpcurl -plaintext -H "Authorization: Bearer $SUPERVISOR" localhost:9094 \
  mto.field.v1.FieldService/ClosePossession -d '{"possession_id": "'"$POSSESSION"'"}'
```

With a team not yet clear of track, `FAILED_PRECONDITION` with the `pending_teams` in the
`ErrorInfo` metadata (`grpcurl` prints the status details); `{"force": true, "reason": "…"}` closes
anyway and records it. Every open stream of the possession ends with a normal completion, and the
board watchers receive the last board.

## The simulator

Instead of typing messages, `src/test/java/com/alejandro/mtofield/simulator` (no Spring) plays
both sides against the same port:

```bash
# the whole night in one JVM: three teams, the third never acknowledges, a cut every 15 s
./mvnw -q test-compile exec:java -Dexec.classpathScope=test \
  -Dexec.args="--mode demo --target localhost:9094 --user campo.responsable --password local --teams 3 --never-ack-team 3 --cut-every 15s --mixed"

# or split: the supervisor prints the shift ids, the devices join them from another terminal
./mvnw -q test-compile exec:java -Dexec.classpathScope=test -Dexec.args="--mode supervisor --user campo.responsable --password local --teams 2"
./mvnw -q test-compile exec:java -Dexec.classpathScope=test -Dexec.args="--mode device --user campo.tecnico1 --password local --shifts <id>,<id> --cut-every 20s --blocking"
```

`demo` opens a possession, runs one device per team (`--devices-per-team`), orders the evacuation
at `--evacuate-after` and closes at `--duration`, forced if someone is still on the track.
`--never-ack-team N` is the team that ignores the evacuation; `--cut-every` makes every device
lose coverage and resume with its last applied command, resending only what the `Welcome` says the
server does not have; `--blocking` uses the blocking v2 stub on two virtual threads instead of the
async observer with `onReady`, and `--mixed` alternates both. Without Keycloak, `--local-issuer`
serves the JWK Set of the test key on `localhost:8082` and mints the tokens; the server is started
with `KEYCLOAK_ISSUER_URI=http://localhost:8082/realms/mto` (`README.md`); each token then carries
the team of its shift in `groups` (`--team-codes` with real shifts) and lasts `--token-ttl`, so a
short one shows the close by expiry and the renewal. After a cut a device resends its acks and
clear-of-track on the channel and uploads its work events through `SyncBufferedEvents`, buffering
the new ones until the `SyncResult` comes back. `--help` lists every option. A duplicate or
out-of-order command on any device makes the process exit with `1`.
