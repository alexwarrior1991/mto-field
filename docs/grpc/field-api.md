# `grpcurl` cookbook

How to talk to `mto-field` by hand against the local environment
(`cd ../mto-platform && docker compose --profile all up -d && ./keycloak/apply-partials.sh`, or
`./mvnw spring-boot:run` here). The gRPC port on the host is **9094**, plaintext (the server has
no TLS configured): `mto-gateway` does not proxy gRPC. **Phase 0: every RPC of
`FieldService` answers `UNIMPLEMENTED`**; the health check, `list` and `describe` already work, and
the rest of this page is what the calls do from Phase 1.

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

With `app.maintenance.enabled=false` (the `dev` default in Phase 1) any UUID is a valid shift: the
`NoOp` client answers a synthetic shift whose team is `T-` plus the first four hex digits of the
id. Without `ends_at` the possession takes the earliest planned end of the shifts.

```bash
grpcurl -plaintext -H "Authorization: Bearer $SUPERVISOR" localhost:9094 \
  mto.field.v1.FieldService/OpenPossession \
  -d '{"shift_ids": ["11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"],
       "ends_at": "2026-10-10T05:00:00Z"}'
# {"id": "…", "code": "PO-000001", "shiftIds": [...], "endsAt": "…", "status": "OPEN"}
POSSESSION=<the id>
```

A shift already in another open possession is `FAILED_PRECONDITION`; no shifts, a bad id or a
different date between shifts, `INVALID_ARGUMENT`.

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

From Phase 1, instead of typing messages: `./mvnw -q test-compile exec:java
-Dexec.classpathScope=test -Dexec.args="--mode supervisor --target localhost:9094 --user
campo.responsable --password local"` and, in other terminals, `--mode device` with `--shifts`, the
number of teams, a cut every N seconds and a team that never acknowledges. It checks that the
sequences it receives are strictly increasing and flags a duplicate as an error.
