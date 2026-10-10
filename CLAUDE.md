# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

MTO Field: a Spring Boot 4.1 / Java 25 **gRPC** service, the live console of a night track
possession (the blocking of a track for maintenance work). `mto-maintenance` keeps the record of a
shift (tasks, defects, materials); `mto-field` keeps the live channel between the devices of the
teams on the track and the supervisor of the possession: who is connected and at which kp, which
tasks start and finish, and the **evacuation order with a nominal acknowledgement** from every team.
Informational only: no voltage, no SCADA. It is a practice project of gRPC and uses the four kinds
of call for a real reason each: unary (`OpenPossession`, `ClosePossession`, `IssueCommand`), server
streaming (`WatchPossessionBoard`, a conflated board), bidirectional (`TeamChannel`, one stream per
device for the whole night) and client streaming (`SyncBufferedEvents`, the backlog accumulated
without coverage, uploaded in order outside the live channel).

Read `docs/` in order for the full domain and architecture context: `00-project-overview.md`,
`01-architecture.md`, `02-domain-model.md`, `03-database.md`, `04-grpc-api.md`,
`05-development-roadmap.md`, `06-messaging.md`, `07-auditing.md`; `docs/grpc/field-api.md` is the
`grpcurl` cookbook. Those documents plus this file are the source of truth; `03-database.md`
documents the schema and should be checked before changing persistence code.

⚠️ `mto-maintenance` and `mto-stock` are **independent sibling repositories**, not modules of this
one. The shifts a possession groups, and the tasks the devices start and complete, belong to
`mto-maintenance`: they are reached through its REST API with the service account `mto-field-svc`
(`RestClientMaintenanceClient`) and stored here only as `uuid` columns without a foreign key. Nothing of the warehouse is
touched here.

⚠️ The local infrastructure (PostgreSQL, Keycloak, the trace collector, and `mto-maintenance`
itself) is brought up by `mto-platform`. `compose.yaml` here holds **only the application**.

⚠️ **`mto-gateway` does not proxy gRPC.** Clients (devices, the simulator, `grpcurl`) reach the
gRPC port directly: `9094` on the host, `9090` in the container. The HTTP port (`8087` / `8080`)
serves Actuator only.

State of the project: **Phases 0 to 5 done**. Every RPC is implemented, exercised by
`GrpcServiceLayerTest`, by `NetworkResilienceIT` (a Toxiproxy between the device and the server),
by `ReplicaClusterTest` (two replicas in one JVM) and by the simulator. `mto-maintenance` is called
through `RestClientMaintenanceClient` with the service account `mto-field-svc`
(`app.maintenance.enabled=true`, the default everywhere): the shifts of a possession are read from
it, and every `TaskStarted` / `TaskCompleted` is passed on to it and retried while it does not
answer. With `false` the `NoOp` client answers synthetic shifts (any shift id opens a possession)
and task events stay `PENDING`, which is what the simulator and most tests use. Phase 3 added the backlog upload (`SyncBufferedEvents` and the `BACKLOG_PENDING`
rule), the close of a stream whose token expired, the team binding by the groups claim and the
measured JWT size. Phase 4 added the replica bus: several replicas sharing over a RabbitMQ fanout
what is not in the database, with the database as the only truth and a catch-up tick that rereads
it (see **Replicas** below). Phase 5 added the own events for `mto-notification` (a possession
opened and closed, the evacuation issued, acknowledged and unacknowledged, the clear-of-track)
through an outbox and an exchange of its own (see **Messaging** below), the ack watchdog and the
simulator's own counter per device (`--state-dir`). There is no further phase planned.

Documentation and commit messages: the docs are in English (the owner's choice); the comments in
the code are in Spanish; commits in Spanish.

## Commands

```bash
./mvnw compile                                    # also generates the gRPC code (target/generated-sources/protobuf)
./mvnw test                                       # Testcontainers postgres:17-alpine, or TEST_DATABASE_URL/USERNAME/PASSWORD
./mvnw verify                                     # + failsafe: NetworkResilienceIT (Toxiproxy in Docker, or TOXIPROXY_URL) and RabbitReplicaBusIT (RabbitMQ in Docker, or TEST_RABBITMQ_URI); each skipped without its broker
./mvnw test -Dtest=GrpcServiceLayerTest           # one class
./mvnw spring-boot:run                            # dev: HTTP 8087, gRPC 9094, maintenance client off
./mvnw -q test-compile exec:java -Dexec.classpathScope=test \
  -Dexec.args="--mode demo --target localhost:9094 --user campo.responsable --password local --teams 3 --never-ack-team 3 --cut-every 15s --mixed"
                                                  # the simulator against the platform; --local-issuer instead of --user/--password without Keycloak
```

Local environment:

```bash
cd ../mto-platform && docker compose --profile all up -d && ./keycloak/apply-partials.sh
```

- Flyway runs on startup against `src/main/resources/db/migration`; Hibernate is `ddl-auto: validate`
  in every profile, so schema changes always go through a new migration. The SQL must be valid on
  PostgreSQL 16 and 17 (local and CI).
- Profiles: `dev`, `test`, `prod` (`SPRING_PROFILES_ACTIVE`; `dev` by default). All three run
  with the maintenance client on (`app.maintenance.enabled=true`) except `test`; `dev` points it at
  the `mto-maintenance` of `mto-platform` (`http://localhost:8083`). `APP_MAINTENANCE_ENABLED=false`
  switches to the `NoOp` client, which is how the simulator runs with invented shift ids.
- Reflection (`grpcurl`) and the health service are open without a token; reflection is off in `prod`.
- The database `mto_field` / `mto_field_user` is created by `mto-platform/postgres/init/01-databases.sql`,
  which only runs when the Postgres volume is created (`down -v`, or re-run the script with `psql`
  inside the container as the platform README says).

## Architecture

The same three layers as `mto-maintenance` under `com.alejandro.mtofield`:

- `domain/model` — framework-free rules: `PossessionRules` (which shifts can be grouped, the
  default `ends_at`, a forced close needs a reason), `PossessionStateMachine`, `TeamLiveness`
  (`CONNECTED` / `STALE` / `DISCONNECTED` from the silence of a device, the `best` of a team),
  `CommandAckSummary` (acked / pending / sent / queued, per team and in board order),
  `ShiftSnapshot` (the subset of a `mto-maintenance` shift this service reads) and
  `ShiftNotWorkableException`.
- `application` — `dto` (`PossessionView`, `CommandDraft`, `StoredCommand`, `EventContext`,
  `StoredEvent`, `SyncJob`, `BoardSnapshot`, `DevicePrincipal`, `ShiftMembership`; a draft and a
  context carry the protobuf message itself, nothing else does; `dto/messaging`: the envelope
  shared with the siblings, `AsynchronousMessage`, `DomainEvent`, `MessageActor`,
  `MessageActorKind`), `event` (`CommandCommitted`,
  `PossessionClosed`, published inside the transaction and delivered `AFTER_COMMIT`;
  `CommandsFannedOut`, after the dispatcher wrote to the streams), `replicas` (`ReplicaMessage`,
  what a replica tells the others; `ReplicaEnvelope`, its JSON shape; `ReplicaId`), `exception`
  (`BusinessException` with a stable `reason`, one subclass per rule), `mapper` (`ProtoJson`, a
  message as canonical JSON and back, which is how commands and events are stored; `ProtoTimestamps`),
  `service` + `service/impl` (package-private impls behind public interfaces: `PossessionService`,
  `FieldCommandService`, `FieldEventService`, `FieldEventSynchronizer` (`MaintenanceEventSynchronizer`
  with the client on, `PendingSyncEventSynchronizer` with it off), `FieldEventSyncRetryService`
  (`FieldEventSyncRetryServiceImpl`, one pass of the retry), `PossessionBoardService`,
  `LivenessRegistry` (`InMemoryLivenessRegistry`), `MaintenanceClient` (`RestClientMaintenanceClient`
  in `infrastructure/maintenance`, or `NoOpMaintenanceClient`), `FieldCodeGenerator`, `ReplicaBus`
  (`RabbitReplicaBus` in `infrastructure/messaging/replicas`, or `NoOpReplicaBus` with
  `app.rabbitmq.enabled=false`), `ReplicaMessageHandler` (what receives the other replicas' messages)
  and `RemoteDeviceStates` (`InMemoryRemoteDeviceStates`, what the other replicas told of their
  devices), `DomainEventPublisher` (`OutboxDomainEventPublisher` in `infrastructure/messaging/outbox`,
  or `NoOpDomainEventPublisher` with `app.rabbitmq.enabled=false`) and `EvacuationAckWatchdog`
  (`EvacuationAckWatchdogImpl`, one pass of the watchdog); `FieldEvents`, the names and the `values`
  of every own event, built in one place) and the two ports the gRPC layer implements for the
  board, `DeviceStreamPresence` and `BoardPublisher`: the application layer never sees a stream or
  an observer.
- `infrastructure/persistence` — `entity` (`Possession`, `PossessionShift`, the read-only
  `FieldCommandRecord`, `CommandAckRecord` and `FieldEventRecord`, the four enums, `AuditableEntity`)
  and `repository` (Spring Data plus the native idempotent SQL: the counter, `on conflict do
  nothing`, the conditional updates, the contiguous watermark, the paged replay).
- `infrastructure/grpc` — `FieldGrpcService` (`@GrpcService`, extends the generated
  `FieldServiceImplBase`, one `@PreAuthorize` per RPC), `GrpcErrors` (a `Status` with
  `google.rpc.ErrorInfo`), `advice/FieldGrpcExceptionAdvice`, `mapper/FieldProtoMapper` (DTO →
  protobuf by hand) and `stream/`: `DeviceStream`, `DeviceStreamRegistry` (also the lane of each
  possession), `CommandDispatcher`, `TeamChannels` (the session behind each `TeamChannel`),
  `SyncSessions` (the session behind each `SyncBufferedEvents`), `DeviceWorkQueues`,
  `CatchUpProbe` (test seam) and `ReplaySource`, `BoardWatcher`, `BoardWatcherRegistry`,
  `PossessionLifecycleListener`, `TeamBinding` (the team of the token against the team of the
  shift), `TokenExpirySweeper` (closes the streams and the board watchers whose token expired) and
  `ReplicaRelay` (this replica among the others: what it publishes, what it applies from them, the
  catch-up tick).
- `infrastructure/messaging/replicas` — the bus over RabbitMQ: `RabbitReplicaBus` (a transient
  `send` to the fanout, never failing towards the caller), `ReplicaMessageConsumer` (the listener:
  own messages ignored, a bad body or a failing handler logged and consumed), `ReplicaEnvelopeCodec`
  (the envelope as JSON with the application's `JsonMapper`, unknown fields tolerated),
  `ReplicaRabbitMqNames`.
- `infrastructure/messaging/outbox` — the outbox of the own events: a copy of `mto-configuration`'s
  `core/outbox`, the same one `mto-maintenance` and `mto-stock` carry (`OutboxMessage`, the
  repository with its `SKIP LOCKED` claim in order per aggregate, `OutboxService`, the relay and
  its scheduler, the immediate dispatch after the commit, `OutboxRabbitPublisher` with publisher
  confirms, the retry policy, the metrics, the purge, the endpoint, the tracing), plus the envelope
  factory, `MessageContextResolver` (the actor from the token of the gRPC call, the correlation
  from `MessagingCorrelation`, a thread-local the hooks fix with the possession code) and
  `OutboxDomainEventPublisher`; `infrastructure/messaging/rabbitmq/FieldRabbitMqNames` (the
  exchange, the routing keys and the event types of the contract).
- `configuration/security` — the Keycloak resource server, the same pieces as the siblings:
  `SecurityConfiguration` (the HTTP chain, **Actuator only**: health/info open, `POST`/`DELETE
  /actuator/**` → `OPS_WRITE`, the rest of Actuator → `OPS_METRICS`; the `JwtDecoder` built on the
  JWK Set, fetched lazily; the static `jwtValidator(...)`, issuer + validity + audience, that the
  test decoder reuses), `GrpcSecurityConfiguration` (the `@GlobalServerInterceptor`
  `AuthenticationProcessInterceptor`: health and reflection `permitAll`, everything else
  authenticated with the same decoder and converter), `KeycloakJwtAuthenticationConverter`,
  `JwtAudienceValidator`, `SecurityProperties`, `SecurityRoles` (`FIELD_TEAM`, `FIELD_SUPERVISE`,
  `OPS_METRICS`, `OPS_WRITE`), `CurrentUserService` (also `getTokenExpiresAt()`, for the stream
  that captures it at open, and `getGroups(claim)`, the groups of the token without their path).
- `configuration/grpc` — `GrpcServerConfiguration`: the `GrpcServerExecutorProvider` (a virtual
  thread per task; Boot does not give the gRPC server virtual threads on its own), the
  `fieldStreamExecutor` for what the service takes off the callback thread (catch-up, dispatch, the
  board) and the production `CatchUpProbe` (no-op); `FieldProperties` (`app.field.*`: the queue
  capacities, the catch-up paging and timeout, the liveness thresholds, the board tick, the
  token-expiry sweep and the team binding).
- `configuration/maintenance` — `MaintenanceProperties` (`app.maintenance.*`) and
  `MaintenanceClientConfiguration`: with `enabled=true` (the default) the `RestClient` towards
  `mto-maintenance` with the service-account bearer (an `AuthorizedClientServiceOAuth2AuthorizedClientManager`
  built there, never the request-bound one of Spring Security: no call leaves from an HTTP
  request), the circuit breaker `maintenance` (Resilience4j, `app.maintenance.circuit-breaker.*`,
  ignoring the rejections) and the `RestClientMaintenanceClient`; with `false`, nothing, and the
  `NoOp` client remains.
- `infrastructure/maintenance` — `RestClientMaintenanceClient`: `GET /shifts/{id}`,
  `GET /orders/{id}/tasks/{taskId}`, `POST .../start` and `POST .../complete` of
  `mto-maintenance`, payload records that read only the keys used here. A 4xx other than
  401/403/408/429 is a `MaintenanceRejectedException` (with mto-maintenance's `errorCode`); anything
  else (network, timeout, 5xx, open circuit, the service account refused) is a
  `MaintenanceUnavailableException`. The advice maps them to `FAILED_PRECONDITION` (metadata
  `maintenance_status`, `maintenance_error_code`) and `UNAVAILABLE`.
- `configuration/replicas` — `ReplicasConfiguration` (the `ReplicaId`: `app.field.replicas.id` or
  the host with a random suffix; the `NoOpReplicaBus` with `app.rabbitmq.enabled=false`) and
  `ReplicasRabbitConfiguration` (with the bus on: the durable fanout exchange
  `app.rabbitmq.replicas.exchange`, the exclusive auto-delete queue `mto.field.replicas.<id>`, its
  binding, the codec, the `RabbitReplicaBus`, the consumer and the `SimpleMessageListenerContainer`
  built through Boot's configurer so `spring.rabbitmq.listener.simple.*` applies, with
  `defaultRequeueRejected=false`). `spring.rabbitmq.*` is the connection (with publisher confirms
  and returns, which the outbox relay demands and the bus simply does not wait for); the broker is
  out of the health by default (`management.health.rabbit.enabled=false`).
- `configuration/rabbitmq` — `FieldEventsRabbitConfiguration` (the topic exchange of the own
  events, `app.rabbitmq.events.exchange`, no queue) and `FieldEventsProperties`;
  `configuration/outbox/OutboxConfiguration` (every outbox piece as a `@Bean`, gone with
  `app.rabbitmq.enabled=false`); `configuration/messaging` (`MessagePayloadSignature`, the signer
  with the shared secret, `app.messaging.signature.*`; only the signer: nothing is consumed here).
- `configuration/scheduling` — `FieldSchedulingConfiguration`: the board tick;
  `ReplicaCatchUpConfiguration`: `ReplicaRelay.catchUp()` every `app.field.replicas.catch-up`, always;
  `TokenExpiryConfiguration`: the sweep of `TokenExpirySweeper` every `app.field.token-expiry.sweep`
  (on by default; `enabled=false` turns it off);
  `FieldEventSyncRetryConfiguration`: the retry of the task events every
  `app.maintenance.sync-retry.interval`, only with the client on and `sync-retry.enabled` (off in
  the tests, which call `FieldEventSyncRetryService.retryDue()` by hand);
  `EvacuationWatchConfiguration` and `EvacuationWatchProperties`: the ack watchdog every
  `app.field.evacuation.check-every`, only with `app.field.evacuation.enabled` (off in the tests,
  which call `EvacuationAckWatchdog.check()` by hand).
  `configuration/metrics` — `FieldMetrics`, every meter name in one place. `ClockConfiguration` (the
  `Clock` the services and the board use; fixed in the tests), `AuditActorResolver` and
  `JpaAuditingConfiguration` (`created_by` is the username inside a callback, `system` in the
  service's own threads).
- `grpc.v1` — generated from `src/main/proto/mto/field/v1/field_service.proto`
  (`java_package com.alejandro.mtofield.grpc.v1`). Fields may be added; semantics do not change.

No MapStruct: the protobuf builders are not beans, the DTO → protobuf mapping is by hand
(`FieldProtoMapper`), and the REST bodies towards `mto-maintenance` are maps and records built in
the client. `Possession`, `FieldCommand` and `CommandAck` also exist as generated messages: the
entities that clash carry the suffix `Record`, and the gRPC layer only sees DTOs.

### Synchronization with `mto-maintenance`

A task event is persisted `PENDING` in the stream callback and handed to the device's work queue;
`MaintenanceEventSynchronizer` runs there (or in the scheduled retry), never in the stream thread,
and decides how the event ends. Neither `start` nor `complete` is idempotent in `mto-maintenance`,
so a lost answer comes back as a 409 `TRN-001` and is **reconciled by reading the task**:

| Event | `mto-maintenance` says | Outcome |
|---|---|---|
| any | 2xx | `SYNCED`, `EventResult{APPLIED}` |
| `TaskStarted` | 409 `TRN-001` and the task is `IN_PROGRESS` or `COMPLETED` | `SYNCED` (the answer was lost, or another device of the team started it) |
| `TaskCompleted` | 409 `TRN-001` and the task is `COMPLETED` | `SYNCED` |
| any | any other 4xx of business (`TRN-001` with the task `PENDING`/`CANCELLED`, `SHF-001`, `MO-404`, `VAL-001`...), ids that are not UUIDs | `REJECTED` with `<code>: <message>` in `last_error` and in the `EventResult`; never retried |
| any | no answer (network, timeout, 5xx, circuit open, 401/403 of the service account) | `FAILED`, `next_attempt_at = now + interval`; the first attempt answers `EventResult{PENDING_SYNC}`, a retry only answers when it resolves |

`mto-maintenance` accepts completing a `PENDING` task (it sets the start itself), so a completion
that overtakes a start left `FAILED` is applied, and that start, when retried, finds the task
completed and counts as applied. The retry (`FieldEventSyncRetryServiceImpl`) takes the `FAILED`
and the `PENDING` events whose `next_attempt_at` has passed, in order of arrival and in batches,
and stops at the first one `mto-maintenance` does not answer, like the stock retry of
`mto-maintenance`. A possession closed while an event waited has nobody to answer: the outcome is
recorded and the `EventResult` is skipped. `field.event.sync` counts the outcomes.

### Streams

The invariants the streams core (`infrastructure/grpc/stream`, Phase 1) is built on. Removing any
of them reopens a window of loss or duplication:

- **One writer per stream.** Only the thread that wins the `draining` CAS writes to the
  `ServerCallStreamObserver`; whoever offers a command never writes, and the terminal
  (`onCompleted` / `onError`) is emitted by the drainer, exactly once. Flow control is manual:
  `disableAutoRequest()`, `request(1)` per processed message, `isReady()` + `setOnReadyHandler`.
- **Register before replay.** A `DeviceStream` is registered in the `DeviceStreamRegistry` before
  its resumption query; commands committed meanwhile are held and merged after the replay. A second
  stream with the same `device_id` supersedes the first (`ABORTED "superseded"`); unregistering is
  by instance, so the superseded stream never removes its successor.
- **Gapless, commit-ordered sequence per possession.** `possession.next_command_seq` is a column,
  not a PostgreSQL sequence: `update ... returning` inside the command's transaction holds the row
  lock until commit, so numbers become visible in order and a rollback leaves no hole.
- **Dispatch after commit, off the committing thread.** `@TransactionalEventListener(AFTER_COMMIT)`
  hands the command to `fieldStreamExecutor`; one lane per possession, created at registration
  with `max(sequence)` from the database (never lazily at dispatch), fills from the database
  whatever it has not fanned out yet. A stream never enqueues a sequence ≤ the last one it enqueued:
  the dispatcher may resend, it never leaves a gap.
- **Contiguous watermark upstream.** `field_event` is unique on `(device_id, sequence)`, written
  with `on conflict do nothing`; `Welcome.last_applied_sequence` is the contiguous maximum, not
  `max(sequence)`. A heartbeat travels with `sequence 0` and is not stored. The device sees a
  subset of the possession sequence (targeted `EventResult`s of other shifts take numbers too), so
  it cannot infer a loss from a gap; resumption is `Join.last_command_sequence`.
- **Inline vs queued events.** `CommandAck` and `ClearOfTrack` are applied in the callback, one
  transaction each, and answered with an `EventResult`. `TaskStarted` / `TaskCompleted` are
  persisted `PENDING` and queued to a bounded per-device work queue whose consumer talks to
  `mto-maintenance`; `request(1)` only when the queue accepted, a full queue closes the stream with
  `RESOURCE_EXHAUSTED` (the event is persisted; the watermark prevents a resend). A business
  rejection inside a stream goes in-band as `EventResult{REJECTED}`: a `Status` closes the stream.
- **Do not rely on the `SecurityContext` in own threads.** The interceptor sets it around each
  callback; the principal and the token expiry are captured at open (`CurrentUserService`), and the
  work-queue threads run with the root context and no security context on purpose.
- **What a device declares applied counts as sent.** `Join.last_command_sequence` seeds the new
  stream's `lastSentSequence`, which is what the board reads for `sent_to`: a device that resumes
  holding the evacuation order does not go back to `queued_for` with every reconnection.
- **A backlog goes through `SyncBufferedEvents`, in order.** After a cut, the events the server
  does not hold (those above `Welcome.last_applied_sequence`) are uploaded through the client
  stream: a `Join` first, then strictly increasing sequences (`INVALID_ARGUMENT OUT_OF_ORDER`
  otherwise), acks and clear-of-track applied inline, task events stored and queued as on the
  live channel, and a `SyncResult{last_applied_sequence, applied, duplicates, rejected}` on the
  half-close. While a device has a backlog, a task event on `TeamChannel` whose sequence leaves a
  gap over the contiguous watermark is answered `EventResult{REJECTED, BACKLOG_PENDING}`; acks and
  clear-of-track are never held back. The `EventResult`s of what the sync stored still travel on
  the command sequence (the `TeamChannel` if open, resumption otherwise), not in the `SyncResult`.
- **A stream dies with its token.** The JWT is validated when the call opens and a stream lasts
  the night: `TokenExpirySweeper` closes every `DeviceStream` and `BoardWatcher` whose captured
  expiry has passed with `UNAUTHENTICATED TOKEN_EXPIRED` (metadata `expired_at`), and the device
  resumes with a fresh token and `Join.last_command_sequence`, losing nothing.
- **The team of the token.** With `app.field.team-binding.enabled` (the default), a `Join` on
  either stream is refused with `PERMISSION_DENIED TEAM_NOT_ALLOWED` (metadata `team_code`)
  unless the shift's `team_code` is among the token's groups (`app.field.team-binding.claim`,
  `groups`, the Keycloak group-membership mapper without the path) or the token carries
  `field-supervise`. The membership is captured at open with the principal (`TeamBinding.capture`).
- **The board is conflated.** Recompute is coalesced per possession (N heartbeats are not N
  recomputes), a watcher keeps only the latest board and a slow one skips versions; a watcher is
  registered before the first publication. It is marked dirty by the dispatcher after every
  fan-out (also with nobody connected), by a `Join`, a heartbeat and a stream closing, and by the
  tick while someone watches.

### Replicas

Several replicas behind an L4 balancer, a possession's devices spread over them and the board
watched from any (`docs/06-messaging.md`). The rules, in order of importance:

- **The database is the truth; the bus only accelerates.** The replicas share over a RabbitMQ
  fanout (`ReplicaBus`, `ReplicaRelay`) only what is not in the database: every committed command
  by its number (the receiver reads it from the database and its dispatcher fans it out, filling the
  lane's gap), every close, the state of each device (join, heartbeat, close, written to) and the
  goodbye at shutdown. Nothing a device receives can come only from the bus.
- **The catch-up tick is the guarantee.** Every `app.field.replicas.catch-up` (2 s), with or
  without bus, each replica rereads the database for every lane it holds: `max(sequence)` against
  what the lane fanned out, possessions already `CLOSED`, and remote states nobody refreshed within
  `app.field.replicas.remote-ttl`. A broker that is down, a lost message or a replica that died
  without saying goodbye cost latency, never correctness.
- **Publishing never blocks or fails the business**: after the commit, on the stream executor; a
  failure is a WARN and a counter (`field.replicas.messages`). The broker is not in the health.
- **Local wins; the newest stream wins.** The board merges `RemoteDeviceStates` with the local
  liveness and presence: a device with a stream open on this replica is local; a device known only
  as closed here and open elsewhere is remote. A `DEVICE_STATE` with a newer `openedAt` for a
  device this replica holds supersedes the local stream (`ABORTED SUPERSEDED`), like a second
  local stream; a closed-stream state older than the open one known is ignored.
- **A replica that says goodbye stops counting at once**; what it tells afterwards about closed
  streams is dropped until it opens a stream again with that name. One that dies silently expires
  by the TTL.
- **The bus has no signature, no outbox, no dead-letter queue, no queue of anyone else's**: it is
  one service talking to itself, transient and recoverable from the database; the messages carry
  the `kind` and an unknown one is ignored, so versions coexist during a deployment. The own events
  (below) are the opposite: signed, through the outbox, on an exchange of their own.
- **Board versions are per replica** (seeded with the clock); a watcher always talks to one. With
  the bus off a replica only knows the presence of its own devices.

### Messaging

What this service tells the outside world (`docs/06-messaging.md`, *Published events*) goes to
`mto-notification` through `mto.field.exchange` (topic, durable, declared here; the queue
`mto.notification.field.queue` is the consumer's), routing key `mto.field.possession.<event>`, as a
`DomainEvent` in the `AsynchronousMessage` envelope of `mto-configuration`, signed with the shared
secret. **`DomainEventPublisher.publish` is the only door, and it is called inside the business
transaction**: the event goes to `outbox_message` (`V2`) with the change and the relay publishes it
afterwards with publisher confirms (`OutboxRabbitPublisher` refuses to start without them). One
aggregate, `possession`, so the relay's order per aggregate keeps a night in order. The hooks:
`PossessionServiceImpl.open/close` (`opened`, `closed` with the teams still on the track),
`FieldCommandServiceImpl.insert` (`evacuation-issued`, only for `EVACUATE_NOW`: messages, window
changes and `EventResult`s are the channel's), `FieldEventServiceImpl.recordAck/recordClearOfTrack`
(`evacuation-acknowledged` and `clear-of-track`, only the first time a team does it, with the teams
still pending) and `EvacuationAckWatchdogImpl` (`evacuation-unacknowledged`, once per evacuation
past `app.field.evacuation.ack-timeout`, decided by the conditional mark `ack_watched_at` of `V3`,
with an `operationId` derived from the command). The names and `values` of every event live in
`FieldEvents` and nowhere else; the actor is read from the token of the gRPC call by
`MessageContextResolver` (`PERSON` for the supervisor and the technician, `SYSTEM` for the
watchdog) and the `correlationId` is the **possession code**, fixed by each hook around the publish
with `MessagingCorrelation` (gRPC has no `X-Correlation-Id`, and a night spans many calls).
**Keys are only added**; a new key or event changes its example in `docs/messaging/examples/` in
the same commit (`MessagingContractExamplesTest` compares them with the real factory;
`MESSAGING_EXAMPLES_WRITE=true` regenerates them). `DomainEvent` rejects any key that smells like a
secret. With `app.rabbitmq.enabled=false` the publisher is the `NoOpDomainEventPublisher` and no
outbox bean exists. The `mto-field-supervisor` profile carries `notification-inbox` and
`notification-activity-read` so that the supervisor receives it; the technician's profile still
carries nothing of notifications.

### Persistence rules

- UUID ids (`gen_random_uuid()`), `created_at`/`updated_at`/`created_by`/`updated_by` on every table
  (Spring Data auditing, `AuditActorResolver`), snake_case tables, PostgreSQL enums
  for every status and kind (`@Enumerated(STRING)` + `@JdbcTypeCode(SqlTypes.NAMED_ENUM)`),
  `timestamptz` for instants.
- Codes come from `possession_code_seq` (`PO-000001`), never from `MAX + 1`.
- Ids of `mto-maintenance` (`shift_id`, `order_id`, `task_id`) are `uuid` columns without a foreign
  key. A kp is a decimal string in the contract, never a double.
- `field_command.payload` and `field_event.payload` hold the whole protobuf message as `json`
  (`JsonFormat`): resumption resends the command as stored; the typed columns are for querying.
- **No Envers, on purpose**: `field_command`, `command_ack` and `field_event` are append-only and
  are the history; only the audit columns exist (`docs/07-auditing.md`). `outbox_message` (`V2`)
  has no audit columns either: only the outbox writes it, and its history is itself.
- `field_command.ack_watched_at` (`V3`) is the only column of a command written after the insert:
  the ack watchdog's conditional mark, once, which is what makes `evacuation-unacknowledged` be
  published once however many replicas run.
- Idempotency and ordering are decided by the database, inside the writing statement, never
  read-then-write: `insert ... on conflict do nothing` (events, acknowledgements), `update ... where
  ... returning` (the command counter, a shift's clear-of-track), row counts as the answer. The
  partial unique index `uq_possession_shift_open (shift_id) WHERE open` keeps a shift out of two
  open possessions.

### Security

Resource server of the `mto` realm, audience `mto-field-api`, for gRPC and HTTP alike. Permissions
are client roles of `mto-field-api` (`field-team`, `field-supervise`, `ops-metrics`, `ops-write`),
profiles are realm composites (`mto-field-technician`: `field-team`; `mto-field-supervisor`:
`field-team` + `field-supervise`); realm roles are emitted only as `ROLE_REALM_*`. The permission
of each RPC is a `@PreAuthorize` on its method in `FieldGrpcService` (`@EnableMethodSecurity`):
`FIELD_TEAM` for `TeamChannel` and `SyncBufferedEvents`, `FIELD_SUPERVISE` for the other four;
Boot's handler turns the denial into `UNAUTHENTICATED` / `PERMISSION_DENIED`, also when opening a
stream. There is no switch to disable security; an empty `required-audience` with the validation on
stops the startup (`SecurityProperties`). The service account `mto-field-svc` gets
`maintenance-read`/`maintenance-write` on `mto-maintenance-api` from `apply-partials.sh`. A new role
goes to `SecurityRoles` **and** `keycloak/mto-field-partial-import.json` (and to
`mto-platform/keycloak/mto-ops-cross-service.json` if it is an `ops-*`). The realm partial also
declares the groups `EQ-NORTE` and `EQ-SUR` (a group is named after the `code` of the team in
`mto-maintenance`), the development technicians belong to one each, and the login client
`mto-frontend` of `mto-platform` carries the group-membership mapper that puts them in the
access token as `groups`: that is what the team binding reads. The token travels in the
`authorization` metadata of every call, and the server admits 8 KiB of metadata
(`spring.grpc.server.inbound.metadata.max-size`): the worst-case token of the realm (every client
role of the seven APIs, every profile, the groups) measures 3945 bytes of metadata, under half the
limit (`SecurityLayerTest` prints it); it is a reason not to grow the realm's claims carelessly.

### Testing

- `PostgreSQLTestContainer` (in `support/`) starts `postgres:17-alpine` with Testcontainers, or uses
  `TEST_DATABASE_URL`/`TEST_DATABASE_USERNAME`/`TEST_DATABASE_PASSWORD` when set (no Docker
  needed); without either the test is skipped, not failed.
- `TestTokens` mints JWTs with an in-test RSA key (`technician`, `technicianOfTeams`,
  `supervisor`, `supervisorWithoutTeam`, `forAnotherAudience`, `expired`, `signedByAnotherKey`, and
  `mint` with an expiry and groups) and attaches them to a stub; `TestJwtDecoderConfiguration` is a `@Primary` decoder on that public key with the
  **same** validator chain as production (`SecurityConfiguration.jwtValidator`).
- `MtoFieldApplicationTests` boots the whole context against a real PostgreSQL with Netty on a free
  port and checks the health and reflection services without a token (the stand-in for `grpcurl`).
- One class per layer, add methods rather than classes: `DomainModelTest`; `BusinessLayerTest`
  (the services with mocked repositories: open and close, `issue` with the key and the re-read
  after a violation, acks, clear-of-track, task events and their resend, the board and its
  coalescing, the wiring of each mode of the switch, the outcome table of
  `MaintenanceEventSynchronizer` with a mocked client and one pass of the retry);
  `MaintenanceClientTest` (`MockRestServiceServer`: the routes and bodies of the `mto-maintenance`
  contract, what is read from its answers, a rejection with its code that leaves the circuit
  closed, an outage that opens it, the service-account token going out without an HTTP request);
  `FieldRepositoryDataJpaTest` (the gapless sequence with two threads and a rollback, the watermark,
  the partial index, the native updates; its possession codes are random because the gRPC tests
  share the database); `SecurityLayerTest` (converter, audience, `CurrentUserService` with the
  groups, properties, and the size of the worst-case realm token against the metadata limit);
  `GrpcServiceLayerTest` (`@AutoConfigureTestGrpcTransport`, in-process, the real security chain and
  a real PostgreSQL: authentication, the authorization of each RPC including a stream, the unary
  calls, Join and Welcome, the evacuation with acks and the close, resumption, the catch-up race
  through the `CatchUpProbe` seam, duplicates and the watermark, supersession, the board and its
  slow watcher, backpressure with a gated synchronizer, the backlog through `SyncBufferedEvents`
  (stored once, the gap refused on the `TeamChannel` with `BACKLOG_PENDING` until synced, the
  watermark, `Join` required, out of order, no role), the token that expires under a `TeamChannel`
  and under a board watcher (`TOKEN_EXPIRED`, resumed with a fresh one), and, in a second context
  with the client on and a `@MockitoBean` in its place, a start and a completion passed on and
  answered `APPLIED`, a rejection with its code that a resend does not ask again, an outage answered
  `PENDING_SYNC` and resolved by the retry, and a possession that cannot open without the shift or
  without `mto-maintenance`, and, in a third context with the team binding on, the technician of
  another team refused on both streams and the technician of the team and the supervisor admitted;
  `DeviceClient`, `SyncClient` and `BoardClient` in `support/` are its clients).
  `NetworkResilienceIT` (failsafe; `@SpringBootTest` with the real Netty on a free port, the device
  behind a Toxiproxy from `support/ToxiproxyGateway`, the supervisor and the board direct): the cut
  mid-evacuation (the order queued, delivered once and in order on resumption, the board from
  `queued_for` to `sent_to` to `acked_by`), a slow and narrow network (latency with jitter and
  16 KB/s both ways: twenty messages and the evacuation in order, no duplicates), a peer that goes
  mute without closing (the server keepalive detects it in time + timeout and the board says
  `DISCONNECTED`) and a client pinging within the permitted rate. It runs the server with the
  smallest keepalive grpc-java allows (10 s + 1 s: `KeepAliveManager` raises anything lower), so
  the two keepalive scenarios take that long. `DeviceStreamTest` is the planned exception to "one
  class per layer": the streams core with a fake `ServerCallStreamObserver` (draining, catch-up,
  terminals, the registry, the dispatcher, the work queues, the token-expiry sweep with a fixed
  clock, the team-binding rules), like the outbox's own tests, copied from `mto-maintenance` under
  `infrastructure/messaging/outbox` (`OutboxRelayDataJpaTest` against PostgreSQL, `OutboxWiringTest`,
  `OutboxRabbitPublisherTest`…; `OutboxRabbitIT`, failsafe, publishes with confirms against a real
  RabbitMQ and reads the message back). `MessagingLayerTest` (the replica bus without a broker: the
  envelope as JSON and an unknown kind, the publication over a mocked `RabbitTemplate` and a broker
  that is down, the consumer with own messages, a failing handler and a body that is not an
  envelope, the topology and the wiring with `ApplicationContextRunner` in both modes, the own
  exchange; and the envelope of the own events: the actor of the JWT, the correlation that is
  fixed and cleared, the hash over the seven keys, the publisher with the contract names).
  `MessagingContractExamplesTest` (one JSON per published event, built with `FieldEvents` and the
  real factory). The hooks and the watchdog are methods of `BusinessLayerTest`
  (`RecordingEventPublisher`), the whole night in order with the actor of each call a scenario of
  `GrpcServiceLayerTest` (`RecordingDomainEventPublisher`, `@Primary` in its `Probes`), and the
  watchdog's query and mark of `FieldRepositoryDataJpaTest`. `DeviceStateStoreTest` covers the
  simulator's state file and a restarted device. `ReplicaClusterTest`: two contexts started by hand
  (`SpringApplicationBuilder`, properties as arguments because the `dev` profile would override
  defaults, each with its Netty on a free port read from `local.grpc.server.port`) on the same
  database, joined by the in-memory `LocalReplicaBus`/`LocalReplicaHub` of `support/` instead of
  RabbitMQ: the commands of A reach the device of B in order and once and the board of A sees the
  team of B; with the hub cut the catch-up tick delivers them and the close; a newer stream on A
  supersedes the one on B; B stopped no longer counts on A. `RabbitReplicaBusIT` (failsafe): two
  contexts with the real bus configuration on a RabbitMQ container (`support/RabbitMqTestBroker`,
  or `TEST_RABBITMQ_URI`; skipped without either). Every `@SpringBootTest` sets
  `app.rabbitmq.enabled=false` and `app.field.evacuation.enabled=false`: the tests run on the `dev`
  profile, where the bus and the watchdog are on.
  The simulator (`src/test/java/.../simulator`, no Spring) is a tool, not a test: with
  `--local-issuer` it serves the JWK Set of `TestTokens` and mints its own tokens (with the team of
  each shift in `groups` and `--token-ttl`, to watch the expiry close and the renewal), which is
  how it runs against `java -jar` without Keycloak (`README.md`). After a cut its devices resend
  the acks and clear-of-track on the `TeamChannel` and upload the work events the `Welcome` says
  the server lacks through `SyncBufferedEvents`, buffering new work events meanwhile. With several
  `--target`s it spreads the devices over the replicas and keeps the supervisor on the first. With
  `--state-dir` each device persists its counter, its last applied command and its unconfirmed
  uploads (`DeviceStateStore`, `<dir>/<deviceId>.json`, written whole and atomically) and resumes
  from them after a restart; without it, each start continues after the server's watermark.
