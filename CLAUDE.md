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
device for the whole night) and, in Phase 3, client streaming (`SyncBufferedEvents`, the backlog
accumulated without coverage).

Read `docs/` in order for the full domain and architecture context: `00-project-overview.md`,
`01-architecture.md`, `02-domain-model.md`, `03-database.md`, `04-grpc-api.md`,
`05-development-roadmap.md`, `06-messaging.md`, `07-auditing.md`; `docs/grpc/field-api.md` is the
`grpcurl` cookbook. Those documents plus this file are the source of truth; `03-database.md`
documents the schema and should be checked before changing persistence code.

⚠️ `mto-maintenance` and `mto-stock` are **independent sibling repositories**, not modules of this
one. The shifts a possession groups, and the tasks the devices start and complete, belong to
`mto-maintenance`: they are reached through its REST API with the service account `mto-field-svc`
(Phase 2) and stored here only as `uuid` columns without a foreign key. Nothing of the warehouse is
touched here.

⚠️ The local infrastructure (PostgreSQL, Keycloak, the trace collector, and `mto-maintenance`
itself) is brought up by `mto-platform`. `compose.yaml` here holds **only the application**.

⚠️ **`mto-gateway` does not proxy gRPC.** Clients (devices, the simulator, `grpcurl`) reach the
gRPC port directly: `9094` on the host, `9090` in the container. The HTTP port (`8087` / `8080`)
serves Actuator only.

State of the project: **Phases 0 and 1 done**. Every RPC but `SyncBufferedEvents` (Phase 3, still
`UNIMPLEMENTED`) is implemented, exercised by `GrpcServiceLayerTest` and by the simulator.
`mto-maintenance` is not called yet: `app.maintenance.enabled=false` everywhere (the `NoOp` client
answers synthetic shifts, so any shift id opens a possession), and with `true` the application
**refuses to start**, with a message that says so, until Phase 2 brings the REST client. The
sections below say what exists and what each next phase adds; do not describe a Phase 2+ class as
existing.

Documentation and commit messages: the docs are in English (the owner's choice); the comments in
the code are in Spanish; commits in Spanish.

## Commands

```bash
./mvnw compile                                    # also generates the gRPC code (target/generated-sources/protobuf)
./mvnw test                                       # Testcontainers postgres:17-alpine, or TEST_DATABASE_URL/USERNAME/PASSWORD
./mvnw verify                                     # + failsafe (*IT; none yet)
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
- Profiles: `dev`, `test`, `prod` (`SPRING_PROFILES_ACTIVE`; `dev` by default). `dev` runs with
  `app.maintenance.enabled=false` until Phase 2, like the platform compose; with `true` the
  application refuses to start.
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
  context carry the protobuf message itself, nothing else does), `event` (`CommandCommitted`,
  `PossessionClosed`, published inside the transaction and delivered `AFTER_COMMIT`), `exception`
  (`BusinessException` with a stable `reason`, one subclass per rule), `mapper` (`ProtoJson`, a
  message as canonical JSON and back, which is how commands and events are stored; `ProtoTimestamps`),
  `service` + `service/impl` (package-private impls behind public interfaces: `PossessionService`,
  `FieldCommandService`, `FieldEventService`, `FieldEventSynchronizer` —
  `PendingSyncEventSynchronizer` until Phase 2 —, `PossessionBoardService`, `LivenessRegistry`
  (`InMemoryLivenessRegistry`), `MaintenanceClient` (`NoOpMaintenanceClient`), `FieldCodeGenerator`)
  and the two ports the gRPC layer implements for the board, `DeviceStreamPresence` and
  `BoardPublisher`: the application layer never sees a stream or an observer.
- `infrastructure/persistence` — `entity` (`Possession`, `PossessionShift`, the read-only
  `FieldCommandRecord`, `CommandAckRecord` and `FieldEventRecord`, the four enums, `AuditableEntity`)
  and `repository` (Spring Data plus the native idempotent SQL: the counter, `on conflict do
  nothing`, the conditional updates, the contiguous watermark, the paged replay).
- `infrastructure/grpc` — `FieldGrpcService` (`@GrpcService`, extends the generated
  `FieldServiceImplBase`, one `@PreAuthorize` per RPC; `SyncBufferedEvents` still delegates to the
  base: `UNIMPLEMENTED`), `GrpcErrors` (a `Status` with `google.rpc.ErrorInfo`),
  `advice/FieldGrpcExceptionAdvice`, `mapper/FieldProtoMapper` (DTO → protobuf by hand) and
  `stream/`: `DeviceStream`, `DeviceStreamRegistry` (also the lane of each possession),
  `CommandDispatcher`, `TeamChannels` (the session behind each `TeamChannel`), `DeviceWorkQueues`,
  `CatchUpProbe` (test seam) and `ReplaySource`, `BoardWatcher`, `BoardWatcherRegistry`,
  `PossessionLifecycleListener`.
- `configuration/security` — the Keycloak resource server, the same pieces as the siblings:
  `SecurityConfiguration` (the HTTP chain, **Actuator only**: health/info open, `POST`/`DELETE
  /actuator/**` → `OPS_WRITE`, the rest of Actuator → `OPS_METRICS`; the `JwtDecoder` built on the
  JWK Set, fetched lazily; the static `jwtValidator(...)`, issuer + validity + audience, that the
  test decoder reuses), `GrpcSecurityConfiguration` (the `@GlobalServerInterceptor`
  `AuthenticationProcessInterceptor`: health and reflection `permitAll`, everything else
  authenticated with the same decoder and converter), `KeycloakJwtAuthenticationConverter`,
  `JwtAudienceValidator`, `SecurityProperties`, `SecurityRoles` (`FIELD_TEAM`, `FIELD_SUPERVISE`,
  `OPS_METRICS`, `OPS_WRITE`), `CurrentUserService` (also `getTokenExpiresAt()`, for the stream
  that captures it at open).
- `configuration/grpc` — `GrpcServerConfiguration`: the `GrpcServerExecutorProvider` (a virtual
  thread per task; Boot does not give the gRPC server virtual threads on its own), the
  `fieldStreamExecutor` for what the service takes off the callback thread (catch-up, dispatch, the
  board) and the production `CatchUpProbe` (no-op); `FieldProperties` (`app.field.*`: the queue
  capacities, the catch-up paging and timeout, the liveness thresholds, the board tick, the
  token-expiry switch of Phase 3).
- `configuration/maintenance` — `MaintenanceProperties` (`app.maintenance.*`) and
  `MaintenanceClientConfiguration`, which with `enabled=true` defines a bean that throws at startup
  with the message to run with `APP_MAINTENANCE_ENABLED=false` until Phase 2.
- `configuration/scheduling` — `FieldSchedulingConfiguration`: the board tick.
  `configuration/metrics` — `FieldMetrics`, every meter name in one place. `ClockConfiguration` (the
  `Clock` the services and the board use; fixed in the tests), `AuditActorResolver` and
  `JpaAuditingConfiguration` (`created_by` is the username inside a callback, `system` in the
  service's own threads).
- `grpc.v1` — generated from `src/main/proto/mto/field/v1/field_service.proto`
  (`java_package com.alejandro.mtofield.grpc.v1`). Fields may be added; semantics do not change.

Phase 2 adds `infrastructure/maintenance/RestClientMaintenanceClient`. No MapStruct: the protobuf
builders are not beans, the DTO → protobuf mapping is by hand (`FieldProtoMapper`). `Possession`,
`FieldCommand` and `CommandAck` also exist as generated messages: the entities that clash carry the
suffix `Record`, and the gRPC layer only sees DTOs.

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
- **The board is conflated.** Recompute is coalesced per possession (N heartbeats are not N
  recomputes), a watcher keeps only the latest board and a slow one skips versions; a watcher is
  registered before the first publication. It is marked dirty by the dispatcher after every
  fan-out (also with nobody connected), by a `Join`, a heartbeat and a stream closing, and by the
  tick while someone watches.

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
  are the history; only the audit columns exist (`docs/07-auditing.md`).
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
`mto-platform/keycloak/mto-ops-cross-service.json` if it is an `ops-*`).

### Testing

- `PostgreSQLTestContainer` (in `support/`) starts `postgres:17-alpine` with Testcontainers, or uses
  `TEST_DATABASE_URL`/`TEST_DATABASE_USERNAME`/`TEST_DATABASE_PASSWORD` when set (no Docker
  needed); without either the test is skipped, not failed.
- `TestTokens` mints JWTs with an in-test RSA key (`technician`, `supervisor`,
  `supervisorWithoutTeam`, `forAnotherAudience`, `expired`, `signedByAnotherKey`) and attaches them
  to a stub; `TestJwtDecoderConfiguration` is a `@Primary` decoder on that public key with the
  **same** validator chain as production (`SecurityConfiguration.jwtValidator`).
- `MtoFieldApplicationTests` boots the whole context against a real PostgreSQL with Netty on a free
  port and checks the health and reflection services without a token (the stand-in for `grpcurl`).
- One class per layer, add methods rather than classes: `DomainModelTest`; `BusinessLayerTest`
  (the services with mocked repositories: open and close, `issue` with the key and the re-read
  after a violation, acks, clear-of-track, task events and their resend, the board and its
  coalescing, the disconnected client and the refusal to start with the client on);
  `FieldRepositoryDataJpaTest` (the gapless sequence with two threads and a rollback, the watermark,
  the partial index, the native updates; its possession codes are random because the gRPC tests
  share the database); `SecurityLayerTest` (converter, audience, `CurrentUserService`, properties);
  `GrpcServiceLayerTest` (`@AutoConfigureTestGrpcTransport`, in-process, the real security chain and
  a real PostgreSQL: authentication, the authorization of each RPC including a stream, the unary
  calls, Join and Welcome, the evacuation with acks and the close, resumption, the catch-up race
  through the `CatchUpProbe` seam, duplicates and the watermark, supersession, the board and its
  slow watcher, backpressure with a gated synchronizer; `DeviceClient` and `BoardClient` in
  `support/` are its clients). `DeviceStreamTest` is the planned exception to "one class per
  layer": the streams core with a fake `ServerCallStreamObserver` (draining, catch-up, terminals,
  the registry, the dispatcher, the work queues), like the outbox's own tests in `mto-maintenance`.
  The simulator (`src/test/java/.../simulator`, no Spring) is a tool, not a test: with
  `--local-issuer` it serves the JWK Set of `TestTokens` and mints its own tokens, which is how it
  runs against `java -jar` without Keycloak (`README.md`).
