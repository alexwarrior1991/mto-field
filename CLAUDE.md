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

State of the project: **Phase 0 done** (skeleton, contract, `V1`, security, executors, Keycloak
files, image, compose, CI). Every RPC still answers `UNIMPLEMENTED`. The sections below say what
exists and what each phase adds; do not describe a Phase 1+ class as existing.

Documentation and commit messages: the docs are in English (the owner's choice); the comments in
the code are in Spanish; commits in Spanish.

## Commands

```bash
./mvnw compile                                    # also generates the gRPC code (target/generated-sources/protobuf)
./mvnw test                                       # Testcontainers postgres:17-alpine, or TEST_DATABASE_URL/USERNAME/PASSWORD
./mvnw verify                                     # + failsafe (*IT; none yet)
./mvnw test -Dtest=GrpcServiceLayerTest           # one class
./mvnw spring-boot:run                            # dev: HTTP 8087, gRPC 9094, maintenance client off
./mvnw -q test-compile exec:java -Dexec.classpathScope=test -Dexec.args="--mode supervisor --target localhost:9094 ..."   # the simulator (Phase 1)
```

Local environment:

```bash
cd ../mto-platform && docker compose --profile all up -d && ./keycloak/apply-partials.sh
```

- Flyway runs on startup against `src/main/resources/db/migration`; Hibernate is `ddl-auto: validate`
  in every profile, so schema changes always go through a new migration. The SQL must be valid on
  PostgreSQL 16 and 17 (local and CI).
- Profiles: `dev`, `test`, `prod` (`SPRING_PROFILES_ACTIVE`; `dev` by default). `dev` runs with
  `app.maintenance.enabled=false` while Phase 1 lasts.
- Reflection (`grpcurl`) and the health service are open without a token; reflection is off in `prod`.
- The database `mto_field` / `mto_field_user` is created by `mto-platform/postgres/init/01-databases.sql`,
  which only runs when the Postgres volume is created (`down -v`, or re-run the script with `psql`
  inside the container as the platform README says).

## Architecture

The same three layers as `mto-maintenance` under `com.alejandro.mtofield`. What exists today:

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
  thread per task; Boot does not give the gRPC server virtual threads on its own) and the
  `fieldStreamExecutor` for what the service takes off the callback thread.
- `infrastructure/grpc` — `FieldGrpcService` (`@GrpcService`, extends the generated
  `FieldServiceImplBase`, one `@PreAuthorize` per RPC, bodies delegating to the base:
  `UNIMPLEMENTED`).
- `grpc.v1` — generated from `src/main/proto/mto/field/v1/field_service.proto`
  (`java_package com.alejandro.mtofield.grpc.v1`). Fields may be added; semantics do not change.

Phase 1 adds `domain/model` (`PossessionStateMachine`, `PossessionRules`, `TeamLiveness`,
`CommandAckSummary`; no Spring, no JPA), `application` (`dto`, `event`, `exception`, `service` +
`service/impl`: `PossessionService`, `FieldCommandService`, `FieldEventService`,
`FieldEventSynchronizer`, `PossessionBoardService`, `LivenessRegistry`, `MaintenanceClient` with
the `NoOpMaintenanceClient`, `FieldCodeGenerator`), `infrastructure/persistence` (`entity`,
`repository`), `infrastructure/grpc/{advice,mapper,metrics,stream}` and
`configuration/{grpc/FieldProperties,maintenance/MaintenanceProperties,scheduling}`. Phase 2 adds
`infrastructure/maintenance/RestClientMaintenanceClient`. No MapStruct: the protobuf builders are
not beans, the DTO → protobuf mapping is by hand (`FieldProtoMapper`). `Possession`, `FieldCommand`
and `CommandAck` also exist as generated messages: the entities that clash carry the suffix
`Record`, and the gRPC layer only sees DTOs.

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
- **The board is conflated.** Recompute is coalesced per possession (N heartbeats are not N
  recomputes), a watcher keeps only the latest board and a slow one skips versions; a watcher is
  registered before the first publication.

### Persistence rules

- UUID ids (`gen_random_uuid()`), `created_at`/`updated_at`/`created_by`/`updated_by` on every table
  (Spring Data auditing arrives with the entities in Phase 1), snake_case tables, PostgreSQL enums
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
- One class per layer, add methods rather than classes: `SecurityLayerTest` (converter, audience,
  `CurrentUserService`, properties), `GrpcServiceLayerTest` (`@AutoConfigureTestGrpcTransport`,
  in-process: authentication and the authorization of each RPC, including a stream). Phase 1 adds
  `DomainModelTest`, `BusinessLayerTest`, `FieldRepositoryDataJpaTest` (the gapless sequence with
  two threads, the watermark, the partial index) and the scenarios 3–10 of `GrpcServiceLayerTest`
  (acknowledgements, resumption, the catch-up race through a `CatchUpProbe` seam, duplicates,
  supersession, the slow watcher, backpressure). `DeviceStreamTest` is the planned exception to
  "one class per layer": a unit test of the stream with a fake `ServerCallStreamObserver`, like the
  outbox's own tests in `mto-maintenance`. The simulator (`src/test/java/.../simulator`, no Spring)
  is a tool, not a test.
