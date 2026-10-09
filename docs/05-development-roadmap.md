# 05 · Development roadmap

## Done (Phase 0 · scaffolding)

- `pom.xml` in the shape of `mto-maintenance`: Spring gRPC server, protobuf generation, no AMQP,
  no MapStruct, no springdoc, no Envers; `exec-maven-plugin` ready for the simulator.
- The contract, `src/main/proto/mto/field/v1/field_service.proto`: the six RPCs and their messages.
- The schema, `V1__create_field_schema.sql`: possession, its shifts, the commands with the gapless
  counter, the acknowledgements, the events (`03-database.md`).
- Configuration by profile (`dev` 8087/9094, `test`, `prod`), transport keepalive, reflection and
  health, graceful shutdown, Actuator and OTLP tracing.
- Security: the HTTP chain for Actuator only; the gRPC interceptor with health and reflection open
  and everything else authenticated; one `@PreAuthorize` per RPC; the JWK Set fetched lazily; the
  validator chain shared with the test decoder.
- The virtual-thread executors of the gRPC server and of the streams work.
- Keycloak: `mto-field-api` with its four roles, the profiles `mto-field-technician` and
  `mto-field-supervisor`, the service account `mto-field-svc` with the audience to
  `mto-maintenance-api`, the dev users.
- Image (`temurin-25`, ports 8080/9090), app-only `compose.yaml`, CI with the test job and the image
  job whose smoke test runs `grpcurl list` and the health check against the container.
- Tests: `MtoFieldApplicationTests`, `SecurityLayerTest`, `GrpcServiceLayerTest` (authentication
  and authorization per RPC), with `TestTokens` and `TestJwtDecoderConfiguration`.

The platform side lives in `mto-platform`, done on the branch of the same name: the compose
profile `field` (ports `MTO_FIELD_PORT` 8087 and `MTO_FIELD_GRPC_PORT` 9094, `build` from
`../mto-field` until GHCR has the image), the database `mto_field` in its Postgres init, the step
of `apply-partials.sh` that grants `mto-field-svc` its roles on `mto-maintenance-api`, the audience
mapper `audiencia-mto-field-api` in the login clients (`mto-frontend` in both realm files, and
`mto-backoffice` in its own repository), `mto-field-api` in its consistency scripts, and
`mto-field` in its CI and in `e2e.sh`. Its CI resolves the sibling branches by name, so this branch
and the backoffice one are pushed before the platform one.

## Done (Phase 1 · live channel with one replica)

Every RPC but `SyncBufferedEvents`, on one replica:

- The entities and repositories of `V1`, with the idempotency and the ordering inside the SQL:
  the gapless counter (`update … returning`, proven with two emitters and a rollback), `on conflict
  do nothing` for events and acknowledgements, the conditional updates, the contiguous watermark,
  the paged replay.
- The domain rules: which shifts can be grouped and the default window, the close (all clear or
  forced with a reason), the liveness thresholds, the acknowledgement summary per team.
- The services: `open`/`close` (the shifts read through `MaintenanceClient`, the partial unique
  index as the guarantee, the close on the same row lock as the counter, `PossessionClosed` after
  commit), `issue` (one transaction that takes the number, stores the `FieldCommand` as JSON and
  publishes `CommandCommitted` for after the commit; the idempotency key resolved before and
  re-read after a violation), the events (acknowledgements and clear-of-track applied inline and
  answered in-band; task events stored `PENDING` and queued), the Phase 1 synchronizer
  (`PENDING_SYNC`), the in-memory liveness, the `NoOpMaintenanceClient` with deterministic
  synthetic shifts and the refusal to start with `app.maintenance.enabled=true`.
- The streams core (`CLAUDE.md`, *Streams*): `DeviceStream` with one writer, the bounded outbound
  queue, catch-up with held commands, dedupe by last enqueued sequence, supersession; the registry
  with one lane per possession created at registration; the dispatcher after commit, off the
  committing thread, filling gaps from the database; the per-device work queues on virtual threads
  without context, retired atomically; `TeamChannels` with manual flow control, the principal
  captured at open, `Join` first, register before replay, errors in-band.
- The board: coalesced recompute, `BoardWatcher` holding only the latest board, the tick while
  someone watches, the last board and `onCompleted` on close, `sent_to` counting what a resumed
  device declared applied.
- `GrpcErrors` and `FieldGrpcExceptionAdvice` (`google.rpc.ErrorInfo` with a stable `reason`), the
  metrics of `FieldMetrics`, one observation per processed message.
- The simulator, two client styles, `--local-issuer` for a machine without Keycloak. Checked by
  hand: three teams, the one that never acknowledges stays `pending` (`sent_to` while connected,
  `queued_for` while cut), and a device cut during the evacuation receives `EvacuateNow` once and in
  order on resumption; the forced close ends every stream and the board.
- Tests: `DomainModelTest`, `BusinessLayerTest`, `FieldRepositoryDataJpaTest`, `DeviceStreamTest`,
  the scenarios of `GrpcServiceLayerTest` (Join and Welcome, evacuation with acks and close,
  resumption, the catch-up race through the `CatchUpProbe` seam, duplicates and the watermark,
  supersession, the board and its slow watcher, backpressure) and the bean list of
  `MtoFieldApplicationTests`.

## Phase 2 · `mto-maintenance` (done)

- `RestClientMaintenanceClient` (`infrastructure/maintenance`): `GET /shifts/{id}`,
  `GET /orders/{id}/tasks/{taskId}`, `POST .../start`, `POST .../complete` of `mto-maintenance`,
  with the service account `mto-field-svc` (`client_credentials`, the manager built in
  `MaintenanceClientConfiguration` because no call leaves from an HTTP request) inside the circuit
  breaker `maintenance`, which ignores the business rejections. `MaintenanceRejectedException`
  (a 4xx other than 401/403/408/429, with mto-maintenance's `errorCode`) and
  `MaintenanceUnavailableException` (everything else), mapped by the advice to
  `FAILED_PRECONDITION` and `UNAVAILABLE`.
- `MaintenanceEventSynchronizer` replaces `PendingSyncEventSynchronizer` when the client is on:
  `TaskStarted` → `start` with the shift and the person; `TaskCompleted` → `complete` with the
  types, the notes, the inline defects (`DEFECT_SEVERITY_HIGH` → `HIGH`) and the photos; a lost
  answer (409 `TRN-001`) reconciled by reading the task; `SYNCED` + `APPLIED`, `REJECTED` with the
  code, or `FAILED` + `PENDING_SYNC` (`CLAUDE.md`, *Synchronization with `mto-maintenance`*).
- `FieldEventSyncRetryServiceImpl` and `FieldEventSyncRetryConfiguration`: every
  `app.maintenance.sync-retry.interval`, the `FAILED` and the overdue `PENDING` events in order of
  arrival, stopping at the first one `mto-maintenance` does not answer.
- `app.maintenance.enabled=true` in `dev`, in `.env.example`, in `compose.yaml`, in the CI smoke
  test and in the platform compose (`MTO_FIELD_MAINTENANCE_ENABLED`); the fail-fast bean is gone.
- Tests: `MaintenanceClientTest` (the contract against `MockRestServiceServer`, the circuit, the
  token without an HTTP request), the synchronizer's outcome table and the retry in
  `BusinessLayerTest`, the wiring of both modes of the switch, and the scenarios of
  `GrpcServiceLayerTest` in a second context with the client on and `mto-maintenance` mocked
  (applied, rejected and final, down then resolved by the retry, a possession that needs the
  shift). Not checked in this environment: a real `mto-maintenance` behind the client (no Docker);
  the CI `e2e` of `mto-platform` brings both up.

## Phase 3 · the edges (done)

- `SyncBufferedEvents` (`SyncSessions`): a `Join`, then the backlog in strictly increasing
  sequence (`OUT_OF_ORDER` otherwise), applied as on the live channel, answered with
  `SyncResult{last_applied_sequence, applied, duplicates, rejected}` (`rejected` added to the
  contract). The protocol rule on the `TeamChannel`: a work event that leaves a gap over the
  contiguous watermark is `REJECTED` with `BACKLOG_PENDING`; acknowledgements and clear-of-track
  always go through. The simulator resends acks on the channel and uploads work through the sync,
  buffering new work meanwhile.
- The close by token expiry: `TokenExpirySweeper` and `TokenExpiryConfiguration`
  (`app.field.token-expiry.*`, on by default, every 30 s) end every stream and board watcher whose
  token expired with `UNAUTHENTICATED` `TOKEN_EXPIRED` (metadata `expired_at`); the device resumes
  with a fresh token. The keepalive table of `04-grpc-api.md`, with
  `keepalive.connection.max-idle-time` (5 min) and the 10 s floor of grpc-java.
- The team of the token: `app.field.team-binding.*` and `TeamBinding`; the groups `EQ-NORTE` and
  `EQ-SUR` in the realm partial, one development technician in each, and the group-membership
  mapper `grupos` on the login client `mto-frontend` in `mto-platform`.
- The size of the JWT: `SecurityLayerTest` mints the worst-case token of the realm and keeps it
  under half of `spring.grpc.server.inbound.metadata.max-size` (3945 of 8192 bytes).
- `NetworkResilienceIT` with Toxiproxy (`ToxiproxyGateway`: the container with Docker,
  `TOXIPROXY_URL` to a local server without it, skipped with neither): the cut mid-evacuation,
  latency with jitter and 16 KB/s, a peer that goes mute without closing, the client ping within
  the permitted rate.

## Next

### Phase 4 · several replicas (optional)

The per-JVM state (stream registry, liveness, dispatch lanes, board versions) over a RabbitMQ
fanout, so a possession's devices may connect to different replicas (`06-messaging.md`). The
schema already allows it: the commands are the truth, and resumption recovers.
