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

## Phase 4 · several replicas (done)

A possession's devices spread over several replicas and the supervisor's board watching from any,
with the per-JVM state shared over a RabbitMQ fanout and the database as the only truth
(`06-messaging.md`):

- `ReplicaBus` (`NoOpReplicaBus` with `app.rabbitmq.enabled=false`, `RabbitReplicaBus` otherwise):
  one fanout exchange, one exclusive auto-delete queue per replica, transient JSON envelopes with
  one body per `kind`, an unknown kind ignored; no signature, no outbox, no dead-letter queue, and
  the broker out of the health on purpose.
- `ReplicaRelay`: after the commit it tells the others every command (its number) and every close,
  the state of a device when it joins, heartbeats, closes or is written to, and says goodbye at
  shutdown; from the others it fans out the command read from the database, keeps the remote
  device states, supersedes a local stream when a newer one opened elsewhere, closes what they
  closed and forgets what a stopped replica told.
- `RemoteDeviceStates` merged into the board (local wins for an open stream here), with a TTL for
  replicas that die without saying goodbye.
- The catch-up tick (`app.field.replicas.catch-up`): commands the lane has not fanned out,
  possessions already closed, remote states expired — read from the database, always, bus or not.
- `mto-platform`: the broker in the `field` service and a second replica (`field-cluster`); the
  simulator spreads its devices over several `--target`s.
- Tests: `MessagingLayerTest` (the envelope, the publication, the consumer, the topology),
  `ReplicaClusterTest` (two contexts over an in-memory bus, also cut), `RabbitReplicaBusIT` (the
  real broker in CI).

## Phase 5 · the night told to `mto-notification`, and the device's own counter (done)

- The own events (`06-messaging.md`, *Published events*): `possession.opened`, `closed`,
  `evacuation-issued`, `evacuation-acknowledged`, `evacuation-unacknowledged` and `clear-of-track`,
  built in `FieldEvents` and published from the hooks inside the business transaction through the
  outbox copied from `mto-maintenance` (`outbox_message`, `V2`; relay with publisher confirms,
  `/actuator/outbox`, metrics, purge), on the exchange `mto.field.exchange` with the shared
  signature. One aggregate, the possession; the actor from the token of the gRPC call; the
  possession code as `correlationId`. Six examples in `docs/messaging/examples/`, the contract
  `mto-notification` copies.
- The ack watchdog (`EvacuationAckWatchdog`, `app.field.evacuation.*`): an evacuation whose teams
  have not all acknowledged after the timeout is told once, decided by the database
  (`field_command.ack_watched_at`, `V3`).
- The realm: `mto-field-supervisor` gains `notification-inbox` and `notification-activity-read`;
  the technician still gets nothing of notifications.
- The simulator persists each device's counter, last applied command and unconfirmed uploads
  (`--state-dir`) and resumes from them after a restart.
- `mto-notification`: the `field` source (queue `mto.notification.field.queue` on
  `mto.field.exchange`), the `FIELD` category, the adapter and the rules; `mto-platform`: the
  signature secret in the `field` service; the frontends: the category label.
- Tests: the outbox's own tests, `OutboxRabbitIT`, the hooks in `BusinessLayerTest` and the whole
  night in `GrpcServiceLayerTest`, the envelope in `MessagingLayerTest`,
  `MessagingContractExamplesTest`, the watchdog in `FieldRepositoryDataJpaTest` and
  `BusinessLayerTest`, `DeviceStateStoreTest`.

## Next

Nothing planned. What a later phase could add, if ever needed: the board as a server-side merge
instead of a per-replica view (today the board `version` is per replica), and the push channel and
the `TEAM`/`ZONE` audiences on the `mto-notification` side, so that an evacuation also reaches the
technicians' devices.
