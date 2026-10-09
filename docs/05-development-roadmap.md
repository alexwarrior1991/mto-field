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

On the platform side (`mto-platform`), the `field` service runs with the maintenance client off
(`MTO_FIELD_MAINTENANCE_ENABLED=false`) until Phase 2 flips it.

## Next

### Phase 2 · `mto-maintenance`

`RestClientMaintenanceClient`: `client_credentials` as `mto-field-svc`, circuit breaker
`maintenance`, the shifts of a possession read from the real service, `TaskStarted`/`TaskCompleted`
passed on, `FAILED` events retried (`app.maintenance.sync-retry.*`), `app.maintenance.enabled=true`
in `dev` and in the platform compose (`MTO_FIELD_MAINTENANCE_ENABLED`), the fail-fast bean of
`MaintenanceClientConfiguration` replaced by the real client.

### Phase 3 · the edges

`SyncBufferedEvents` (client streaming of the backlog), closing a stream with `UNAUTHENTICATED`
when its token expires (`app.field.token-expiry.*`), keepalive tuning, the size of the JWT in the
metadata, an integration test with Toxiproxy (cuts and latency), a group claim in the token.

### Phase 4 · several replicas (optional)

The per-JVM state (stream registry, liveness, dispatch lanes, board versions) over a RabbitMQ
fanout, so a possession's devices may connect to different replicas (`06-messaging.md`). The
schema already allows it: the commands are the truth, and resumption recovers.
