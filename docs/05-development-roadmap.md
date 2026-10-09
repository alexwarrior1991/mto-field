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

## Next

### Phase 1 · live channel with one replica

Every RPC but `SyncBufferedEvents`: entities and repositories, the domain rules (possession state
machine, shift validation, liveness, acknowledgement summary), the services (`open`/`close`,
`issue` with the gapless counter and the idempotency key, acknowledgements and clear-of-track
inline, task events queued), the `NoOpMaintenanceClient` with synthetic shifts, the streams core
(`DeviceStream`, registry, dispatcher after commit, per-device work queues, catch-up), the conflated
board, the `@GrpcAdvice` with `ErrorInfo`, the metrics, and the simulator (devices and the
supervisor's console, with cuts and a team that never acknowledges). Done when three simulated
teams show the right board and a device cut during the evacuation receives `EvacuateNow` once and
in order on resumption.

### Phase 2 · `mto-maintenance`

`RestClientMaintenanceClient`: `client_credentials` as `mto-field-svc`, circuit breaker
`maintenance`, the shifts of a possession read from the real service, `TaskStarted`/`TaskCompleted`
passed on, `FAILED` events retried (`app.maintenance.sync-retry.*`), `app.maintenance.enabled=true`
in `dev`.

### Phase 3 · the edges

`SyncBufferedEvents` (client streaming of the backlog), closing a stream with `UNAUTHENTICATED`
when its token expires (`app.field.token-expiry.*`), keepalive tuning, the size of the JWT in the
metadata, an integration test with Toxiproxy (cuts and latency), a group claim in the token.

### Phase 4 · several replicas (optional)

The per-JVM state (stream registry, liveness, dispatch lanes, board versions) over a RabbitMQ
fanout, so a possession's devices may connect to different replicas (`06-messaging.md`). The
schema already allows it: the commands are the truth, and resumption recovers.
