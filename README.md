# MTO Field

Spring Boot 4.1 / Java 25 **gRPC** service: the live console of a night track possession of the
MTO domain. `mto-maintenance` keeps the record of a shift (its tasks, defects and materials);
`mto-field` keeps the channel open between the devices of the teams on the track and the person in
charge of the possession while it lasts: who is connected and at which kp, which tasks start and
finish and, above all, the **evacuation order with a nominal acknowledgement** from every team.
Informational only: it controls neither voltage nor SCADA.

It is a practice project of gRPC, and it uses the four kinds of call with a real reason for each:
unary (open and close a possession, issue a command), server streaming (the possession board),
bidirectional (the team channel of a device) and, in Phase 3, client streaming (the backlog a
device accumulated without coverage).

It is a sibling of [`mto-maintenance`](https://github.com/alexwarrior1991/mto-maintenance), whose
shifts it groups into a possession, published with
[`mto-platform`](https://github.com/alexwarrior1991/mto-platform).
[`mto-gateway`](https://github.com/alexwarrior1991/mto-gateway) does **not** route gRPC: clients
reach the gRPC port directly.

Functional and technical documentation lives in [`docs/`](docs/README.md). Today (Phases 0 and 1)
every RPC but `SyncBufferedEvents` works on one replica, with the maintenance client still off;
[`docs/05-development-roadmap.md`](docs/05-development-roadmap.md) says what is done and what
each next phase adds.

## Requirements

- JDK 25
- PostgreSQL 17 (any 16+ works; `mto-platform` runs 17)
- Docker, for the Testcontainers-backed tests and for the local environment
- Keycloak and, from Phase 2, `mto-maintenance`, both provided by `mto-platform`
- `grpcurl`, optional, to talk to the service by hand ([`docs/grpc/field-api.md`](docs/grpc/field-api.md))

## Configuration

Everything is read from the environment; `.env.example` lists every variable with its local value.
The ones without a default in the `prod` profile come first:

| Variable | What |
|---|---|
| `DATABASE_URL`, `DATABASE_USERNAME`, `DATABASE_PASSWORD` | Application database (`mto_field`, user `mto_field_user` in `mto-platform`) |
| `KEYCLOAK_ISSUER_URI` | Realm that issues the tokens (`http://auth.mto.local:8082/realms/mto`) |
| `KEYCLOAK_CLIENT_ID`, `KEYCLOAK_AUDIENCE` | `mto-field-api` |
| `KEYCLOAK_TOKEN_URI` | Token endpoint of the realm, for the service account |
| `KEYCLOAK_SERVICE_CLIENT_ID`, `KEYCLOAK_SERVICE_CLIENT_SECRET` | Service account `mto-field-svc` used to call `mto-maintenance` (Phase 2) |
| `MTO_MAINTENANCE_URL` | Base URL of `mto-maintenance` (`http://localhost:8083` from the IDE) |
| `SERVER_PORT`, `SPRING_GRPC_SERVER_PORT` | HTTP (Actuator) and gRPC ports inside the process: `8087` / `9094` in `dev`, `8080` / `9090` in the image |
| `APP_PORT`, `APP_GRPC_PORT` | Host ports published by `compose.yaml` here (`8087` / `9094`) |
| `APP_MAINTENANCE_ENABLED` | `false` leaves the `NoOp` maintenance client (synthetic shifts); `false` in `dev` during Phase 1 |
| `APP_MAINTENANCE_SYNC_RETRY_ENABLED`, `APP_MAINTENANCE_SYNC_RETRY_INTERVAL` | Retry of the task events `mto-maintenance` did not answer (`PT1M`) |
| `APP_MAINTENANCE_CONNECT_TIMEOUT`, `APP_MAINTENANCE_READ_TIMEOUT`, `APP_MAINTENANCE_CB_*` | Timeouts and the thresholds of the circuit breaker `maintenance` |
| `SPRING_GRPC_SERVER_KEEPALIVE_TIME`, `_TIMEOUT`, `_PERMIT_TIME` | Transport keepalive (`20s`, `10s`, `10s`); not the application heartbeat |
| `SPRING_GRPC_SERVER_REFLECTION_ENABLED` | Server reflection, what `grpcurl` uses: on in `dev`, off in `prod` |
| `SPRING_GRPC_SERVER_SHUTDOWN_GRACE_PERIOD` | Time given to the open streams at shutdown (`30s`) |
| `APP_FIELD_OUTBOUND_QUEUE_CAPACITY`, `APP_FIELD_WORK_QUEUE_CAPACITY` | Per-stream outbound queue (`256`) and per-device work queue (`64`) |
| `APP_FIELD_BOARD_TICK` | How often the board is marked dirty for the liveness to decay (`5s`) |
| `APP_FIELD_TOKEN_EXPIRY_ENABLED` | Phase 3: close a stream when its token expires (`false`) |
| `KEYCLOAK_AUDIENCE_VALIDATION_ENABLED` | `true`; `false` only in the `test` profile |
| `SPRING_FLYWAY_ENABLED`, `SPRING_FLYWAY_LOCATIONS` | Migrations (`classpath:db/migration`) |
| `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE`, `MANAGEMENT_ENDPOINT_HEALTH_SHOW_DETAILS` | Actuator exposure (`health,info,metrics,prometheus`) and health detail |
| `MTO_TRACING_ENABLED`, `MTO_TRACING_SAMPLING_PROBABILITY`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Tracing (`0.1`, collector of `mto-platform`) |
| `LOGGING_LEVEL_ROOT`, `LOGGING_LEVEL_APP`, `LOGGING_LEVEL_SQL`, `JPA_SHOW_SQL`, `JPA_FORMAT_SQL` | Logging |
| `JAVA_OPTS`, `SHUTDOWN_TIMEOUT`, `APP_VERSION` | JVM options, graceful shutdown timeout (`30s`), the version `/actuator/info` reports |

## Spring profiles

`dev` (HTTP 8087, gRPC 9094, application logs at `DEBUG`, reflection on, health details shown,
maintenance client **off**: the `NoOp` client answers synthetic shifts, so a simulator can use any
shift id; with `APP_MAINTENANCE_ENABLED=true` the application refuses to start until Phase 2 brings
the REST client, and says so), `test` (random ports, audience validation off, maintenance off, board tick 1 s,
`TEST_DATABASE_*` honoured; the suite sets the same things explicitly and does not depend on it) and `prod` (graceful shutdown, no defaults for secrets, URLs
and the database, reflection off, health details hidden). Without `SPRING_PROFILES_ACTIVE` the
application starts in `dev`.

## Run locally

```bash
cd ../mto-platform && docker compose --profile all up -d && ./keycloak/apply-partials.sh
cd ../mto-field
export SPRING_PROFILES_ACTIVE=dev
export DATABASE_URL=jdbc:postgresql://localhost:5432/mto_field
export DATABASE_USERNAME=mto_field_user DATABASE_PASSWORD=mto_field_password
export KEYCLOAK_ISSUER_URI=http://auth.mto.local:8082/realms/mto
export KEYCLOAK_TOKEN_URI=http://auth.mto.local:8082/realms/mto/protocol/openid-connect/token
export KEYCLOAK_SERVICE_CLIENT_SECRET=mto-field-svc-secret   # keycloak/mto-field-dev.json
./mvnw spring-boot:run
```

`auth.mto.local` must resolve to the host (`127.0.0.1 auth.mto.local` in `/etc/hosts`): the token
`iss` carries that name and the application fetches the JWK Set from it. The HTTP port is only
Actuator; the API listens on the gRPC port, `9094`.

The whole stack, this service included, also runs from `mto-platform` with
`docker compose --profile field up -d` (published image, host ports `MTO_FIELD_PORT` 8087 and
`MTO_FIELD_GRPC_PORT` 9094). `compose.yaml` here builds and runs a local image against that
infrastructure:

```bash
cp .env.example .env    # fill DATABASE_*, KEYCLOAK_SERVICE_CLIENT_SECRET
docker compose up -d --build
```

The database `mto_field` and its user are created by `mto-platform/postgres/init/01-databases.sql`,
which PostgreSQL runs **only when the volume is created**. On a stack that already existed, either
`docker compose --profile all down -v` (data is lost) or, as the platform README documents, recreate
`postgres` so it carries the new variables and run the script again inside the container:

```bash
cd ../mto-platform
docker compose up -d --force-recreate postgres
docker compose exec postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/01-databases.sql'
```

## The simulator

`src/test/java/com/alejandro/mtofield/simulator` (no Spring) plays the supervisor and the teams
against a running server: it opens a possession, prints every version of the board, heartbeats,
starts and completes tasks, orders the evacuation, acknowledges (except the team told not to),
clears the track, cuts the coverage every N seconds and resumes, and exits with `1` if any device
received a command twice or out of order. Against the platform:

```bash
./mvnw -q test-compile exec:java -Dexec.classpathScope=test \
  -Dexec.args="--mode demo --target localhost:9094 --user campo.responsable --password local --teams 3 --never-ack-team 3 --cut-every 15s --mixed"
```

Without Keycloak, the simulator can be the issuer: it serves the JWK Set of the test key on
`localhost:8082` and mints its own tokens, so the server only needs to be told that issuer:

```bash
./mvnw -q package -DskipTests
KEYCLOAK_ISSUER_URI=http://localhost:8082/realms/mto DATABASE_PASSWORD=… MTO_TRACING_ENABLED=false \
  java -jar target/mto-field-*.jar --spring.profiles.active=dev &
./mvnw -q test-compile exec:java -Dexec.classpathScope=test \
  -Dexec.args="--mode demo --local-issuer --teams 3 --never-ack-team 3 --cut-every 15s --mixed --evacuate-after 20s --duration 60s"
```

`--mode supervisor` and `--mode device --shifts …` split the two halves across JVMs; `--help`
lists the options, and [`docs/grpc/field-api.md`](docs/grpc/field-api.md) explains them.

## Database migrations

Flyway, `src/main/resources/db/migration`. Hibernate validates the schema on boot, so every change
is a new `V<n>__*.sql`. `V1` creates the whole schema: the possession and its shifts, the downstream
commands with their gapless sequence, the acknowledgements and the events the devices upload.
Details in [`docs/03-database.md`](docs/03-database.md). There are no Envers tables, on purpose
([`docs/07-auditing.md`](docs/07-auditing.md)).

## The gRPC API

One service, `mto.field.v1.FieldService` (`src/main/proto/mto/field/v1/field_service.proto`), with
six RPCs: `OpenPossession`, `ClosePossession` and `IssueCommand` (unary), `WatchPossessionBoard`
(server streaming), `TeamChannel` (bidirectional, one stream per device for the whole night) and
`SyncBufferedEvents` (client streaming, Phase 3). Server reflection and the standard health service
are on and open in `dev`; reflection is off in `prod`. There is no HTTP API and no Swagger: the
contract is the `.proto`, summarised in [`docs/04-grpc-api.md`](docs/04-grpc-api.md), and
[`docs/grpc/field-api.md`](docs/grpc/field-api.md) walks through it with `grpcurl`.

## Security

Keycloak resource server, audience `mto-field-api`, for the gRPC server and for Actuator alike.
Permissions are client roles, profiles are realm composites (`mto-field-technician`,
`mto-field-supervisor`); see [`keycloak/README.md`](keycloak/README.md). In gRPC there are no verbs
or routes, so the permission of each RPC is a `@PreAuthorize` on its method:

| RPC | Permission |
|---|---|
| `TeamChannel`, `SyncBufferedEvents` | `field-team` |
| `OpenPossession`, `ClosePossession`, `IssueCommand`, `WatchPossessionBoard` | `field-supervise` |
| `grpc.health.v1.Health/*`, `grpc.reflection.*/ServerReflection/*` | none |
| `/actuator/**` (except health/info) | `ops-metrics` (reads) / `ops-write` (`POST`, `DELETE`) |

A call without a token is `UNAUTHENTICATED`; with a valid token of the realm issued for another
audience, too; with the token but without the role, `PERMISSION_DENIED`. The service account
`mto-field-svc` carries an audience mapper to `mto-maintenance-api`, and
`mto-platform/keycloak/apply-partials.sh` grants it `maintenance-read` and `maintenance-write`.

Dev users (password `local`): `campo.tecnico1` and `campo.tecnico2` (`mto-field-technician`),
`campo.responsable` (`mto-field-supervisor`). A token for a person comes from the password grant
of the client `mto-frontend` of the local realm:

```bash
TOKEN=$(curl -s -X POST http://auth.mto.local:8082/realms/mto/protocol/openid-connect/token \
  -d grant_type=password -d client_id=mto-frontend \
  -d username=campo.responsable -d password=local | jq -r .access_token)
grpcurl -plaintext -H "Authorization: Bearer $TOKEN" localhost:9094 \
  mto.field.v1.FieldService/OpenPossession -d '{"shift_ids": ["11111111-1111-4111-8111-111111111111"]}'
```

## Actuator and tracing

`/actuator/health` (with the `liveness` and `readiness` probes) and `/actuator/info` are public;
`metrics` and `prometheus` need `ops-metrics`. The circuit breaker `maintenance` reports into the
health. Traces go to the OTLP collector of `mto-platform` (`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`,
`spring-boot-starter-opentelemetry`), with the gRPC server observed
(`spring.grpc.server.observation.enabled`); metrics are scraped from `/actuator/prometheus`, the
OTLP metrics export is off. The metrics of the channel (`FieldMetrics`: `field.streams.open`,
`field.stream.outbound.depth`, `field.stream.not_ready`, `field.work_queue.depth`,
`field.teams.connected`, `field.commands.pending_ack`, `field.command.ack.time`) are on the same
endpoint, and each processed message is one observation (`field.event`, tagged by kind): a span
per call is useless for a stream that lasts the whole night.

## Running tests

```bash
./mvnw test                     # Testcontainers: postgres:17-alpine
./mvnw verify                   # + failsafe (*IT), none yet; Phase 3 adds the Toxiproxy one
```

`MtoFieldApplicationTests` boots the whole context against a real PostgreSQL and checks the health
and the reflection services over Netty without a token (the local stand-in for `grpcurl list`);
`GrpcServiceLayerTest` drives the service over the in-process transport with tokens signed by an
in-test RSA key and validated by the production validator chain. Without Docker, point the suite at
any PostgreSQL:

```bash
TEST_DATABASE_URL=jdbc:postgresql://localhost:5432/mto_field_test \
TEST_DATABASE_USERNAME=mto_field TEST_DATABASE_PASSWORD=mto_field ./mvnw test
```

Without Docker and without that variable the PostgreSQL-backed tests are skipped, not failed.

## Roadmap

Phase 0 (done): skeleton, contract, schema, security, image, compose and CI. Phase 1 (done): every
RPC but `SyncBufferedEvents`, the streams core, the board and the simulator. Phase 2: the REST
client of `mto-maintenance` (until then `APP_MAINTENANCE_ENABLED` stays `false`). Phase 3: `SyncBufferedEvents`, token-expiry close, keepalive tuning, the
Toxiproxy IT. Phase 4 (optional): several replicas over a RabbitMQ fanout. The detail is in
[`docs/05-development-roadmap.md`](docs/05-development-roadmap.md).
