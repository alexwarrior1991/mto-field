# 01 · Architecture

## Stack

Java 25, Spring Boot 4.1, Spring gRPC (grpc-java on Netty; in-process transport for the tests),
protobuf (`protobuf-maven-plugin` from Boot's plugin management, `src/main/proto`), Spring Data JPA,
Flyway, PostgreSQL, Lombok, Spring Security (OAuth2 resource server on both the gRPC server and the
HTTP chain, OAuth2 client for the outgoing service account), Spring `RestClient` + Spring Cloud
CircuitBreaker (Resilience4j) for `mto-maintenance` (Phase 2), Micrometer/OpenTelemetry,
Testcontainers. On purpose **without** MapStruct (protobuf builders are not beans; the mapping is
by hand), springdoc (there is no HTTP API), Envers (`07-auditing.md`) and AMQP (`06-messaging.md`).

## Layers

```
com.alejandro.mtofield
├── grpc.v1                 generated from src/main/proto/mto/field/v1/field_service.proto (java_package)
├── configuration
│   ├── security            SecurityConfiguration (HTTP chain: Actuator only; JwtDecoder; the static jwtValidator),
│   │                       GrpcSecurityConfiguration (the global AuthenticationProcessInterceptor),
│   │                       KeycloakJwtAuthenticationConverter, JwtAudienceValidator, SecurityProperties, SecurityRoles,
│   │                       SecurityAuthorityPrefixes, JwtClaimNames, CurrentUserService
│   └── grpc                GrpcServerConfiguration: virtual-thread executor of the server + fieldStreamExecutor
└── infrastructure.grpc     FieldGrpcService (@GrpcService, @PreAuthorize per RPC; UNIMPLEMENTED bodies)
```

Phase 1 completes the three layers of `mto-maintenance`:

```
├── domain.model            PossessionStateMachine, PossessionRules, TeamLiveness, CommandAckSummary (no Spring, no JPA)
├── application
│   ├── dto                 snapshots and drafts the gRPC layer and the services exchange (never protobuf, never entities)
│   ├── event               CommandCommitted, PossessionClosed (Spring application events, delivered AFTER_COMMIT)
│   ├── exception           business exceptions mapped to a gRPC Status by the advice
│   ├── service             PossessionService, FieldCommandService, FieldEventService, FieldEventSynchronizer,
│   │                       PossessionBoardService, LivenessRegistry, MaintenanceClient, FieldCodeGenerator
│   └── service.impl        package-private implementations; NoOpMaintenanceClient (app.maintenance.enabled=false)
├── infrastructure
│   ├── persistence.entity | .repository     Possession, PossessionShift, FieldCommandRecord, CommandAckRecord, FieldEventRecord
│   ├── grpc.advice         FieldGrpcExceptionAdvice: exception -> Status + google.rpc.ErrorInfo
│   ├── grpc.mapper         FieldProtoMapper: DTO -> protobuf by hand
│   ├── grpc.metrics        FieldMetrics
│   ├── grpc.stream         DeviceStream, DeviceStreamRegistry, CommandDispatcher, TeamChannelHandler, DeviceWorkQueues,
│   │                       BoardWatcher, BoardWatcherRegistry, PossessionLifecycleListener
│   └── maintenance         RestClientMaintenanceClient (Phase 2)
└── configuration           grpc.FieldProperties (app.field.*), maintenance.MaintenanceProperties (app.maintenance.*),
                            scheduling (board tick; token-expiry sweep in Phase 3), JPA auditing
```

Rules that keep the layers honest:

- `FieldGrpcService` talks to service interfaces and maps DTOs to protobuf; entities never reach
  the gRPC layer, and protobuf never reaches the services. `Possession`, `FieldCommand` and
  `CommandAck` exist as generated messages, so the entities that clash carry the suffix `Record`.
- A business rule that closes a call is an exception translated by the advice; one that answers a
  message inside a stream is an in-band `EventResult{REJECTED}`, never a `Status`.
- Idempotency and ordering live in the writing SQL statement (`on conflict do nothing`,
  `update ... where ... returning`), never in a read-then-write.
- The streams core never writes to an observer from the offering thread, and never trusts the
  `SecurityContext` in a thread it created (`CLAUDE.md`, *Streams*).

## Threads

Boot does not give the gRPC server virtual threads on its own: `GrpcServerConfiguration` provides
a `GrpcServerExecutorProvider` with a virtual thread per task, which is what hundreds of streams
that spend the night waiting want; grpc-java serializes the callbacks of a call, so the order per
stream is kept. A second executor, `fieldStreamExecutor`, takes what the service moves off the
callback thread: the catch-up of a stream, the dispatch of commands after commit, the recompute of
the board. Tomcat (Actuator) and the scheduled tasks use `spring.threads.virtual.enabled`.

## Integrations

| Direction | Peer | Mechanism |
|---|---|---|
| Inbound | devices, the simulator, the supervisor's console | gRPC on `9094` (host) / `9090` (container), JWT of the `mto` realm with audience `mto-field-api` in the `Authorization` metadata |
| Outbound | `mto-maintenance` | REST, service account `mto-field-svc`, circuit breaker `maintenance` (Phase 2; a `NoOp` client with synthetic shifts until then) |
| None | RabbitMQ | No broker until Phase 4 (`06-messaging.md`) |

## Cross-cutting

- **Security**: the HTTP chain covers Actuator only; the gRPC interceptor leaves health and
  reflection open and authenticates everything else; the permission of each RPC is a
  `@PreAuthorize` (`04-grpc-api.md`, *Security*).
- **Errors**: inside a stream, in-band `EventResult`s; what closes a call is a `Status` with a
  `google.rpc.ErrorInfo` (`reason`, `domain mto-field`) via `StatusProto` (Phase 1).
- **Observability**: Actuator health/info public; metrics and prometheus behind `ops-metrics`;
  traces by OTLP with the gRPC server observed; one observation per processed event rather than a
  span per hour-long call. Phase 1 adds the channel metrics (teams connected, commands with a
  pending acknowledgement, time to acknowledge, outbound queue depth, time not ready).
