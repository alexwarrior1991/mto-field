# Field channel

Documentation of `mto-field`, in reading order:

| Document | Content |
|---|---|
| [00-project-overview.md](00-project-overview.md) | What the service is for and what it deliberately leaves to its siblings |
| [01-architecture.md](01-architecture.md) | Layers, packages, threads, integrations |
| [02-domain-model.md](02-domain-model.md) | Possession, shifts, commands, acknowledgements, events, liveness |
| [03-database.md](03-database.md) | Schema `V1`: enums, sequence, tables, constraints |
| [04-grpc-api.md](04-grpc-api.md) | The six RPCs, resumption, idempotency, status codes, permissions |
| [05-development-roadmap.md](05-development-roadmap.md) | What is done (Phase 0) and what each next phase adds |
| [06-messaging.md](06-messaging.md) | No broker until Phase 4, and what it would add |
| [07-auditing.md](07-auditing.md) | Audit columns, and why there is no Envers |
| [grpc/field-api.md](grpc/field-api.md) | `grpcurl` cookbook: a token, the possession, the board, the team channel |
