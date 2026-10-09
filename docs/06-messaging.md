# 06 · Messaging

`mto-field` has **no broker**, on purpose, until Phase 4: no RabbitMQ dependency, no queue, no
exchange, no outbox, no inbox. The CI, the image and the local environment run without one.

## Why not

- Everything the service tells a device travels on the device's own stream, and everything a
  device tells the service arrives on it. The truth is the database: `field_command` is the
  ordered, gapless log of what was sent, `field_event` the idempotent log of what was received,
  and resumption (`Join.last_command_sequence`) recovers whatever a stream lost. An outbox would
  duplicate that log.
- What `mto-field` needs from `mto-maintenance` (the shifts of a possession) and tells it (a task
  started or completed) is a request with an answer: a REST call with the service account, with a
  circuit breaker and a retry of what was not answered (Phase 2). The master-data events of
  `mto-configuration` are not needed here.
- With one replica, the state that is not in the database (open streams, liveness, the dispatch
  lane of each possession, the board versions) lives in the JVM and needs no transport.

The consumers of the domain's events (`mto-notification`) hear about a night's work from
`mto-maintenance`, which publishes the shift, the order and the task outcomes; nothing of that is
published twice from here.

## What Phase 4 would add

Several replicas, with a possession's devices connected to different ones and the supervisor's
board watching from any. The per-JVM state is the subject:

- the `CommandCommitted` event of a possession, published after commit to a RabbitMQ **fanout**
  (every replica receives every command and fans it out to the streams it holds, with the same
  dedupe by last enqueued sequence that already makes a resend harmless);
- liveness and clear-of-track shared the same way, so the board of any replica is complete;
- the per-possession counter stays in the database, where it already serializes the issuers across
  replicas.

The schema needs no change for it: the commands are the truth, and a stream that misses a fanout
message recovers by resumption. The decision, and the topology, are taken when that phase starts.

## What this means today

- There is no `app.rabbitmq.*` property, no `spring-boot-starter-amqp` in the `pom.xml` and no
  RabbitMQ container in `compose.yaml` or in the CI; `MtoFieldApplicationTests` boots the whole
  context with nothing switched off on that side.
- A new event of this service, if one is ever needed by `mto-notification`, would follow the
  siblings' shape (`DomainEvent` in the `AsynchronousMessage` envelope, through an outbox) and
  would be decided then, not added ahead of a consumer.
