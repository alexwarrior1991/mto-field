# 06 · Messaging

`mto-field` has two uses of the broker. Since Phase 4, the **bus between its own replicas**, a
RabbitMQ fanout through which the replicas tell each other what is not in the database. Since
Phase 5, its **own events for `mto-notification`**: what a night tells the outside world, through
an outbox and an exchange of its own (*Published events*, below). It has no queue of anyone else's
and no inbox: it consumes nothing.

## The principle: the database is the truth, the broker only accelerates

Everything a device must receive is in `field_command`, with a gapless sequence per possession;
everything a device uploaded is in `field_event`; the acknowledgements and the clear-of-track are
rows. Resumption (`Join.last_command_sequence`) recovers whatever a stream lost. What is **not** in
the database is the state each JVM keeps: which devices have a stream open on this replica, their
last heartbeat (kp, battery, signal), how far each stream has been written (`sent_to`), the
dispatch lane of each possession and the board versions.

With several replicas (`docs/05-development-roadmap.md`, Phase 4) a possession's devices connect
to any of them and the supervisor's board watches from any. For that the replicas share that
per-JVM state over the bus, and two rules keep the bus from being load-bearing:

- **A lost message loses nothing.** Every `app.field.replicas.catch-up` (2 s) each replica rereads
  the database: for every possession it holds a lane of, `max(sequence)` against the last sequence
  the lane fanned out (the dispatcher fills the gap, as it does on a resend), whether the possession
  is already `CLOSED` (then it closes what it holds), and what other replicas told that nobody
  refreshed within `app.field.replicas.remote-ttl` (90 s: a replica that died without saying
  goodbye stops counting). The bus only makes that moment come sooner.
- **Publishing never blocks the business.** The relay publishes after the commit, on the stream
  executor; a broker that is down is logged and counted (`field.replicas.messages{outcome=failed}`)
  and nothing else happens. The broker is not in the health either
  (`management.health.rabbit.enabled=false` by default): without it the service is still correct,
  only slower between replicas. `app.rabbitmq.enabled=false` leaves the `NoOpReplicaBus` and opens
  no connection at all, which is how the tests, the CI smoke test and a single replica run.

## What travels

A `ReplicaEnvelope{replicaId, sentAt, kind, …}` in JSON (the application's `JsonMapper`, ISO
instants), one body field per `kind`; a kind this version does not know is ignored, so two versions
coexist during a deployment. The messages (`application/replicas/ReplicaMessage`):

| `kind` | Body | When | What the receiver does |
|---|---|---|---|
| `COMMAND_COMMITTED` | `{possessionId, sequence}` | After the commit of every command (also the `EventResult`s) | Reads the command from the database and hands it to its dispatcher, which fans it out to the streams of that possession it holds and fills any gap of its lane; without a lane, marks the board dirty |
| `DEVICE_STATE` | `{deviceId, shiftId, possessionId, teamCode, openedAt, lastSeen, kp, batteryPct, signalDbm, streamOpen, lastSentSequence}` | When a device joins, heartbeats, closes its stream, or the dispatcher wrote to it | Keeps it in `RemoteDeviceStates`; the board merges it with the local state (a device with a stream open here is local; otherwise the newest state wins); a newer `openedAt` for a device this replica holds **supersedes** the local stream (`ABORTED`, `SUPERSEDED`), exactly as a second local stream would |
| `POSSESSION_CLOSED` | `{possessionId}` | After the commit of `ClosePossession` | The same as locally: completes the streams, sends the last board, forgets the liveness and the remote states |
| `REPLICA_STOPPED` | – | At `ContextClosedEvent`, synchronously | Forgets everything that replica told; what it tells afterwards about a closed stream is late and dropped (a replica that comes back with the same name starts counting again with its first open stream) |

Only the sequence travels for a command, never its body: every replica reads the same row, so the
bus cannot deliver anything the database does not hold. The message is transient (non-persistent)
and carries the `x-mto-replica` header and the `kind` for whoever inspects the queues.

## Topology

- One **fanout** exchange, durable, `mto.field.replicas.exchange`
  (`app.rabbitmq.replicas.exchange`), redeclared identically by every replica.
- One queue per replica, `mto.field.replicas.<id>` (`app.field.replicas.id`; blank → the host with
  a random suffix), **exclusive and auto-delete**, bound to the exchange: only the connection that
  declared it consumes it and it disappears with that connection, so a dead replica leaves no queue
  filling up with what nobody will read. On reconnection the `RabbitAdmin` declares it again and the
  listener container consumes it again.
- The consumer (`ReplicaMessageConsumer`) never rejects: a body that is not an envelope, an unknown
  kind or a handler that fails are logged and counted, and the message is consumed. There is no
  dead-letter queue because nothing here deserves a second look: the database has it.
- The replica ignores its own messages by `replicaId` (a fanout gives them back to the publisher).

## Why the bus has no signature and no outbox

The siblings sign the messages that cross service boundaries, and so does this service for its own
events (below). The bus is one service talking to itself with the broker's own credentials: a
signature would prove nothing a compromised broker credential does not already grant. And an
outbox would make the bus load-bearing — exactly what the catch-up tick exists to avoid: the truth
is already in `field_command` and `field_event`, and a message that never leaves is recovered from
there. `mto-notification` does not hear the `mto.field.replicas.exchange`: it is not a domain
event, it is plumbing.

## Published events

Everything this service tells the outside world goes through **one door**,
`DomainEventPublisher.publish(DomainEvent)` (`application/service`), called **inside the business
transaction** that makes the change, and reaches RabbitMQ through the same outbox as the siblings
(a copy of `mto-configuration`'s `core/outbox`, the one `mto-maintenance` and `mto-stock` carry):
the event is written to `outbox_message` (`V2`) with the change, and the relay publishes it
afterwards with publisher confirms, so the event exists if and only if the change it tells was
committed, and it survives a broker outage. With `app.rabbitmq.enabled=false` the publisher is the
`NoOpDomainEventPublisher` and nothing is written.

### Topology

| | |
|---|---|
| Exchange | `mto.field.exchange` (`app.rabbitmq.events.exchange`), topic, durable; declared here, apart from the replica fanout |
| Routing key | `mto.field.possession.<event>` (`FieldRabbitMqNames`) |
| Queue | **none here**. `mto-notification` declares `mto.notification.field.queue` bound to `mto.field.#`, with its dead letters |
| AMQP `message_id` | the id of the `outbox_message` row (the consumer's inbox key falls back to it) |
| Headers | `eventType`, `aggregateType`, `aggregateId`, `sequenceNumber` (global, increasing), `messageSignature` and `messageSignatureAlgorithm` (`app.messaging.signature.*`: HMAC-SHA256 with the secret the siblings share, SHA-256 without one), `traceparent`/`tracestate` |

### Envelope

The `AsynchronousMessage` of `mto-configuration`, with `data` a `DomainEvent`
(`entityName`, `entityId`, `eventName`, `values`):

- `eventType` is `FIELD_POSSESSION_<EVENT>`; the consumer derives the activity type from the
  `DomainEvent` (`field.possession.<event>`).
- `messageHash` is SHA-256 over the JSON of the seven original keys only; `actor` and
  `correlationId` are outside it.
- `actor` is who asked for the operation, read from the token of the gRPC call in the thread that
  writes the outbox (`MessageContextResolver`): `PERSON` for the supervisor who opens, closes and
  orders the evacuation, and for the technician whose device acknowledges or clears the track
  (the `preferred_username` and `sub` of the token the stream was opened with); `SYSTEM` for the
  ack watchdog, which runs in a thread of its own.
- `correlationId` is the **code of the possession** (`PO-000012`): gRPC carries no
  `X-Correlation-Id` and a night spans many calls, so the hook fixes it around the publish
  (`MessagingCorrelation`) and everything that happened in that night groups under it in
  `mto-notification`, as the jobs of `mto-configuration` group under their `jobId`.
- **Keys are only added.** Renaming or removing a key, the entity name, an event name or a routing
  key breaks `mto-notification`; a new key changes the example in `docs/messaging/examples/` in
  the same commit. `values` never carries anything that smells like a secret: `DomainEvent` rejects
  such keys at any depth. Null values do travel (`closeReason: null` is information).

### Events

One aggregate, `possession`: every event of a night carries the possession as its entity, so the
relay's strict ordering per aggregate keeps `opened` before `evacuation-issued` and that before
`closed` even if one fails and is retried. Every event carries the possession (`code`, `status`,
`shiftDate`, `endsAt`, `openedAt`, `openedBy`, `shiftCount`, `teamCodes[]` and `shifts[]` with
`shiftId`, `shiftCode`, `teamCode`, `teamName`, `plannedEnd`, in team order) and adds:

| Event | When (inside the transaction) | Adds to `values` |
|---|---|---|
| `opened` | `OpenPossession`, after the row is written | – |
| `closed` | `ClosePossession` | `closedAt`, `closedBy`, `forced`, `closeReason`, `pendingTeams[]` (still on the track), `allClear` |
| `evacuation-issued` | `IssueCommand` with `EvacuateNow` (only that: messages, window changes and `EventResult`s belong to the channel) | `commandId`, `sequence`, `reason`, `issuedAt`, `issuedBy` |
| `evacuation-acknowledged` | the **first** `CommandAck` of a team to an evacuation (resends do not count) | `commandId`, `sequence`, `shiftId`, `shiftCode`, `teamCode`, `deviceId`, `ackedBy`, `accepted`, `reason`, `ackedAt`, `pendingTeams[]` (not yet acknowledged), `allAcknowledged` |
| `evacuation-unacknowledged` | the ack watchdog, once per evacuation whose teams have not all acknowledged after `app.field.evacuation.ack-timeout` (2 min) | `commandId`, `sequence`, `issuedAt`, `issuedBy`, `pendingTeams[]`, `overdueSeconds` |
| `clear-of-track` | the **first** `ClearOfTrack` of a team (resends do not count) | `shiftId`, `shiftCode`, `teamCode`, `deviceId`, `clearedBy`, `earthingRemoved`, `clearedAt`, `pendingTeams[]` (still on the track), `allClear` |

The values are built in one place, `FieldEvents` (`application/service/impl`), field by field: that
construction is the whitelist. Every event has a real JSON example in `docs/messaging/examples/`
that `MessagingContractExamplesTest` builds with the real factory and compares with the file
(`MESSAGING_EXAMPLES_WRITE=true ./mvnw test -Dtest=MessagingContractExamplesTest` regenerates
them); `mto-notification` copies those files as its contract fixtures.

### The ack watchdog

`EvacuationAckWatchdog` runs every `app.field.evacuation.check-every` (30 s) on every replica and
looks at the evacuations of open possessions issued before `now - ack-timeout` that nobody looked
at yet (`field_command.ack_watched_at is null`, `V3`). For each one, in its own transaction, it
marks the row with a conditional `UPDATE` and only the replica that wins the mark decides: if
teams are still silent it publishes `evacuation-unacknowledged` with an `operationId` derived from
the command (`nameUUIDFromBytes("evacuation-unacknowledged:" + commandId)`), so the consumer's
inbox discards any repetition; if everyone acknowledged meanwhile it only marks. Each evacuation is
looked at once; a possession that closes first is never looked at. The decision is the database's,
never read-then-write.

### The outbox

The same pieces as in `mto-maintenance`, each a `@Bean` of `OutboxConfiguration` under
`app.rabbitmq.enabled`: the immediate dispatch after the commit with the 5 s poll as the safety
net, publisher confirms and returns (the relay refuses to start without
`spring.rabbitmq.publisher-confirm-type=correlated`; the replica bus shares the template and
still waits for nothing), exponential backoff over 20 attempts, `FAILED` and its redrive through
`POST /actuator/outbox` (`ops-write`; `GET` with `ops-metrics`), `app.outbox.enabled=false` to stop
only the relay, the metrics (`outbox.messages.*`, `outbox.publish.total`), the 7-day purge and the
trace context that travels with the message. Its tests live under
`infrastructure/messaging/outbox` (`OutboxRelayDataJpaTest` against PostgreSQL,
`OutboxWiringTest`, `OutboxRabbitPublisherTest`…) and `OutboxRabbitIT` publishes against a real
RabbitMQ with confirms (the CI, or `TEST_RABBITMQ_URI`).

## What the siblings get

`mto-notification` hears the events above (its `field` source, category `FIELD`) and turns them
into the activity log and the notifications of its rules: the supervisors of the possession and
the maintenance managers see the possession open and close, the evacuation issued, a team that
refuses or that does not acknowledge in time, and the track clear, in the bell and the log of both
frontends. The shifts, the orders and the task outcomes of the same night are still told by
`mto-maintenance`, which this service never repeats.

## Running two replicas

`mto-platform` brings the second one up with `--profile field-cluster` (`mto-field-2`, host ports
8088 and 9095) sharing the database and the broker; the simulator spreads its devices with
`--target localhost:9094,localhost:9095`. From the IDE, a second instance only needs another
`SERVER_PORT`, `SPRING_GRPC_SERVER_PORT` and `APP_FIELD_REPLICAS_ID`. `ReplicaClusterTest` runs two
contexts in one JVM over an in-memory bus (and over a cut one, to watch the tick);
`RabbitReplicaBusIT` runs the real bus against a RabbitMQ container (or `TEST_RABBITMQ_URI`).

What a watcher sees depends on the replica it talks to only in two things: the board `version` is
per replica (seeded with the clock, increasing on that replica), and with the bus **off** the board
of a replica only knows the presence of its own devices (`sent_to`, `queued_for`, the liveness of a
team with no device here), while the acknowledgements, the clear-of-track and the commands are the
same everywhere because they come from the database.
