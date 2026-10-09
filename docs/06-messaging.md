# 06 · Messaging

`mto-field` has one use of the broker, since Phase 4: the **bus between its own replicas**, a
RabbitMQ fanout through which the replicas tell each other what is not in the database. It has no
queue of anyone else's, no exchange of its own for `mto-notification`, no outbox and no inbox.

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

## Why no signature and no outbox

The siblings sign the master-data messages because they cross service boundaries. This bus is one
service talking to itself with the broker's own credentials: a signature would prove nothing a
compromised broker credential does not already grant. And an outbox would make the bus
load-bearing — exactly what the catch-up tick exists to avoid: the truth is already in
`field_command` and `field_event`, and a message that never leaves is recovered from there.

## What the siblings get

Nothing from here. The consumers of the domain's events (`mto-notification`) hear about a night's
work from `mto-maintenance`, which publishes the shift, the order and the task outcomes; nothing of
that is published twice. A new event of this service, if one is ever needed, would follow the
siblings' shape (`DomainEvent` in the `AsynchronousMessage` envelope, through an outbox, on an
exchange of its own) and would be decided then, not added ahead of a consumer. `mto-notification`
hears the `mto.field.replicas.exchange` neither: it is not a domain event, it is plumbing.

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
