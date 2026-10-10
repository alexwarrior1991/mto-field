# 07 · Auditing

Two questions, and on purpose only two layers.

## Who touched a row last

`created_at`/`updated_at`/`created_by`/`updated_by` on every table, defaulting to `now()` and
`'system'` in `V1`. Spring Data JPA auditing fills them from the entities, resolving the
actor like `mto-maintenance` does (`AuditActorResolver`): the `preferred_username` of the JWT
inside a call, `system` outside one. The threads that write from the per-device work queues run
without a security context on purpose, so `created_by` of what they write reads `system`; the
person is in the row itself: `field_event.reported_by` and `command_ack.acked_by` carry the
username of the token of the stream the message arrived on, `possession.opened_by`/`closed_by` and
`field_command.issued_by` the supervisor's.

## What happened, in order

`field_command`, `command_ack` and `field_event` are **append-only**: a row is inserted once and
never updated, except the synchronization columns of an event (`sync_status`, `sync_attempts`,
`next_attempt_at`, `synced_at`, `last_error`), which describe the conversation with
`mto-maintenance` and not the event, and `field_command.ack_watched_at` (`V3`), which says when the
ack watchdog looked at an evacuation and not what the evacuation was. The history of a possession is therefore the tables
themselves, read in `sequence` order: what was sent to whom and when (`issued_at`, `issued_by`,
`target_shift_id`), who acknowledged (`acked_by`, `device_id`, `acked_at`), what each device
reported (`occurred_at` by its clock, `received_at` by the server's), and the payloads as they
travelled (`json`, `JsonFormat`).

## Why there is no Envers

A `_aud` twin of an append-only table is a copy of it, with the same rows and no new information.
`possession` and `possession_shift` do change (the close, the clear-of-track, the counter), but
what changed them is already recorded next to the change: the closing instant and author, the
`forced` flag with its reason, the clear-of-track with its author and device, and every command
that moved the counter is a row of `field_command`. A revision table would answer "what were the
previous values" with values that are never overwritten. So there is no Envers, no revision
entity and no `_aud` migration, and `ddl-auto: validate` has nothing to validate on that side.

`outbox_message` (`V2`) is the only table without the four audit columns: only the outbox writes
it, with native SQL the auditing listener never sees, and its history is itself (`status`,
`attempts`, `published_at`, `last_error`); a published row is purged after seven days.

What is deliberately not kept: the liveness (heartbeats are not stored; `sequence 0`) and the
board versions, both in-memory and recomputable. A night's trace is in the commands, the
acknowledgements and the events, not in who was `STALE` at 03:12.

## Two clocks

An event carries two instants on purpose: `occurred_at` is the device's clock, as the device sent
it, and `received_at` the server's. A device without coverage reports late, and its clock may be
off; `Welcome.server_time` lets it compute its offset, but the stored `occurred_at` is never
corrected here, so what the device said and when the server heard it stay distinguishable.
