# 03 · Database

PostgreSQL, Flyway migrations in `src/main/resources/db/migration`, Hibernate `ddl-auto: validate`.
All tables have `id uuid` (`gen_random_uuid()`), `created_at`/`updated_at timestamptz` and
`created_by`/`updated_by varchar(100)` (default `'system'`). Ids of `mto-maintenance` (`shift_id`,
`order_id`, `task_id`) are `uuid` columns **without a foreign key**: they belong to another
service. The SQL is valid on PostgreSQL 16 and 17 (local and CI). There are no `_aud` tables:
no Envers (`07-auditing.md`).

## Enums (PostgreSQL types)

| Type | Values |
|---|---|
| `possession_status` | `OPEN`, `CLOSED` |
| `field_command_kind` | `WINDOW_CHANGED`, `EVACUATE_NOW`, `SUPERVISOR_MESSAGE`, `EVENT_RESULT` |
| `field_event_kind` | `TASK_STARTED`, `TASK_COMPLETED`, `COMMAND_ACK`, `CLEAR_OF_TRACK` |
| `field_event_sync_status` | `NOT_REQUIRED`, `PENDING`, `SYNCED`, `FAILED`, `REJECTED` |

Values match the Java enums one to one; adding a value is a migration
(`ALTER TYPE … ADD VALUE`).

## Sequences

`possession_code_seq` (`PO-000001`): six digits, no yearly reset, because a code is cited months
later and must stay unique.

## Tables (`V1`)

### `possession`

| Column | Type | Notes |
|---|---|---|
| `code` | `varchar(16)` | `uq_possession_code` |
| `status` | `possession_status` | default `OPEN` |
| `shift_date` | `date` | |
| `ends_at` | `timestamptz` | the possession window |
| `opened_at`, `opened_by` | `timestamptz` (default `now()`), `varchar(100)` | |
| `closed_at`, `closed_by` | nullable | |
| `forced` | `boolean` | default `false` |
| `close_reason` | `text` | nullable |
| `next_command_seq` | `bigint` | default `0`; the counter of the downstream sequence |

Constraints: `chk_possession_closed_fields` (`OPEN` ⇔ `closed_at`/`closed_by` null, `CLOSED` ⇔ both
set), `chk_possession_forced_has_reason` (`forced` ⇒ `close_reason`),
`chk_possession_next_command_seq_non_negative`. Index `(status, shift_date)`.

`next_command_seq` is a column and **not a PostgreSQL sequence, on purpose**: `update … returning`
holds the row lock until commit, so numbers are confirmed in order and without holes (a rollback
undoes the increment). With a sequence, two concurrent issues could make `11` visible before `10`,
and a device resuming at `11` would skip `10` for ever.

### `possession_shift`

| Column | Type | Notes |
|---|---|---|
| `possession_id` | `uuid` | FK `possession` `on delete cascade` |
| `shift_id` | `uuid` | the `MaintenanceShift` |
| `shift_code` | `varchar(32)` | nullable snapshot |
| `team_code` | `varchar(16)` | **not null**: what the board and the acknowledgements name |
| `team_name` | `varchar(120)` | nullable |
| `planned_end` | `timestamptz` | nullable |
| `open` | `boolean` | default `true`; copy of the possession's state for the partial index, set to `false` by the closing transaction |
| `clear_of_track_at`, `clear_of_track_by`, `clear_of_track_device` | nullable | |
| `earthing_removed` | `boolean` | nullable |

Constraints: `uq_possession_shift_possession_shift (possession_id, shift_id)`;
`chk_possession_shift_clear_fields` (`clear_of_track_at` ⇒ `_by` and `_device`); the partial unique
index **`uq_possession_shift_open ON possession_shift (shift_id) WHERE open`**, which keeps a shift
out of two open possessions. Index `(shift_id)`.

### `field_command`

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid` | it **is** the `FieldCommand.command_id` on the stream |
| `possession_id` | `uuid` | FK `on delete cascade` |
| `sequence` | `bigint` | `> 0` |
| `kind` | `field_command_kind` | |
| `target_shift_id` | `uuid` | nullable: `NULL` = broadcast |
| `idempotency_key` | `varchar(100)` | nullable: only the supervisor's commands |
| `requires_ack` | `boolean` | default `false` |
| `issued_at`, `issued_by` | `timestamptz` (default `now()`), `varchar(100)` | |
| `payload` | `json` | the whole `FieldCommand` as sent (`JsonFormat`); the typed columns are for querying |

Constraints: `uq_field_command_possession_sequence (possession_id, sequence)`,
`uq_field_command_idempotency_key (possession_id, idempotency_key)` (nulls do not collide: the
`EventResult`s, without a key, coexist), `chk_field_command_sequence_positive`. Indexes
`(possession_id, target_shift_id, sequence)` (the resumption query) and the partial
`(possession_id, sequence) WHERE requires_ack` (the board).

### `command_ack`

| Column | Type | Notes |
|---|---|---|
| `command_id` | `uuid` | FK `field_command` `on delete cascade` |
| `shift_id` | `uuid` | the team |
| `device_id` | `varchar(100)` | |
| `acked_by` | `varchar(100)` | the username of the token |
| `accepted` | `boolean` | |
| `reason` | `text` | nullable |
| `acked_at` | `timestamptz` | default `now()` |

Constraint: `uq_command_ack_command_shift (command_id, shift_id)`: one acknowledgement per command
and team, any device of the team counts.

### `field_event`

| Column | Type | Notes |
|---|---|---|
| `device_id` | `varchar(100)` | |
| `sequence` | `bigint` | `TeamMessage.sequence`, `> 0`; the heartbeat travels with `0` and is not stored |
| `possession_id` | `uuid` | FK `on delete cascade` |
| `shift_id` | `uuid` | |
| `kind` | `field_event_kind` | |
| `occurred_at` | `timestamptz` | the device's clock |
| `received_at` | `timestamptz` | default `now()` |
| `reported_by` | `varchar(100)` | the username of the token |
| `payload` | `json` | the whole `TeamMessage` as received |
| `sync_status` | `field_event_sync_status` | |
| `sync_attempts` | `integer` | default `0`, `>= 0` |
| `next_attempt_at`, `synced_at` | `timestamptz` | nullable |
| `last_error` | `text` | nullable |

Constraints: **`uq_field_event_device_sequence (device_id, sequence)`** (the upstream idempotency),
`chk_field_event_sequence_positive`, `chk_field_event_sync_attempts_non_negative`,
`chk_field_event_synced_at` (`SYNCED` ⇒ `synced_at`). Indexes: the partial
`(next_attempt_at) WHERE sync_status IN ('PENDING', 'FAILED')` (the retry) and
`(possession_id, received_at)`.

## Native SQL the services rely on

- The downstream counter: `update possession set next_command_seq = next_command_seq + 1 where id = :pid and status = 'OPEN' returning next_command_seq` (0 rows: the possession is not open).
- The upstream idempotency: `insert into field_event … on conflict (device_id, sequence) do nothing` and `insert into command_ack … on conflict (command_id, shift_id) do nothing`, with the row count as the answer.
- The contiguous watermark of a device: the greatest `sequence` whose row number equals it
  (`{1,2,4,5}` → `2`, `{2,3}` → `0`).
- The clear-of-track: `update possession_shift set clear_of_track_at = now(), … where … and clear_of_track_at is null`.
- The resumption replay: the commands of the possession after a sequence that are broadcast or
  target the device's shift, in order, paged.
