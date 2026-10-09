-- Esquema del canal de campo: la posesion (el bloqueo de via que agrupa varios turnos de
-- mto-maintenance), sus turnos, los comandos descendentes (append-only, con una secuencia sin
-- huecos por posesion), sus acuses y los eventos subidos por los dispositivos (append-only, unicos
-- por dispositivo y secuencia).
--
-- Los ids propios son uuid; los de mto-maintenance (turnos, ordenes, tareas) tambien son uuid y se
-- guardan como columnas sueltas, sin clave foranea: pertenecen a otro servicio.
--
-- No hay Envers a proposito: comandos, acuses y eventos son registros de solo insercion y ya son la
-- historia. Las columnas created_at / updated_at / created_by / updated_by las rellena Spring Data
-- auditing (ver docs/07-auditing.md).

-- ---------------------------------------------------------------------------------------------
-- Tipos
-- ---------------------------------------------------------------------------------------------
CREATE TYPE possession_status AS ENUM ('OPEN', 'CLOSED');
CREATE TYPE field_command_kind AS ENUM ('WINDOW_CHANGED', 'EVACUATE_NOW', 'SUPERVISOR_MESSAGE', 'EVENT_RESULT');
CREATE TYPE field_event_kind AS ENUM ('TASK_STARTED', 'TASK_COMPLETED', 'COMMAND_ACK', 'CLEAR_OF_TRACK');
CREATE TYPE field_event_sync_status AS ENUM ('NOT_REQUIRED', 'PENDING', 'SYNCED', 'FAILED', 'REJECTED');

-- Codigo legible de la posesion (PO-000001). Una secuencia sin reinicio anual: el codigo tiene que
-- seguir siendo unico cuando se cite meses despues.
CREATE SEQUENCE possession_code_seq START WITH 1 INCREMENT BY 1;

-- ---------------------------------------------------------------------------------------------
-- Posesion
-- ---------------------------------------------------------------------------------------------
CREATE TABLE possession (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code varchar(16) NOT NULL,
    status possession_status NOT NULL DEFAULT 'OPEN',
    shift_date date NOT NULL,
    ends_at timestamptz NOT NULL,
    opened_at timestamptz NOT NULL DEFAULT now(),
    opened_by varchar(100) NOT NULL,
    closed_at timestamptz,
    closed_by varchar(100),
    forced boolean NOT NULL DEFAULT false,
    close_reason text,
    -- Contador de la secuencia descendente. NO es una secuencia de PostgreSQL a proposito: el
    -- 'update ... returning' bloquea la fila hasta el commit, asi que los numeros se confirman en
    -- orden y sin huecos (un rollback deshace el incremento). Con una secuencia, dos emisiones
    -- concurrentes podrian hacer visible la 11 antes que la 10, y un dispositivo que reanuda en
    -- la 11 se saltaria la 10 para siempre.
    next_command_seq bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    created_by varchar(100) NOT NULL DEFAULT 'system',
    updated_by varchar(100) NOT NULL DEFAULT 'system',
    CONSTRAINT uq_possession_code UNIQUE (code),
    CONSTRAINT chk_possession_closed_fields CHECK (
        (status = 'OPEN' AND closed_at IS NULL AND closed_by IS NULL)
        OR (status = 'CLOSED' AND closed_at IS NOT NULL AND closed_by IS NOT NULL)
    ),
    -- Un cierre forzado (sin que todos hayan salido de la via) deja constancia del motivo.
    CONSTRAINT chk_possession_forced_has_reason CHECK (NOT forced OR close_reason IS NOT NULL),
    CONSTRAINT chk_possession_next_command_seq_non_negative CHECK (next_command_seq >= 0)
);
CREATE INDEX idx_possession_status_shift_date ON possession (status, shift_date);

-- ---------------------------------------------------------------------------------------------
-- Turnos de la posesion (los MaintenanceShift de mto-maintenance que agrupa)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE possession_shift (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    possession_id uuid NOT NULL,
    shift_id uuid NOT NULL,
    shift_code varchar(32),
    -- Instantanea del equipo del turno: es lo que el tablero y los acuses nombran.
    team_code varchar(16) NOT NULL,
    team_name varchar(120),
    planned_end timestamptz,
    -- Copia desnormalizada del estado de la posesion, solo para el indice parcial de abajo: un turno
    -- no puede estar en DOS posesiones abiertas. La pone a false la transaccion que cierra.
    open boolean NOT NULL DEFAULT true,
    clear_of_track_at timestamptz,
    clear_of_track_by varchar(100),
    clear_of_track_device varchar(100),
    earthing_removed boolean,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    created_by varchar(100) NOT NULL DEFAULT 'system',
    updated_by varchar(100) NOT NULL DEFAULT 'system',
    CONSTRAINT fk_possession_shift_possession FOREIGN KEY (possession_id) REFERENCES possession (id) ON DELETE CASCADE,
    CONSTRAINT uq_possession_shift_possession_shift UNIQUE (possession_id, shift_id),
    CONSTRAINT chk_possession_shift_clear_fields CHECK (
        clear_of_track_at IS NULL OR (clear_of_track_by IS NOT NULL AND clear_of_track_device IS NOT NULL)
    )
);
CREATE UNIQUE INDEX uq_possession_shift_open ON possession_shift (shift_id) WHERE open;
CREATE INDEX idx_possession_shift_shift ON possession_shift (shift_id);

-- ---------------------------------------------------------------------------------------------
-- Comandos descendentes (servidor -> dispositivos). Append-only.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE field_command (
    -- Es el FieldCommand.command_id que viaja por el stream.
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    possession_id uuid NOT NULL,
    sequence bigint NOT NULL,
    kind field_command_kind NOT NULL,
    -- Turno destinatario; NULL = difusion a todos los turnos de la posesion.
    target_shift_id uuid,
    -- Solo los de IssueCommand: reintentar el unario no puede duplicar un desalojo.
    idempotency_key varchar(100),
    requires_ack boolean NOT NULL DEFAULT false,
    issued_at timestamptz NOT NULL DEFAULT now(),
    issued_by varchar(100) NOT NULL,
    -- El FieldCommand entero tal como se envia (JsonFormat de protobuf). La reanudacion lo reenvia
    -- tal cual; las columnas tipadas de arriba solo sirven para consultar.
    payload json NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    created_by varchar(100) NOT NULL DEFAULT 'system',
    updated_by varchar(100) NOT NULL DEFAULT 'system',
    CONSTRAINT fk_field_command_possession FOREIGN KEY (possession_id) REFERENCES possession (id) ON DELETE CASCADE,
    CONSTRAINT uq_field_command_possession_sequence UNIQUE (possession_id, sequence),
    -- Los NULL no chocan entre si: los EventResult, sin clave, conviven.
    CONSTRAINT uq_field_command_idempotency_key UNIQUE (possession_id, idempotency_key),
    CONSTRAINT chk_field_command_sequence_positive CHECK (sequence > 0)
);
CREATE INDEX idx_field_command_target ON field_command (possession_id, target_shift_id, sequence);
CREATE INDEX idx_field_command_requires_ack ON field_command (possession_id, sequence) WHERE requires_ack;

-- ---------------------------------------------------------------------------------------------
-- Acuses (un acuse por comando y EQUIPO; cualquier dispositivo del equipo cuenta)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE command_ack (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    command_id uuid NOT NULL,
    shift_id uuid NOT NULL,
    device_id varchar(100) NOT NULL,
    -- El usuario del token que acuso, y desde que dispositivo.
    acked_by varchar(100) NOT NULL,
    accepted boolean NOT NULL,
    reason text,
    acked_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    created_by varchar(100) NOT NULL DEFAULT 'system',
    updated_by varchar(100) NOT NULL DEFAULT 'system',
    CONSTRAINT fk_command_ack_command FOREIGN KEY (command_id) REFERENCES field_command (id) ON DELETE CASCADE,
    CONSTRAINT uq_command_ack_command_shift UNIQUE (command_id, shift_id)
);

-- ---------------------------------------------------------------------------------------------
-- Eventos subidos (dispositivo -> servidor). Append-only; unicos por dispositivo y secuencia.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE field_event (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id varchar(100) NOT NULL,
    -- TeamMessage.sequence (> 0): el latido viaja con 0 y no se guarda.
    sequence bigint NOT NULL,
    possession_id uuid NOT NULL,
    shift_id uuid NOT NULL,
    kind field_event_kind NOT NULL,
    occurred_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    reported_by varchar(100) NOT NULL,
    -- El TeamMessage entero tal como llego (JsonFormat de protobuf).
    payload json NOT NULL,
    -- NOT_REQUIRED: se aplica aqui (acuse, salida de via). PENDING / SYNCED / FAILED / REJECTED:
    -- el ciclo de una tarea que hay que contar a mto-maintenance.
    sync_status field_event_sync_status NOT NULL,
    sync_attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    synced_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    created_by varchar(100) NOT NULL DEFAULT 'system',
    updated_by varchar(100) NOT NULL DEFAULT 'system',
    CONSTRAINT fk_field_event_possession FOREIGN KEY (possession_id) REFERENCES possession (id) ON DELETE CASCADE,
    CONSTRAINT uq_field_event_device_sequence UNIQUE (device_id, sequence),
    CONSTRAINT chk_field_event_sequence_positive CHECK (sequence > 0),
    CONSTRAINT chk_field_event_sync_attempts_non_negative CHECK (sync_attempts >= 0),
    CONSTRAINT chk_field_event_synced_at CHECK (sync_status <> 'SYNCED' OR synced_at IS NOT NULL)
);
CREATE INDEX idx_field_event_retry ON field_event (next_attempt_at) WHERE sync_status IN ('PENDING', 'FAILED');
CREATE INDEX idx_field_event_possession_received ON field_event (possession_id, received_at);
