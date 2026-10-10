-- Outbox de los eventos propios de este servicio (docs/06-messaging.md, fase 5).
--
-- Un evento (una posesion abierta o cerrada, un desalojo emitido, un equipo que acusa o que no acusa
-- a tiempo, la via libre) se escribe en esta tabla DENTRO de la transaccion de negocio que lo
-- provoca, y un relay lo publica despues en RabbitMQ esperando la confirmacion del broker. Asi el
-- evento existe si y solo si el cambio que cuenta se confirmo, y sobrevive a un broker caido.
--
-- Es la tabla outbox_message de mto-configuration en su estado final, la misma que llevan
-- mto-maintenance (su V13) y mto-stock, para que el codigo del outbox sea el mismo en todos: payload
-- como text y no como large object (que no se borra con la fila), CHECK del estado con IN_PROGRESS,
-- indices parciales, contexto de traza y numero de secuencia.
--
-- La escribe solo el outbox y su historia es ella misma: sin columnas de auditoria, como el resto
-- de lo que aqui se escribe con SQL nativo (docs/07-auditing.md).

-- Numero de secuencia monotono por mensaje, asignado por la base de datos y no por la aplicacion:
-- con varias replicas escribiendo a la vez es el unico sitio donde el contador es de verdad unico y
-- creciente. Ordena el reclamo del relay y retiene un mensaje mientras otro anterior del mismo
-- agregado siga sin publicarse (created antes que status-changed de la misma orden).
CREATE SEQUENCE outbox_message_sequence;

CREATE TABLE outbox_message (
    id              uuid         NOT NULL,
    aggregate_type  varchar(100) NOT NULL,
    aggregate_id    varchar(150) NOT NULL,
    event_type      varchar(150) NOT NULL,
    exchange_name   varchar(200) NOT NULL,
    routing_key     varchar(200) NOT NULL,
    -- El JSON que viaja tal cual: el sobre AsynchronousMessage con el DomainEvent dentro.
    payload         text         NOT NULL,
    status          varchar(30)  NOT NULL,
    attempts        integer      NOT NULL,
    max_attempts    integer      NOT NULL,
    sequence_number bigint       NOT NULL DEFAULT nextval('outbox_message_sequence'),
    created_at      timestamptz  NOT NULL,
    next_attempt_at timestamptz,
    published_at    timestamptz,
    last_error      varchar(1000),
    -- Contexto de traza W3C de la operacion que escribio el evento. El relay publica minutos despues
    -- desde el hilo del planificador: sin esto el span de la publicacion colgaria del planificador y
    -- no de la peticion que la origino. Nulos en los eventos generados fuera de una traza (una tarea
    -- programada, por ejemplo).
    trace_parent    varchar(64),
    trace_state     varchar(512),
    CONSTRAINT pk_outbox_message PRIMARY KEY (id),
    -- ddl-auto: validate NO comprueba las restricciones CHECK: si faltara IN_PROGRESS aqui, el fallo
    -- saldria en la primera pasada del relay y no al arrancar.
    CONSTRAINT outbox_message_status_check CHECK (status IN ('PENDING', 'IN_PROGRESS', 'PUBLISHED', 'FAILED'))
);

-- Los indices son PARCIALES a proposito: la tabla solo crece, pero las filas que consultan el relay
-- y la purga son una fraccion minuscula del total, y un indice parcial se mantiene diminuto aunque
-- se acumulen millones de mensajes publicados.

-- Reclamo del relay: sirve el ORDER BY y corta la exploracion en el tamano del lote.
CREATE INDEX idx_outbox_message_claim
    ON outbox_message (sequence_number)
    WHERE status IN ('PENDING', 'IN_PROGRESS');

-- Retencion por agregado: resuelve "existe algo anterior de este mismo agregado todavia sin
-- publicar" sin recorrer la tabla.
CREATE INDEX idx_outbox_message_aggregate
    ON outbox_message (aggregate_type, aggregate_id, sequence_number)
    WHERE status IN ('PENDING', 'IN_PROGRESS');

-- Purga de los mensajes ya publicados, por antiguedad de published_at.
CREATE INDEX idx_outbox_message_purge
    ON outbox_message (published_at)
    WHERE status = 'PUBLISHED';

-- Mensajes fallidos: los consulta el redrive y, sobre todo, la metrica que los cuenta cada pocos
-- segundos, que sin indice seria un seq scan periodico por un contador que casi siempre vale cero.
CREATE INDEX idx_outbox_message_failed
    ON outbox_message (created_at)
    WHERE status = 'FAILED';
