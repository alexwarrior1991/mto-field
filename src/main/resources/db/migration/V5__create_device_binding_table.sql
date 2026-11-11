-- El dispositivo y la persona (docs/03-database.md): un device_id es de quien se unio primero con el.
--
-- Hasta aqui el device_id de un Join lo decia el cliente y nada lo ligaba al token: cualquier tecnico
-- admitido en un turno podia unirse con el dispositivo de otro, sustituir su stream (ABORTED
-- superseded) y subir eventos bajo su secuencia. La liga la decide la base en UNA sentencia
-- (DeviceBindingRepository.bind: insert ... on conflict do update ... where), nunca leer-y-escribir,
-- asi que dos replicas no ligan el mismo dispositivo a dos personas.
--
-- Una fila por dispositivo y no por (dispositivo, posesion), porque la secuencia de subida y la marca
-- de agua son del dispositivo a secas (uq_field_event_device_sequence) y el registro de streams
-- tambien: una liga por posesion dejaria suplantarlo desde otra posesion abierta a la vez. La fila se
-- suelta sola cuando se cierra la posesion en la que se ligo: la tableta pasa a otra persona otra
-- noche sin que nadie toque nada. La misma persona la lleva de una posesion a otra cuando quiera.
--
-- Sin columnas de auditoria, como outbox_message: solo la escribe esa sentencia, y lo que dirian
-- (quien y cuando) ya son username y bound_at.
CREATE TABLE device_binding (
    device_id varchar(100) PRIMARY KEY,
    -- El preferred_username del token del primer Join: el mismo con el que se guarda lo que sube.
    username varchar(100) NOT NULL,
    -- La posesion en la que se ligo (o a la que su persona lo llevo despues). Mientras este abierta,
    -- el dispositivo es de esa persona.
    possession_id uuid NOT NULL,
    bound_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_device_binding_possession FOREIGN KEY (possession_id) REFERENCES possession (id) ON DELETE CASCADE
);
CREATE INDEX idx_device_binding_possession ON device_binding (possession_id);
