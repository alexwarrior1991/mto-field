-- El vigilante de acuses (fase 5): un desalojo con equipos sin acusar pasado el plazo se cuenta
-- hacia fuera (possession.evacuation-unacknowledged) UNA sola vez aunque haya varias replicas.
-- Lo decide la base, no el codigo: la marca se pone con un UPDATE condicional (ack_watched_at IS
-- NULL) y solo quien lo gana publica; nunca leer-y-escribir. La marca tambien se pone cuando, al
-- mirar, ya habian acusado todos: cada desalojo se mira una vez.
ALTER TABLE field_command ADD COLUMN ack_watched_at timestamptz;

-- Lo que el vigilante tiene pendiente de mirar: los que piden acuse y aun no se han mirado.
CREATE INDEX idx_field_command_ack_watch ON field_command (issued_at) WHERE requires_ack AND ack_watched_at IS NULL;
