package com.alejandro.mtofield.infrastructure.messaging.replicas;

import com.alejandro.mtofield.application.replicas.ReplicaEnvelope;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * El sobre como JSON, con el {@code JsonMapper} de la aplicacion: fechas ISO y, a proposito, sin
 * fallar por claves desconocidas, para que una replica mas nueva pueda anadir campos sin que las
 * viejas tiren sus mensajes.
 */
public class ReplicaEnvelopeCodec {

    private final JsonMapper mapper;

    public ReplicaEnvelopeCodec(JsonMapper mapper) {
        this.mapper = mapper.rebuild().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    }

    public byte[] encode(ReplicaEnvelope envelope) {
        return mapper.writeValueAsBytes(envelope);
    }

    /** @throws IllegalArgumentException si los bytes no son un sobre */
    public ReplicaEnvelope decode(byte[] body) {
        try {
            ReplicaEnvelope envelope = mapper.readValue(body, ReplicaEnvelope.class);
            if (envelope == null || envelope.replicaId() == null || envelope.replicaId().isBlank()) {
                throw new IllegalArgumentException("a replica envelope needs a replicaId");
            }
            return envelope;
        } catch (RuntimeException notAnEnvelope) {
            throw new IllegalArgumentException("not a replica envelope: " + notAnEnvelope.getMessage(), notAnEnvelope);
        }
    }
}
