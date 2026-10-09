package com.alejandro.mtofield.application.replicas;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Optional;

/**
 * La forma en que un {@link ReplicaMessage} viaja por el bus: quien lo manda, cuando, de que clase
 * es y su cuerpo en el campo de su clase. Solo se anaden claves y clases: un {@code kind} que esta
 * replica no conoce se ignora ({@link #message()} vacio), nunca se rechaza, para que dos versiones
 * del servicio convivan durante un despliegue.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ReplicaEnvelope(String replicaId, Instant sentAt, String kind, ReplicaMessage.Command command, ReplicaMessage.Device device,
                              ReplicaMessage.Closed closed) {

    public static final String KIND_COMMAND_COMMITTED = "COMMAND_COMMITTED";
    public static final String KIND_DEVICE_STATE = "DEVICE_STATE";
    public static final String KIND_POSSESSION_CLOSED = "POSSESSION_CLOSED";
    public static final String KIND_REPLICA_STOPPED = "REPLICA_STOPPED";

    public static ReplicaEnvelope of(String replicaId, Instant sentAt, ReplicaMessage message) {
        return switch (message) {
            case ReplicaMessage.Command command -> new ReplicaEnvelope(replicaId, sentAt, KIND_COMMAND_COMMITTED, command, null, null);
            case ReplicaMessage.Device device -> new ReplicaEnvelope(replicaId, sentAt, KIND_DEVICE_STATE, null, device, null);
            case ReplicaMessage.Closed closed -> new ReplicaEnvelope(replicaId, sentAt, KIND_POSSESSION_CLOSED, null, null, closed);
            case ReplicaMessage.Stopped ignored -> new ReplicaEnvelope(replicaId, sentAt, KIND_REPLICA_STOPPED, null, null, null);
        };
    }

    /** El mensaje que trae, o vacio si es de una clase que esta version no conoce o le falta el cuerpo. */
    public Optional<ReplicaMessage> message() {
        if (kind == null) {
            return Optional.empty();
        }
        return switch (kind) {
            case KIND_COMMAND_COMMITTED -> Optional.ofNullable(command);
            case KIND_DEVICE_STATE -> Optional.ofNullable(device);
            case KIND_POSSESSION_CLOSED -> Optional.ofNullable(closed);
            case KIND_REPLICA_STOPPED -> Optional.of(new ReplicaMessage.Stopped());
            default -> Optional.empty();
        };
    }
}
