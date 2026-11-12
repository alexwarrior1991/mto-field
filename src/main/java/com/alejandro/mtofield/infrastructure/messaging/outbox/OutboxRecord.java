package com.alejandro.mtofield.infrastructure.messaging.outbox;

import java.time.Instant;
import java.util.UUID;

/**
 * Copia inmutable y desligada de JPA de un mensaje reclamado por el relay.
 * <p>
 * El relay publica FUERA de la transaccion de base de datos, asi que no puede
 * pasearse con entidades gestionadas: se lleva este record y vuelve luego con el
 * id para cerrar el estado en una transaccion corta.
 * <p>
 * {@code attempts} es el numero de ESTE intento: se cuenta al reclamar, no al fallar.
 * {@code claimedUntil} es la visibilidad que este relay escribio en la fila al
 * reclamarla (o al renovarla) y hace de testigo del reclamo: renovar, aplazar, agotar
 * y soltar solo valen mientras la fila siga llevando ese mismo instante. Cada reclamo
 * nuevo escribe uno estrictamente posterior, asi que nunca coinciden dos.
 *
 * @param attempts     intentos consumidos, contando el que esta en curso
 * @param maxAttempts  intentos de los que dispone el mensaje
 * @param claimedUntil hasta cuando es de este relay; null en un record que no viene de un reclamo
 */
public record OutboxRecord(
        UUID id,
        String aggregateType,
        String aggregateId,
        String eventType,
        String exchangeName,
        String routingKey,
        String payload,
        int attempts,
        long sequenceNumber,
        String traceParent,
        String traceState,
        int maxAttempts,
        Instant claimedUntil
) {

    /** El mismo mensaje con su reclamo renovado. */
    OutboxRecord renewedUntil(Instant newClaimedUntil) {
        return new OutboxRecord(id, aggregateType, aggregateId, eventType, exchangeName, routingKey,
                payload, attempts, sequenceNumber, traceParent, traceState, maxAttempts, newClaimedUntil);
    }

    static OutboxRecord from(OutboxMessage message) {
        return new OutboxRecord(
                message.getId(),
                message.getAggregateType(),
                message.getAggregateId(),
                message.getEventType(),
                message.getExchangeName(),
                message.getRoutingKey(),
                message.getPayload(),
                message.getAttempts(),
                message.getSequenceNumber(),
                message.getTraceParent(),
                message.getTraceState(),
                message.getMaxAttempts(),
                message.getNextAttemptAt()
        );
    }
}
