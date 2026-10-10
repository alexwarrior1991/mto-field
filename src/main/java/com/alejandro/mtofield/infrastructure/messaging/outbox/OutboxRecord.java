package com.alejandro.mtofield.infrastructure.messaging.outbox;

import java.util.UUID;

/**
 * Copia inmutable y desligada de JPA de un mensaje reclamado por el relay.
 * <p>
 * El relay publica FUERA de la transaccion de base de datos, asi que no puede
 * pasearse con entidades gestionadas: se lleva este record y vuelve luego con el
 * id para cerrar el estado en una transaccion corta.
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
        String traceState
) {

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
                message.getTraceState()
        );
    }
}
