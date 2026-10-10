package com.alejandro.mtofield.infrastructure.messaging.outbox;

import com.alejandro.mtofield.application.dto.messaging.AsynchronousMessage;
import com.alejandro.mtofield.application.dto.messaging.DomainEvent;
import com.alejandro.mtofield.application.service.DomainEventPublisher;
import com.alejandro.mtofield.configuration.rabbitmq.FieldEventsProperties;
import com.alejandro.mtofield.infrastructure.messaging.rabbitmq.FieldRabbitMqNames;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

/**
 * Escribe cada evento propio en el outbox, envuelto en el sobre comun, para que el relay lo publique
 * en {@code mto.field.exchange} con la clave {@code mto.field.<entidad>.<evento>}.
 *
 * <p>El agregado del outbox es la entidad del evento ({@code possession}-{@code <id>}), de modo que
 * el orden estricto por agregado del relay conserva {@code opened} antes que
 * {@code evacuation-issued} y este antes que {@code closed} de la misma posesion aunque uno falle y
 * se reintente.</p>
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxDomainEventPublisher implements DomainEventPublisher {

    private final AsynchronousMessageFactory messageFactory;
    private final OutboxService outboxService;
    private final FieldEventsProperties properties;

    @Override
    public void publish(DomainEvent event) {
        publish(UUID.randomUUID(), event);
    }

    @Override
    public void publish(UUID operationId, DomainEvent event) {
        String eventType = FieldRabbitMqNames.eventType(event.entityName(), event.eventName());

        AsynchronousMessage<DomainEvent> message = messageFactory.create(
                operationId,
                event.entityName() + "-" + event.entityId(),
                eventType,
                event
        );

        outboxService.save(
                event.entityName(),
                event.entityId(),
                eventType,
                properties.exchange(),
                FieldRabbitMqNames.routingKey(event.entityName(), event.eventName()),
                message
        );

        log.debug("Domain event in the outbox: {} {} {} (operationId={}, actor={})",
                event.entityName(), event.entityId(), event.eventName(), operationId, message.actor());
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}
