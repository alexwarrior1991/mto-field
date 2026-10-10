package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.messaging.DomainEvent;
import com.alejandro.mtofield.application.service.DomainEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Publicador apagado ({@code app.rabbitmq.enabled=false}): registra lo que habria publicado y no
 * escribe nada en el outbox. Es lo que usan los tests y un entorno sin broker, igual que
 * {@link NoOpMaintenanceClient} con {@code mto-maintenance}.
 */
@Component
@ConditionalOnProperty(prefix = "app.rabbitmq", name = "enabled", havingValue = "false", matchIfMissing = false)
class NoOpDomainEventPublisher implements DomainEventPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(NoOpDomainEventPublisher.class);

    @Override
    public void publish(DomainEvent event) {
        publish(UUID.randomUUID(), event);
    }

    @Override
    public void publish(UUID operationId, DomainEvent event) {
        LOGGER.debug("Messaging disabled: event {}.{} of {} {} not published (operationId={})",
                event.entityName(), event.eventName(), event.entityName(), event.entityId(), operationId);
    }

    @Override
    public boolean isEnabled() {
        return false;
    }
}
