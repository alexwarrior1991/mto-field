package com.alejandro.mtofield.infrastructure.messaging.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.UUID;

@RequiredArgsConstructor
public class OutboxService {

    private final OutboxMessageRepository outboxMessageRepository;
    private final OutboxProperties outboxProperties;
    private final ObjectMapper objectMapper;
    private final OutboxTracing outboxTracing;
    private final ApplicationEventPublisher applicationEventPublisher;


    public void save(
            String aggregateType,
            String aggregateId,
            String eventType,
            String exchangeName,
            String routingKey,
            Object payload
    ) {

        try {

            Instant now = Instant.now();

            OutboxMessage message = new OutboxMessage();
            message.setId(UUID.randomUUID());
            message.setAggregateType(aggregateType);
            message.setAggregateId(aggregateId);
            message.setEventType(eventType);
            message.setExchangeName(exchangeName);
            message.setRoutingKey(routingKey);
            message.setPayload(objectMapper.writeValueAsString(payload));
            message.setStatus(OutboxStatus.PENDING);
            message.setAttempts(0);
            message.setMaxAttempts(outboxProperties.getMaxAttempts());
            message.setCreatedAt(now);
            message.setNextAttemptAt(now);

            // Se captura AQUI, dentro de la transaccion de negocio, porque es el unico
            // momento en el que el contexto de la operacion original sigue vivo.
            OutboxTraceContext traceContext = outboxTracing.capture();
            message.setTraceParent(traceContext.traceParent());
            message.setTraceState(traceContext.traceState());

            outboxMessageRepository.save(message);

            // Despierta al relay al confirmar la transaccion, para no pagar el sondeo.
            applicationEventPublisher.publishEvent(new OutboxMessageSavedEvent());
        } catch (Exception exception) {
            throw new IllegalStateException("Error creating outbox message", exception);
        }

    }

}
