package com.alejandro.mtofield.infrastructure.messaging.outbox;

import com.alejandro.mtofield.application.dto.messaging.AsynchronousMessage;

import java.time.Instant;
import java.util.UUID;

/**
 * Construye el sobre de un mensaje: identificador, origen, fecha, huella y el contexto de la
 * operacion que lo genera (quien y bajo que {@code correlationId}).
 *
 * <p>El contexto se lee aqui, en el momento de crear el mensaje, porque es el unico en el que
 * existe: el evento se escribe en el outbox dentro de la transaccion de negocio, y el relay que lo
 * publica despues corre en un hilo del planificador, sin peticion ni usuario.</p>
 */
public class AsynchronousMessageFactory {

    private final AsynchronousMessageHashService hashService;
    private final MessageContextResolver contextResolver;
    private final String applicationName;

    /**
     * @param applicationName el {@code origin} de todo lo que se publica: {@code spring.application.name}
     */
    public AsynchronousMessageFactory(AsynchronousMessageHashService hashService,
                                      MessageContextResolver contextResolver,
                                      String applicationName) {
        this.hashService = hashService;
        this.contextResolver = contextResolver;
        this.applicationName = applicationName;
    }

    public <T> AsynchronousMessage<T> create(String referenceId, String eventType, T data) {
        return create(UUID.randomUUID(), referenceId, eventType, data);
    }

    public <T> AsynchronousMessage<T> create(UUID operationId, String referenceId, String eventType, T data) {
        AsynchronousMessage<T> message = new AsynchronousMessage<>(
                operationId,
                referenceId,
                applicationName,
                Instant.now(),
                eventType,
                data,
                "PENDING",
                contextResolver.currentActor(),
                contextResolver.currentCorrelationId()
        );

        return message.withMessageHash(hashService.calculate(message));
    }
}
