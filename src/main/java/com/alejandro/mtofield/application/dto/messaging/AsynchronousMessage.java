package com.alejandro.mtofield.application.dto.messaging;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.UUID;

/**
 * El sobre de todo lo que este servicio publica: el mismo {@code AsynchronousMessage} de
 * {@code mto-configuration}, para que {@code mto-notification} lea todas las fuentes igual.
 *
 * <p>Las siete primeras claves son el contrato original, y {@code messageHash} se calcula solo sobre
 * ellas. {@code actor} y {@code correlationId} son las dos claves que los productores nuevos anaden
 * para que el consumidor sepa quien hizo el cambio y bajo que peticion o mensaje. En el contrato solo
 * se anaden claves: renombrar o quitar una rompe al consumidor.</p>
 *
 * @param actor         quien pidio la operacion, clasificado por este servicio; nunca nulo en lo que
 *                      se publica, pero opcional en el contrato
 * @param correlationId el {@code X-Correlation-Id} de la peticion, o el identificador del mensaje de
 *                      datos maestros que provoco la escritura; nulo fuera de ambos
 */
public record AsynchronousMessage<T>(
        @NotNull UUID operationId,
        @NotBlank String referenceId,
        @NotBlank String origin,
        @NotNull Instant creationDate,
        @NotBlank String eventType,
        @NotNull T data,
        @NotBlank String messageHash,
        MessageActor actor,
        String correlationId
) {

    /** El mismo mensaje con su huella ya calculada. */
    public AsynchronousMessage<T> withMessageHash(String hash) {
        return new AsynchronousMessage<>(
                operationId,
                referenceId,
                origin,
                creationDate,
                eventType,
                data,
                hash,
                actor,
                correlationId
        );
    }
}
