package com.alejandro.mtofield.infrastructure.messaging.outbox;

import com.alejandro.mtofield.application.dto.messaging.AsynchronousMessage;
import lombok.RequiredArgsConstructor;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;

/**
 * Huella del contenido del mensaje en el momento de crearlo, calculada como en
 * {@code mto-configuration}: SHA-256 del JSON de las siete claves originales del sobre.
 *
 * <p>Sirve para correlacionar y para detectar dos eventos de contenido identico. NO es una firma y
 * NO sirve para verificar integridad: es un SHA-256 sin secreto, y se calcula sobre el objeto antes
 * de serializarlo, asi que un consumidor tendria que deserializar y volver a serializar para
 * comprobarla, y esa ida y vuelta no es la identidad. Para verificar de verdad, el mensaje viaja
 * firmado en cabecera sobre los bytes reales ({@code MessagePayloadSignature}).</p>
 *
 * <p>{@code actor} y {@code correlationId} quedan fuera a proposito: un consumidor que ya calculaba
 * la huella sobre las siete claves sigue obteniendo la misma.</p>
 */
@RequiredArgsConstructor
public class AsynchronousMessageHashService {

    private final ObjectMapper objectMapper;

    public <T> String calculate(AsynchronousMessage<T> message) {
        return calculate(
                message.operationId(),
                message.referenceId(),
                message.origin(),
                message.creationDate(),
                message.eventType(),
                message.data()
        );
    }

    private <T> String calculate(
            UUID operationId,
            String referenceId,
            String origin,
            Instant creationDate,
            String eventType,
            T data
    ) {
        try {
            String rawValue = objectMapper.writeValueAsString(new HashSource<>(
                    operationId,
                    referenceId,
                    origin,
                    creationDate,
                    eventType,
                    data
            ));

            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(rawValue.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (Exception exception) {
            throw new IllegalStateException("Error calculating asynchronous message hash", exception);
        }
    }

    private record HashSource<T>(
            UUID operationId,
            String referenceId,
            String origin,
            Instant creationDate,
            String eventType,
            T data
    ) {
    }
}
