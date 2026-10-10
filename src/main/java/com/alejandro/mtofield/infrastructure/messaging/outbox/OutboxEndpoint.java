package com.alejandro.mtofield.infrastructure.messaging.outbox;

import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;

import java.util.Map;

/**
 * Endpoint de explotacion del outbox: {@code GET /actuator/outbox} para ver el estado
 * y {@code POST /actuator/outbox} para reencolar los mensajes FAILED.
 * <p>
 * Va en actuator y no en un controlador REST porque es una operacion de explotacion,
 * no de negocio. Requiere exponerlo (management.endpoints.web.exposure.include) y esta
 * restringido en SecurityConfiguration: la lectura exige el rol de metricas de
 * explotacion y el redrive, que modifica el estado, uno propio de escritura.
 */
@Endpoint(id = "outbox")
@RequiredArgsConstructor
public class OutboxEndpoint {

    private static final int DEFAULT_REDRIVE_LIMIT = 100;

    private final OutboxAdminService outboxAdminService;

    @ReadOperation
    public OutboxStats stats() {
        return outboxAdminService.stats();
    }

    @WriteOperation
    public Map<String, Object> redrive(@Nullable Integer limit) {
        int effectiveLimit = limit != null ? limit : DEFAULT_REDRIVE_LIMIT;
        return Map.of("redriven", outboxAdminService.redriveFailed(effectiveLimit));
    }
}
