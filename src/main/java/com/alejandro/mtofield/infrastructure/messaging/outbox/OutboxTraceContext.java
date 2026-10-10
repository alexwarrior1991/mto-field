package com.alejandro.mtofield.infrastructure.messaging.outbox;

/**
 * Contexto de traza W3C que viaja con el mensaje.
 *
 * @param traceParent cabecera {@code traceparent}: version, trace-id, span-id y flags
 * @param traceState  cabecera {@code tracestate}, opcional
 */
public record OutboxTraceContext(String traceParent, String traceState) {

    public static final OutboxTraceContext EMPTY = new OutboxTraceContext(null, null);

    public boolean isPresent() {
        return traceParent != null && !traceParent.isBlank();
    }
}
