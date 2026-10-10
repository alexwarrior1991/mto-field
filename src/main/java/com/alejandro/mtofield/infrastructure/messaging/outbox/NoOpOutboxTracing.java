package com.alejandro.mtofield.infrastructure.messaging.outbox;

/**
 * Implementacion para cuando no hay trazabilidad configurada
 * ({@code management.tracing.enabled=false} o sin bridge en el classpath).
 * <p>
 * El outbox no puede depender de que haya un Tracer: publicar eventos es su trabajo,
 * trazarlos es un extra.
 */
public class NoOpOutboxTracing implements OutboxTracing {

    @Override
    public OutboxTraceContext capture() {
        return OutboxTraceContext.EMPTY;
    }

    @Override
    public Scope startPublishScope(OutboxRecord record) {
        return Scope.NOOP;
    }
}
