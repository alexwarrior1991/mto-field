package com.alejandro.mtofield.infrastructure.messaging.outbox;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Propagacion de traza sobre Micrometer Tracing (bridge de OpenTelemetry).
 * <p>
 * Se usa el {@link Propagator} y no se compone el {@code traceparent} a mano a
 * proposito: el formato lo decide el propagador configurado, de modo que si manana se
 * cambia de W3C a B3 esto sigue funcionando sin tocarlo.
 */
@Slf4j
@RequiredArgsConstructor
public class MicrometerOutboxTracing implements OutboxTracing {

    static final String TRACE_PARENT = "traceparent";
    static final String TRACE_STATE = "tracestate";

    private static final String PUBLISH_SPAN_NAME = "outbox publish";

    private final Tracer tracer;
    private final Propagator propagator;

    @Override
    public OutboxTraceContext capture() {
        Span current = tracer.currentSpan();

        if (current == null) {
            return OutboxTraceContext.EMPTY;
        }

        Map<String, String> carrier = new LinkedHashMap<>();
        propagator.inject(current.context(), carrier, Map::put);

        return new OutboxTraceContext(carrier.get(TRACE_PARENT), carrier.get(TRACE_STATE));
    }

    @Override
    public Scope startPublishScope(OutboxRecord record) {
        if (record.traceParent() == null || record.traceParent().isBlank()) {
            return Scope.NOOP;
        }

        try {
            Map<String, String> carrier = new LinkedHashMap<>();
            carrier.put(TRACE_PARENT, record.traceParent());
            if (record.traceState() != null && !record.traceState().isBlank()) {
                carrier.put(TRACE_STATE, record.traceState());
            }

            Span span = propagator.extract(carrier, Map::get)
                    .name(PUBLISH_SPAN_NAME)
                    .tag("outbox.event.type", String.valueOf(record.eventType()))
                    .tag("messaging.destination.name", String.valueOf(record.exchangeName()))
                    .start();

            Tracer.SpanInScope spanInScope = tracer.withSpan(span);

            return () -> {
                spanInScope.close();
                span.end();
            };
        } catch (Exception exception) {
            // Un contexto corrupto no puede impedir que el mensaje salga.
            log.warn("No se ha podido recuperar el contexto de traza del outbox message {}",
                    record.id(), exception);
            return Scope.NOOP;
        }
    }
}
