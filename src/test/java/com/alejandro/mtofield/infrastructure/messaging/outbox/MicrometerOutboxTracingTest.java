package com.alejandro.mtofield.infrastructure.messaging.outbox;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Propagacion de la traza a traves del outbox, con el SDK real de OpenTelemetry y el
 * propagador W3C: lo que se comprueba es el {@code traceparent} de verdad, no una
 * cadena inventada por un mock.
 */
class MicrometerOutboxTracingTest {

    private Tracer tracer;
    private MicrometerOutboxTracing tracing;

    @BeforeEach
    void setUp() {
        // Sin exportador: aqui no se comprueba que los spans salgan a ningun sitio,
        // sino que el contexto sobrevive al salto del outbox.
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder().build();

        OtelCurrentTraceContext currentTraceContext = new OtelCurrentTraceContext();

        tracer = new OtelTracer(
                tracerProvider.get("test"),
                currentTraceContext,
                event -> { },
                new io.micrometer.tracing.otel.bridge.OtelBaggageManager(
                        currentTraceContext, java.util.List.of(), java.util.List.of()));

        tracing = new MicrometerOutboxTracing(
                tracer,
                new OtelPropagator(
                        ContextPropagators.create(W3CTraceContextPropagator.getInstance()),
                        tracerProvider.get("test")));
    }

    private OutboxRecord record(String traceParent, String traceState) {
        return new OutboxRecord(
                UUID.randomUUID(), "station", "1", "MASTER_DATA_STATION_CREATED",
                "mto.master-data.exchange", "mto.master-data.station.created", "{}", 0, 1L,
                traceParent, traceState);
    }

    @Test
    void sinTrazaActivaNoHayNadaQueGuardar() {
        assertThat(tracing.capture()).isEqualTo(OutboxTraceContext.EMPTY);
        assertThat(tracing.capture().isPresent()).isFalse();
    }

    @Test
    void elContextoCapturadoLlevaElTraceIdDeLaOperacionEnCurso() {
        Span span = tracer.nextSpan().name("PUT /stations/42").start();

        OutboxTraceContext contexto;
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            contexto = tracing.capture();
        } finally {
            span.end();
        }

        assertThat(contexto.isPresent()).isTrue();
        assertThat(contexto.traceParent())
                .as("formato W3C: version-traceid-spanid-flags")
                .matches("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");
        assertThat(contexto.traceParent()).contains(span.context().traceId());
    }

    @Test
    void laPublicacionSeEngranaEnLaTrazaDeLaOperacionOriginal() {
        Span original = tracer.nextSpan().name("PUT /stations/42").start();

        OutboxTraceContext contexto;
        try (Tracer.SpanInScope ignored = tracer.withSpan(original)) {
            contexto = tracing.capture();
        } finally {
            original.end();
        }

        // Minutos despues, en el hilo del scheduler y sin ninguna traza activa.
        assertThat(tracer.currentSpan()).isNull();

        String traceIdDentroDelAmbito;
        try (OutboxTracing.Scope ignored = tracing.startPublishScope(
                record(contexto.traceParent(), contexto.traceState()))) {
            traceIdDentroDelAmbito = tracer.currentSpan().context().traceId();
        }

        assertThat(traceIdDentroDelAmbito)
                .as("sin esto el span de publicacion colgaria del scheduler y la traza"
                        + " quedaria partida en dos mitades que nadie relaciona")
                .isEqualTo(original.context().traceId());
    }

    @Test
    void elAmbitoSeCierraYDejaElHiloComoEstaba() {
        Span original = tracer.nextSpan().start();
        OutboxTraceContext contexto;
        try (Tracer.SpanInScope ignored = tracer.withSpan(original)) {
            contexto = tracing.capture();
        } finally {
            original.end();
        }

        try (OutboxTracing.Scope ignored = tracing.startPublishScope(
                record(contexto.traceParent(), null))) {
            assertThat(tracer.currentSpan()).isNotNull();
        }

        // El hilo del scheduler se reutiliza para el siguiente mensaje: dejarlo con un
        // span abierto haria que el mensaje siguiente heredase una traza que no es suya.
        assertThat(tracer.currentSpan()).isNull();
    }

    @Test
    void unMensajeSinContextoNoAbreNingunAmbito() {
        assertThat(tracing.startPublishScope(record(null, null))).isSameAs(OutboxTracing.Scope.NOOP);
        assertThat(tracing.startPublishScope(record("   ", null))).isSameAs(OutboxTracing.Scope.NOOP);
    }

    @Test
    void unContextoCorruptoNoImpideQueElMensajeSalga() {
        // Publicar es el trabajo; trazar es el extra. Nunca al reves.
        assertThatCode(() -> {
            try (OutboxTracing.Scope ignored = tracing.startPublishScope(record("esto-no-es-un-traceparent", null))) {
                assertThat(true).isTrue();
            }
        }).doesNotThrowAnyException();
    }

    @Test
    void laImplementacionVaciaNoSeInventaNada() {
        NoOpOutboxTracing noOp = new NoOpOutboxTracing();

        assertThat(noOp.capture()).isEqualTo(OutboxTraceContext.EMPTY);
        assertThatCode(() -> noOp.startPublishScope(record("00-x-y-01", null)).close())
                .doesNotThrowAnyException();
    }
}
