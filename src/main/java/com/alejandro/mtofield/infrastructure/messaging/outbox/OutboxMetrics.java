package com.alejandro.mtofield.infrastructure.messaging.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metricas del outbox.
 * <p>
 * Sin esto, que el relay se pare no se nota hasta que alguien pregunta por que un
 * dato maestro no ha llegado. La senal que hay que vigilar es
 * {@code outbox.pending.oldest.age.seconds}: si envejece, el circuito esta roto, sea
 * cual sea la causa (broker caido, base bloqueada, scheduler muerto, mensajes
 * agotando reintentos). Una alerta por encima de 60 segundos cubre practicamente
 * todos los modos de fallo con un solo umbral.
 * <p>
 * Los contadores se llevan en memoria y los gauges se refrescan desde una foto
 * periodica, NO en cada scrape: un gauge consultando la base de datos cada vez que
 * Prometheus pregunta convierte la observabilidad en carga.
 * <p>
 * No se publica el total de PUBLISHED como gauge a proposito: contarlo es recorrer
 * la tabla entera, y lo que de verdad se quiere saber (el ritmo de publicacion) ya
 * lo da el contador {@code outbox.publish.total}. El total absoluto sigue disponible
 * bajo demanda en /actuator/outbox.
 */
@Slf4j
public class OutboxMetrics {

    static final String PENDING_COUNT = "outbox.messages.pending";
    static final String IN_PROGRESS_COUNT = "outbox.messages.in.progress";
    static final String FAILED_COUNT = "outbox.messages.failed";
    static final String OLDEST_PENDING_AGE = "outbox.pending.oldest.age.seconds";
    static final String PUBLISH_COUNT = "outbox.publish.total";

    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong inProgress = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong oldestPendingAgeSeconds = new AtomicLong();

    private final Counter published;
    private final Counter publishFailures;

    public OutboxMetrics(MeterRegistry meterRegistry) {
        Gauge.builder(PENDING_COUNT, pending, AtomicLong::get)
                .description("Mensajes pendientes de publicar")
                .register(meterRegistry);

        Gauge.builder(IN_PROGRESS_COUNT, inProgress, AtomicLong::get)
                .description("Mensajes reclamados por un relay y todavia sin cerrar")
                .register(meterRegistry);

        Gauge.builder(FAILED_COUNT, failed, AtomicLong::get)
                .description("Mensajes que han agotado sus reintentos y esperan un redrive")
                .register(meterRegistry);

        Gauge.builder(OLDEST_PENDING_AGE, oldestPendingAgeSeconds, AtomicLong::get)
                .description("Antiguedad del mensaje pendiente mas antiguo. Si sube, el relay no avanza")
                .baseUnit("seconds")
                .register(meterRegistry);

        this.published = Counter.builder(PUBLISH_COUNT)
                .tag("result", "success")
                .description("Mensajes confirmados por el broker")
                .register(meterRegistry);

        this.publishFailures = Counter.builder(PUBLISH_COUNT)
                .tag("result", "failure")
                .description("Intentos de publicacion sin confirmacion del broker")
                .register(meterRegistry);
    }

    public void recordPublished() {
        published.increment();
    }

    public void recordPublishFailure() {
        publishFailures.increment();
    }

    /** Vuelca una foto del outbox en los gauges. */
    public void update(OutboxRelayHealth health) {
        pending.set(health.pending());
        inProgress.set(health.inProgress());
        failed.set(health.failed());
        oldestPendingAgeSeconds.set(ageInSeconds(health.oldestPendingCreatedAt()));
    }

    private long ageInSeconds(Instant oldestPendingCreatedAt) {
        if (oldestPendingCreatedAt == null) {
            return 0L;
        }

        long seconds = Duration.between(oldestPendingCreatedAt, Instant.now()).getSeconds();
        return Math.max(0L, seconds);
    }
}
