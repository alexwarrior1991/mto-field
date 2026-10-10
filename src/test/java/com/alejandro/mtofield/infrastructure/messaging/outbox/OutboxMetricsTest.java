package com.alejandro.mtofield.infrastructure.messaging.outbox;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxMetricsTest {

    private MeterRegistry registry;
    private OutboxMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new OutboxMetrics(registry);
    }

    private double gauge(String name) {
        return registry.get(name).gauge().value();
    }

    private double counter(String result) {
        return registry.get(OutboxMetrics.PUBLISH_COUNT).tag("result", result).counter().count();
    }

    @Test
    void losGaugesSeRegistranDesdeElArranqueAunSinDatos() {
        // Un gauge que solo aparece cuando ya hay problema no sirve para alertar:
        // la alerta no puede distinguir "todo bien" de "la metrica no existe".
        assertThat(gauge(OutboxMetrics.PENDING_COUNT)).isZero();
        assertThat(gauge(OutboxMetrics.IN_PROGRESS_COUNT)).isZero();
        assertThat(gauge(OutboxMetrics.FAILED_COUNT)).isZero();
        assertThat(gauge(OutboxMetrics.OLDEST_PENDING_AGE)).isZero();
        assertThat(counter("success")).isZero();
        assertThat(counter("failure")).isZero();
    }

    @Test
    void losGaugesReflejanLaUltimaFotoDelOutbox() {
        metrics.update(new OutboxRelayHealth(7, 2, 1, Instant.now().minus(Duration.ofMinutes(5))));

        assertThat(gauge(OutboxMetrics.PENDING_COUNT)).isEqualTo(7);
        assertThat(gauge(OutboxMetrics.IN_PROGRESS_COUNT)).isEqualTo(2);
        assertThat(gauge(OutboxMetrics.FAILED_COUNT)).isEqualTo(1);
        assertThat(gauge(OutboxMetrics.OLDEST_PENDING_AGE))
                .as("es la senal que hay que vigilar: si envejece, el relay no avanza")
                .isBetween(295d, 305d);
    }

    @Test
    void sinPendientesLaEdadEsCeroYNoUnValorEngañoso() {
        metrics.update(new OutboxRelayHealth(5, 0, 0, Instant.now().minus(Duration.ofHours(1))));
        assertThat(gauge(OutboxMetrics.OLDEST_PENDING_AGE)).isGreaterThan(3000d);

        metrics.update(new OutboxRelayHealth(0, 0, 0, null));

        assertThat(gauge(OutboxMetrics.OLDEST_PENDING_AGE))
                .as("al vaciarse la cola la edad debe volver a cero, o la alerta se queda pegada")
                .isZero();
    }

    @Test
    void unaFechaEnElFuturoNoProduceUnaEdadNegativa() {
        // Reloj de la base de datos adelantado respecto al de la aplicacion.
        metrics.update(new OutboxRelayHealth(1, 0, 0, Instant.now().plus(Duration.ofMinutes(10))));

        assertThat(gauge(OutboxMetrics.OLDEST_PENDING_AGE)).isZero();
    }

    @Test
    void losContadoresSeparanPublicacionesYFallos() {
        metrics.recordPublished();
        metrics.recordPublished();
        metrics.recordPublishFailure();

        assertThat(counter("success")).isEqualTo(2);
        assertThat(counter("failure")).isEqualTo(1);
    }
}
