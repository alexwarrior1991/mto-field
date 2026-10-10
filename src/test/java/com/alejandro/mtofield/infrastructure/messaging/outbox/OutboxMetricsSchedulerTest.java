package com.alejandro.mtofield.infrastructure.messaging.outbox;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxMetricsSchedulerTest {

    @Mock
    private OutboxAdminService outboxAdminService;

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final OutboxMetrics metrics = new OutboxMetrics(registry);

    private OutboxMetricsScheduler scheduler() {
        return new OutboxMetricsScheduler(outboxAdminService, metrics);
    }

    @Test
    void vuelcaLaFotoDelOutboxEnLosGauges() {
        when(outboxAdminService.metricsSnapshot())
                .thenReturn(new OutboxRelayHealth(4, 1, 2, Instant.now()));

        scheduler().refreshMetrics();

        assertThat(registry.get(OutboxMetrics.PENDING_COUNT).gauge().value()).isEqualTo(4);
        assertThat(registry.get(OutboxMetrics.FAILED_COUNT).gauge().value()).isEqualTo(2);
    }

    @Test
    void unFalloMidiendoNoTumbaElPlanificador() {
        // El mismo planificador mueve el relay: si una excepcion midiendo se propagara,
        // dejaria de publicarse el outbox por no poder contar filas.
        when(outboxAdminService.metricsSnapshot()).thenThrow(new IllegalStateException("base caida"));

        assertThatCode(() -> scheduler().refreshMetrics()).doesNotThrowAnyException();
    }
}
