package com.alejandro.mtofield.infrastructure.messaging.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Refresca la foto del outbox que publican las metricas.
 * <p>
 * Va aparte de {@link OutboxMetrics} para que esa clase no dependa de la base de
 * datos ni del planificador y se pueda probar sola.
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxMetricsScheduler {

    private final OutboxAdminService outboxAdminService;
    private final OutboxMetrics outboxMetrics;

    @Scheduled(fixedDelayString = "${app.outbox.metrics-refresh-delay:30s}")
    public void refreshMetrics() {
        try {
            outboxMetrics.update(outboxAdminService.metricsSnapshot());
        } catch (Exception exception) {
            // Un fallo midiendo no puede tumbar el planificador que tambien mueve el relay.
            log.warn("No se han podido refrescar las metricas del outbox", exception);
        }
    }
}
