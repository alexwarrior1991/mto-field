package com.alejandro.mtofield.configuration.scheduling;

import com.alejandro.mtofield.application.service.FieldEventSyncRetryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * El reintento programado de los eventos de tarea que mto-maintenance no contesto, cada
 * {@code app.maintenance.sync-retry.interval}. Solo con el cliente de mantenimiento encendido y
 * {@code app.maintenance.sync-retry.enabled} (apagado en los tests, que llaman al servicio a mano).
 * El {@code @EnableScheduling} esta en {@link FieldSchedulingConfiguration}.
 */
@Configuration
@ConditionalOnProperty(prefix = "app.maintenance", name = {"enabled", "sync-retry.enabled"}, havingValue = "true", matchIfMissing = true)
public class FieldEventSyncRetryConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldEventSyncRetryConfiguration.class);

    private final FieldEventSyncRetryService retry;

    public FieldEventSyncRetryConfiguration(FieldEventSyncRetryService retry) {
        this.retry = retry;
    }

    @Scheduled(fixedDelayString = "${app.maintenance.sync-retry.interval:PT1M}", initialDelayString = "${app.maintenance.sync-retry.interval:PT1M}")
    public void retryDueEvents() {
        int resolved = retry.retryDue();
        if (resolved > 0) {
            LOGGER.info("Sync retry: {} task event(s) resolved", resolved);
        }
    }
}
