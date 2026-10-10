package com.alejandro.mtofield.configuration.scheduling;

import com.alejandro.mtofield.application.service.EvacuationAckWatchdog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * El vigilante de acuses, cada {@code app.field.evacuation.check-every}; solo con
 * {@code app.field.evacuation.enabled} (apagado en los tests, que llaman a {@code check()} a
 * mano). El {@code @EnableScheduling} esta en {@link FieldSchedulingConfiguration}. Con varias
 * replicas corre en todas y publica una: lo decide el UPDATE condicional de la base.
 */
@Configuration
@ConditionalOnProperty(prefix = "app.field.evacuation", name = "enabled", havingValue = "true", matchIfMissing = true)
public class EvacuationWatchConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(EvacuationWatchConfiguration.class);

    private final EvacuationAckWatchdog watchdog;

    public EvacuationWatchConfiguration(EvacuationAckWatchdog watchdog) {
        this.watchdog = watchdog;
    }

    @Scheduled(fixedDelayString = "${app.field.evacuation.check-every:30s}", initialDelayString = "${app.field.evacuation.check-every:30s}")
    public void checkOverdueEvacuations() {
        int published = watchdog.check();
        if (published > 0) {
            LOGGER.warn("Evacuation watch: {} evacuation(s) still unacknowledged past the timeout", published);
        }
    }
}
