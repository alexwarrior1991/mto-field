package com.alejandro.mtofield.configuration.maintenance;

import com.alejandro.mtofield.application.service.MaintenanceClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * El cliente de mto-maintenance que hay en la fase 1: ninguno. Con {@code app.maintenance.enabled=true}
 * (el valor por defecto, y el de la plataforma) la aplicacion se niega a arrancar con un mensaje
 * que dice que hacer, en vez de arrancar y fingir que valida los turnos. La fase 2 sustituye este
 * bean por el {@code RestClientMaintenanceClient} con la cuenta de servicio y el circuito.
 */
@Configuration
@EnableConfigurationProperties(MaintenanceProperties.class)
public class MaintenanceClientConfiguration {

    @Bean
    @ConditionalOnProperty(prefix = "app.maintenance", name = "enabled", havingValue = "true", matchIfMissing = true)
    public MaintenanceClient restClientMaintenanceClient(MaintenanceProperties properties) {
        throw new IllegalStateException("app.maintenance.enabled=true needs the REST client of mto-maintenance, which arrives in Phase 2;"
                + " run with APP_MAINTENANCE_ENABLED=false until then (base-url was " + properties.baseUrl() + ")");
    }
}
