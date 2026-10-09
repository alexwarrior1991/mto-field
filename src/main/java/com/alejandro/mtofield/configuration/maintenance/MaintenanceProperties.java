package com.alejandro.mtofield.configuration.maintenance;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Integracion con mto-maintenance ({@code app.maintenance.*}).
 *
 * @param enabled              con false el cliente es un NoOp que devuelve turnos sinteticos, y la
 *                             aplicacion arranca sin mto-maintenance ni cuenta de servicio
 * @param baseUrl              raiz de mto-maintenance (en local el 8083; en compose, el nombre del servicio)
 * @param clientRegistrationId registro de spring.security.oauth2.client con el que se pide el token de servicio
 * @param circuitBreaker       umbrales del circuito que aisla a este servicio de un mantenimiento caido
 * @param syncRetry            el reintento de los eventos de tarea que mto-maintenance no contesto
 */
@Validated
@ConfigurationProperties(prefix = "app.maintenance")
public record MaintenanceProperties(
        boolean enabled,
        @NotBlank String baseUrl,
        @NotBlank String clientRegistrationId,
        CircuitBreaker circuitBreaker,
        SyncRetry syncRetry
) {

    public MaintenanceProperties {
        if (circuitBreaker == null) {
            circuitBreaker = new CircuitBreaker(10, 5, 50, Duration.ofSeconds(30), Duration.ofSeconds(10));
        }
        if (syncRetry == null) {
            syncRetry = new SyncRetry(true, Duration.ofMinutes(1));
        }
    }

    /**
     * @param slidingWindowSize       llamadas que se miran para calcular la tasa de fallo
     * @param minimumNumberOfCalls    por debajo de esto no se calcula la tasa
     * @param failureRateThreshold    porcentaje de fallos a partir del cual el circuito abre
     * @param waitDurationInOpenState tiempo que el circuito permanece abierto antes de probar de nuevo
     * @param timeout                 tiempo maximo de una llamada antes de contarla como fallo
     */
    public record CircuitBreaker(
            int slidingWindowSize,
            int minimumNumberOfCalls,
            int failureRateThreshold,
            Duration waitDurationInOpenState,
            Duration timeout
    ) {
    }

    /**
     * @param enabled  si se reintentan los eventos FAILED (apagado en los tests)
     * @param interval cada cuanto
     */
    public record SyncRetry(boolean enabled, Duration interval) {
    }
}
