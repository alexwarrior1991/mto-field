package com.alejandro.mtofield.configuration.grpc;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Los limites y los tiempos del canal de campo ({@code app.field.*}).
 *
 * @param outboundQueueCapacity ordenes en espera de que el dispositivo lea; desbordarla cierra el stream con RESOURCE_EXHAUSTED
 * @param heldCapacity          ordenes retenidas mientras un stream reproduce su atraso
 * @param workQueueCapacity     eventos de tarea en espera de sincronizarse, por dispositivo
 * @param workQueueIdleTimeout  cuanto espera ocioso el hilo de una cola de trabajo antes de retirarse
 * @param catchUpPageSize       ordenes por pagina al reproducir el atraso
 * @param catchUpPutTimeout     cuanto espera la reproduccion a que haya sitio en la cola de salida
 * @param liveness              los umbrales de vida de un dispositivo
 * @param board                 el tablero
 * @param tokenExpiry           el cierre por caducidad del token (fase 3)
 */
@Validated
@ConfigurationProperties(prefix = "app.field")
public record FieldProperties(
        @Positive int outboundQueueCapacity,
        @Positive int heldCapacity,
        @Positive int workQueueCapacity,
        @NotNull Duration workQueueIdleTimeout,
        @Positive int catchUpPageSize,
        @NotNull Duration catchUpPutTimeout,
        @NotNull Liveness liveness,
        @NotNull Board board,
        @NotNull TokenExpiry tokenExpiry
) {

    public record Liveness(@NotNull Duration staleAfter, @NotNull Duration disconnectedAfter) {
    }

    public record Board(@NotNull Duration tick) {
    }

    public record TokenExpiry(boolean enabled, @NotNull Duration sweep) {
    }
}
