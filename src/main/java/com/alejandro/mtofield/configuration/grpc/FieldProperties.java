package com.alejandro.mtofield.configuration.grpc;

import jakarta.validation.constraints.NotBlank;
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
 * @param tokenExpiry           el cierre por caducidad del token
 * @param teamBinding           ligar a la persona con su equipo por el claim de grupos del token
 * @param replicas              esta replica entre las demas: su nombre, el tic de puesta al dia y la caducidad de lo remoto
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
        @NotNull TokenExpiry tokenExpiry,
        @NotNull TeamBinding teamBinding,
        @NotNull Replicas replicas
) {

    public record Liveness(@NotNull Duration staleAfter, @NotNull Duration disconnectedAfter) {
    }

    public record Board(@NotNull Duration tick) {
    }

    public record TokenExpiry(boolean enabled, @NotNull Duration sweep) {
    }

    /**
     * @param enabled con true, al unirse a un turno el codigo de su equipo tiene que estar entre los grupos del token
     * @param claim   el claim del access token con los grupos ({@code groups}, el group membership mapper de Keycloak)
     */
    public record TeamBinding(boolean enabled, @NotBlank String claim) {
    }

    /**
     * @param id        el nombre de esta replica en el bus; en blanco, el host con un sufijo aleatorio
     * @param catchUp   cada cuanto se relee de la base lo que el bus no haya traido (ordenes sin abanicar, posesiones cerradas)
     * @param remoteTtl cuanto vale lo que otra replica conto de un dispositivo sin volver a contarlo
     */
    public record Replicas(String id, @NotNull Duration catchUp, @NotNull Duration remoteTtl) {
    }
}
