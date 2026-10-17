package com.alejandro.mtofield.configuration.positions;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * La posicion de los vehiculos ({@code app.field.positions.*}): el kp que un dispositivo dice en su
 * latido, del vehiculo con el que se unio al turno, va a {@code mto-resources}.
 *
 * @param enabled     si se publica; con {@code false} los latidos solo alimentan el tablero, como antes
 * @param minInterval lo menos que pasa entre dos posiciones del mismo dispositivo, aunque el kp cambie antes
 */
@Validated
@ConfigurationProperties(prefix = "app.field.positions")
public record PositionsProperties(
        @DefaultValue("true") boolean enabled,
        @NotNull @DefaultValue("60s") Duration minInterval
) {
}
