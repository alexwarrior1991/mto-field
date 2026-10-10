package com.alejandro.mtofield.configuration.scheduling;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * El vigilante de acuses ({@code app.field.evacuation.*}).
 *
 * @param enabled    si corre programado; en los tests va apagado y se llama a mano
 * @param ackTimeout cuanto puede tardar un equipo en acusar un desalojo antes de avisar
 * @param checkEvery cada cuanto se miran los desalojos vencidos
 */
@Validated
@ConfigurationProperties(prefix = "app.field.evacuation")
public record EvacuationWatchProperties(
        @DefaultValue("true") boolean enabled,
        @NotNull @DefaultValue("2m") Duration ackTimeout,
        @NotNull @DefaultValue("30s") Duration checkEvery
) {
}
