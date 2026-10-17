package com.alejandro.mtofield.configuration.rabbitmq;

import com.alejandro.mtofield.infrastructure.messaging.rabbitmq.FieldRabbitMqNames;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Names of the exchanges this service publishes to, configurable per environment: its own events
 * ({@code mto.field.exchange}) and the telemetry ({@code mto.field.telemetry.exchange}).
 *
 * <p>Como {@code ReplicasRabbitProperties}: el valor por defecto es el contrato, y una cadena en blanco
 * cuenta como no configurada en vez de arrancar contra un exchange llamado {@code ""}. Solo los
 * exchanges: las claves de enrutado salen de {@link FieldRabbitMqNames} y las colas son de quien las
 * consume.</p>
 */
@Validated
@ConfigurationProperties(prefix = "app.rabbitmq.events")
public record FieldEventsProperties(@NotBlank String exchange, @NotBlank String telemetryExchange) {

    public FieldEventsProperties {
        exchange = exchange == null || exchange.isBlank() ? FieldRabbitMqNames.FIELD_EXCHANGE : exchange;
        telemetryExchange = telemetryExchange == null || telemetryExchange.isBlank() ? FieldRabbitMqNames.FIELD_TELEMETRY_EXCHANGE
                : telemetryExchange;
    }

}
