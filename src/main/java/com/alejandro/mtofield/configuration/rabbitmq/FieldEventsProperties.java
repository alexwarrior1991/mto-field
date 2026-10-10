package com.alejandro.mtofield.configuration.rabbitmq;

import com.alejandro.mtofield.infrastructure.messaging.rabbitmq.FieldRabbitMqNames;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Name of the exchange this service publishes its own events to, configurable per environment.
 *
 * <p>Como {@code ReplicasRabbitProperties}: el valor por defecto es el contrato
 * ({@code mto.field.exchange}), y una cadena en blanco cuenta como no configurada en vez de
 * arrancar contra un exchange llamado {@code ""}. Solo el exchange: las claves de enrutado salen de
 * {@link FieldRabbitMqNames} y la cola es de quien la consume.</p>
 */
@Validated
@ConfigurationProperties(prefix = "app.rabbitmq.events")
public record FieldEventsProperties(@NotBlank String exchange) {

    public FieldEventsProperties {
        exchange = exchange == null || exchange.isBlank() ? FieldRabbitMqNames.FIELD_EXCHANGE : exchange;
    }
}
