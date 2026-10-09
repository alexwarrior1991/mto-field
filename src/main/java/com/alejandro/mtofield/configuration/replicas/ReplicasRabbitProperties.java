package com.alejandro.mtofield.configuration.replicas;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * El bus de replicas sobre RabbitMQ ({@code app.rabbitmq.*}).
 *
 * @param enabled  con false no se abre conexion alguna y el bus es un NoOp: la aplicacion arranca sin broker
 * @param replicas el exchange fanout por el que las replicas se hablan (en blanco, el del contrato)
 */
@Validated
@ConfigurationProperties(prefix = "app.rabbitmq")
public record ReplicasRabbitProperties(boolean enabled, Replicas replicas) {

    public static final String DEFAULT_EXCHANGE = "mto.field.replicas.exchange";

    public ReplicasRabbitProperties {
        if (replicas == null) {
            replicas = new Replicas(DEFAULT_EXCHANGE);
        }
    }

    public record Replicas(@NotBlank String exchange) {
        public Replicas {
            exchange = exchange == null || exchange.isBlank() ? DEFAULT_EXCHANGE : exchange;
        }
    }
}
