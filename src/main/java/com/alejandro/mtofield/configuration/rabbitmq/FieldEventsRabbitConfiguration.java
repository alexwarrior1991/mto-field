package com.alejandro.mtofield.configuration.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * El exchange por el que salen los eventos propios de este servicio (docs/06-messaging.md).
 *
 * <p>Solo el exchange, topic y durable, y ninguna cola: una cola es de quien la consume
 * ({@code mto-notification} declara la suya, ligada con {@code mto.field.#}). Comparte la
 * conexion y la plantilla con el bus de replicas, pero no el exchange: aquel es un fanout
 * transitorio de este servicio consigo mismo; este es un contrato con los demas. Con
 * {@code app.rabbitmq.enabled=false} no se declara nada y el publicador es el NoOp.</p>
 */
@Configuration
@EnableConfigurationProperties(FieldEventsProperties.class)
@ConditionalOnProperty(prefix = "app.rabbitmq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class FieldEventsRabbitConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldEventsRabbitConfiguration.class);

    @Bean
    public TopicExchange fieldEventsExchange(FieldEventsProperties properties) {
        LOGGER.info("Own events on exchange {} (topic, durable; no queue here)", properties.exchange());
        return new TopicExchange(properties.exchange(), true, false);
    }
}
