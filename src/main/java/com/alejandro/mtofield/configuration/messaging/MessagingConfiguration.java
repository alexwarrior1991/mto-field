package com.alejandro.mtofield.configuration.messaging;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the signer of what this service publishes.
 *
 * <p>Solo el firmante: este servicio no consume el mensaje de nadie (el bus de replicas es suyo y
 * va sin firma). No esta bajo la condicion de {@code app.rabbitmq.enabled}: la firma es del
 * mensaje, no del transporte, y con el outbox apagado el bean simplemente no lo usa nadie. Lee el
 * mismo secreto que los hermanos: lo que se firma aqui se comprueba en {@code mto-notification}
 * con el mismo valor.</p>
 */
@Configuration
@EnableConfigurationProperties(MessageSignatureProperties.class)
public class MessagingConfiguration {

    @Bean
    public MessagePayloadSignature messagePayloadSignature(MessageSignatureProperties properties) {
        return new MessagePayloadSignature(properties);
    }
}
