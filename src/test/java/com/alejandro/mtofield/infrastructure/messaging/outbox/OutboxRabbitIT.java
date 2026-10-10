package com.alejandro.mtofield.infrastructure.messaging.outbox;

import com.alejandro.mtofield.configuration.messaging.MessagePayloadSignature;
import com.alejandro.mtofield.configuration.messaging.MessagingConfiguration;
import com.alejandro.mtofield.configuration.rabbitmq.FieldEventsRabbitConfiguration;
import com.alejandro.mtofield.infrastructure.messaging.rabbitmq.FieldRabbitMqNames;
import com.alejandro.mtofield.support.RabbitMqTestBroker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * El publicador del outbox sobre un RabbitMQ de verdad ({@link RabbitMqTestBroker}), con la
 * plantilla que autoconfigura Boot con confirms y returns, como en produccion: lo que publica
 * llega a una cola ligada al exchange propio con el cuerpo, la firma y las cabeceras del
 * contrato, y lo que el broker no puede enrutar no se da por publicado.
 */
class OutboxRabbitIT {

    private static final String EXCHANGE = "mto.field.it.exchange";
    private static Map<String, Object> broker;

    @BeforeAll
    static void broker() {
        broker = RabbitMqTestBroker.properties();
    }

    private ApplicationContextRunner runner() {
        ApplicationContextRunner runner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RabbitAutoConfiguration.class, JacksonAutoConfiguration.class))
                .withUserConfiguration(FieldEventsRabbitConfiguration.class, MessagingConfiguration.class)
                .withPropertyValues("app.rabbitmq.enabled=true", "app.rabbitmq.events.exchange=" + EXCHANGE,
                        "spring.rabbitmq.publisher-confirm-type=correlated", "spring.rabbitmq.publisher-returns=true",
                        "spring.rabbitmq.template.mandatory=true",
                        "app.messaging.signature.secret=it-secret", "app.messaging.signature.mode=OPTIONAL");
        for (Map.Entry<String, Object> property : broker.entrySet()) {
            runner = runner.withPropertyValues(property.getKey() + "=" + property.getValue());
        }
        return runner;
    }

    @Test
    void whatThePublisherConfirmsReachesTheQueueBoundToTheOwnExchangeSignedAndWithItsHeaders() {
        runner().run(context -> {
            RabbitTemplate template = context.getBean(RabbitTemplate.class);
            MessagePayloadSignature signature = context.getBean(MessagePayloadSignature.class);
            AmqpAdmin admin = context.getBean(AmqpAdmin.class);
            Queue queue = QueueBuilder.nonDurable("mto.field.it.queue." + UUID.randomUUID()).exclusive().autoDelete().build();
            admin.declareQueue(queue);
            admin.declareBinding(BindingBuilder.bind(queue).to(context.getBean(TopicExchange.class)).with(FieldRabbitMqNames.FIELD_ROUTING_PATTERN));

            OutboxRabbitPublisher publisher = new OutboxRabbitPublisher(template, new OutboxProperties(), new NoOpOutboxTracing(), signature);
            publisher.verifyPublisherConfirmsEnabled();
            UUID id = UUID.randomUUID();
            String payload = "{\"operationId\":\"" + id + "\",\"data\":{\"entityName\":\"possession\"}}";
            publisher.publish(new OutboxRecord(id, "possession", "p-1", FieldRabbitMqNames.eventType("possession", "opened"), EXCHANGE,
                    FieldRabbitMqNames.routingKey("possession", "opened"), payload, 0, 42L, null, null));

            Message received = template.receive(queue.getName(), 15_000);
            assertThat(received).as("the confirmed message is in the queue").isNotNull();
            assertThat(new String(received.getBody(), StandardCharsets.UTF_8)).isEqualTo(payload);
            assertThat(received.getMessageProperties().getMessageId()).isEqualTo(id.toString());
            assertThat(received.getMessageProperties().getReceivedRoutingKey()).isEqualTo("mto.field.possession.opened");
            assertThat(received.getMessageProperties().getReceivedDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
            assertThat(received.getMessageProperties().getHeader("eventType").toString()).isEqualTo("FIELD_POSSESSION_OPENED");
            assertThat(received.getMessageProperties().getHeader("aggregateType").toString()).isEqualTo("possession");
            assertThat(received.getMessageProperties().getHeader("sequenceNumber").toString()).isEqualTo("42");
            assertThat(received.getMessageProperties().getHeader(MessagePayloadSignature.HEADER_SIGNATURE).toString())
                    .isEqualTo(signature.sign(payload.getBytes(StandardCharsets.UTF_8)));
            assertThat(received.getMessageProperties().getHeader(MessagePayloadSignature.HEADER_SIGNATURE_ALGORITHM).toString())
                    .isEqualTo("HMAC-SHA256");

            // Un mensaje que ninguna cola recibe vuelve devuelto, y el relay no lo marca PUBLISHED.
            assertThatThrownBy(() -> publisher.publish(new OutboxRecord(UUID.randomUUID(), "possession", "p-1", "FIELD_X", EXCHANGE,
                    "nobody.listens.here", payload, 0, 43L, null, null)))
                    .isInstanceOf(OutboxPublishException.class);
        });
    }
}
