package com.alejandro.mtofield.infrastructure.messaging.replicas;

import com.alejandro.mtofield.application.replicas.ReplicaEnvelope;
import com.alejandro.mtofield.application.replicas.ReplicaId;
import com.alejandro.mtofield.application.replicas.ReplicaMessage;
import com.alejandro.mtofield.application.service.ReplicaBus;
import com.alejandro.mtofield.configuration.metrics.FieldMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.Clock;

/**
 * El bus sobre el fanout de RabbitMQ. Publicar es un {@code send} sin confirmacion y sin outbox,
 * a proposito: lo que viaja es efimero (la base es la verdad y el tic de puesta al dia la relee),
 * asi que un broker caido no para el negocio, se registra y se cuenta.
 */
public class RabbitReplicaBus implements ReplicaBus {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitReplicaBus.class);

    private final RabbitTemplate template;
    private final ReplicaEnvelopeCodec codec;
    private final String exchange;
    private final ReplicaId replicaId;
    private final Clock clock;
    private final FieldMetrics metrics;

    public RabbitReplicaBus(RabbitTemplate template, ReplicaEnvelopeCodec codec, String exchange, ReplicaId replicaId, Clock clock,
                            FieldMetrics metrics) {
        this.template = template;
        this.codec = codec;
        this.exchange = exchange;
        this.replicaId = replicaId;
        this.clock = clock;
        this.metrics = metrics;
    }

    public String exchange() {
        return exchange;
    }

    @Override
    public void publish(ReplicaMessage message) {
        ReplicaEnvelope envelope = ReplicaEnvelope.of(replicaId.value(), clock.instant(), message);
        try {
            template.send(exchange, "", toMessage(envelope));
            metrics.recordReplicaMessage(FieldMetrics.REPLICA_OUT, envelope.kind(), FieldMetrics.REPLICA_OUTCOME_SENT);
        } catch (RuntimeException brokerDown) {
            metrics.recordReplicaMessage(FieldMetrics.REPLICA_OUT, envelope.kind(), FieldMetrics.REPLICA_OUTCOME_FAILED);
            LOGGER.warn("Replica message {} not published (the catch-up tick covers it): {}", envelope.kind(), brokerDown.toString());
        }
    }

    Message toMessage(ReplicaEnvelope envelope) {
        return MessageBuilder.withBody(codec.encode(envelope))
                .setContentType(ReplicaRabbitMqNames.CONTENT_TYPE_JSON)
                .setContentEncoding("UTF-8")
                // Efimero: un mensaje que el broker pierda al reiniciar no vale nada cuando vuelve.
                .setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT)
                .setHeader(ReplicaRabbitMqNames.HEADER_REPLICA, envelope.replicaId())
                .setHeader("kind", envelope.kind())
                .build();
    }

}
