package com.alejandro.mtofield.infrastructure.messaging.replicas;

import com.alejandro.mtofield.application.replicas.ReplicaEnvelope;
import com.alejandro.mtofield.application.replicas.ReplicaId;
import com.alejandro.mtofield.application.replicas.ReplicaMessage;
import com.alejandro.mtofield.application.service.ReplicaMessageHandler;
import com.alejandro.mtofield.configuration.metrics.FieldMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;

import java.util.List;
import java.util.Optional;

/**
 * Lo que llega por la cola de esta replica. Nunca lanza: un cuerpo que no es un sobre, un
 * {@code kind} desconocido o un manejador que falla se registran y se cuentan, y el mensaje se
 * da por consumido (no hay DLQ: lo que viaja es efimero y la base lo cubre). Lo propio se ignora
 * por {@code replicaId}: el fanout tambien lo devuelve a quien lo publico.
 */
public class ReplicaMessageConsumer implements MessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaMessageConsumer.class);

    private final ReplicaEnvelopeCodec codec;
    private final ReplicaId replicaId;
    private final List<ReplicaMessageHandler> handlers;
    private final FieldMetrics metrics;

    public ReplicaMessageConsumer(ReplicaEnvelopeCodec codec, ReplicaId replicaId, List<ReplicaMessageHandler> handlers, FieldMetrics metrics) {
        this.codec = codec;
        this.replicaId = replicaId;
        this.handlers = List.copyOf(handlers);
        this.metrics = metrics;
    }

    @Override
    public void onMessage(Message message) {
        ReplicaEnvelope envelope;
        try {
            envelope = codec.decode(message.getBody());
        } catch (IllegalArgumentException notAnEnvelope) {
            metrics.recordReplicaMessage(FieldMetrics.REPLICA_IN, "unknown", FieldMetrics.REPLICA_OUTCOME_DROPPED);
            LOGGER.warn("Replica message dropped: {}", notAnEnvelope.getMessage());
            return;
        }
        if (replicaId.value().equals(envelope.replicaId())) {
            metrics.recordReplicaMessage(FieldMetrics.REPLICA_IN, envelope.kind(), FieldMetrics.REPLICA_OUTCOME_OWN);
            return;
        }
        Optional<ReplicaMessage> payload = envelope.message();
        if (payload.isEmpty()) {
            metrics.recordReplicaMessage(FieldMetrics.REPLICA_IN, String.valueOf(envelope.kind()), FieldMetrics.REPLICA_OUTCOME_DROPPED);
            LOGGER.info("Replica message of kind {} from {} ignored: not known to this version", envelope.kind(), envelope.replicaId());
            return;
        }
        for (ReplicaMessageHandler handler : handlers) {
            try {
                handler.onReplicaMessage(envelope.replicaId(), payload.get());
            } catch (RuntimeException failure) {
                LOGGER.warn("Handler {} failed on replica message {} from {}: {}", handler.getClass().getSimpleName(), envelope.kind(),
                        envelope.replicaId(), failure.toString());
            }
        }
        metrics.recordReplicaMessage(FieldMetrics.REPLICA_IN, envelope.kind(), FieldMetrics.REPLICA_OUTCOME_HANDLED);
    }
}
