package com.alejandro.mtofield.infrastructure.messaging.outbox;

import com.alejandro.mtofield.configuration.messaging.MessagePayloadSignature;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageBuilderSupport;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publica un mensaje del outbox y ESPERA la confirmacion del broker.
 * <p>
 * Antes se llamaba a {@code rabbitTemplate.send(...)} y se marcaba PUBLISHED acto
 * seguido. Ese send es fire-and-forget: vuelve en cuanto el byte sale del socket, de
 * modo que si el broker moria antes de persistir el mensaje, el evento se perdia y la
 * tabla afirmaba que se habia enviado. Es justo la garantia que el patron outbox
 * existe para dar, asi que un mensaje solo pasa a PUBLISHED con ack del broker.
 * <p>
 * Se comprueban las dos formas de fracaso silencioso:
 * <ul>
 *   <li><b>nack</b>: el broker rechaza el mensaje.</li>
 *   <li><b>return</b>: con {@code mandatory=true}, un mensaje que no encaja en
 *       ninguna cola vuelve al publicador en lugar de descartarse. El basic.return
 *       llega SIEMPRE antes del ack, por eso se consulta despues de esperar el
 *       confirm.</li>
 * </ul>
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxRabbitPublisher {

    private final RabbitTemplate rabbitTemplate;
    private final OutboxProperties outboxProperties;
    private final OutboxTracing outboxTracing;
    private final MessagePayloadSignature messagePayloadSignature;

    /**
     * Sin publisher confirms, el future de CorrelationData no se completa nunca y el
     * relay se quedaria esperando hasta el timeout en CADA mensaje. Antes que degradar
     * en silencio (que es el fallo que estamos corrigiendo), no arranca.
     */
    @PostConstruct
    void verifyPublisherConfirmsEnabled() {
        ConnectionFactory connectionFactory = rabbitTemplate.getConnectionFactory();

        if (connectionFactory instanceof CachingConnectionFactory caching && caching.isPublisherConfirms()) {
            return;
        }

        throw new IllegalStateException("""
                El relay del outbox exige publisher confirms para poder marcar un mensaje \
                como PUBLISHED solo cuando RabbitMQ lo ha aceptado. \
                Configura spring.rabbitmq.publisher-confirm-type=correlated \
                (y publisher-returns=true para detectar mensajes no enrutables).""");
    }

    public void publish(OutboxRecord record) {
        // El ambito engancha la publicacion a la traza de la operacion que genero el
        // evento; sin el, este span colgaria del scheduler y la traza quedaria partida.
        try (OutboxTracing.Scope ignored = outboxTracing.startPublishScope(record)) {
            doPublish(record);
        }
    }

    private void doPublish(OutboxRecord record) {
        CorrelationData correlationData = new CorrelationData(record.id().toString());

        rabbitTemplate.send(record.exchangeName(), record.routingKey(), toMessage(record), correlationData);

        CorrelationData.Confirm confirm = waitForConfirm(record, correlationData);

        ReturnedMessage returned = correlationData.getReturned();
        if (returned != null) {
            throw new OutboxPublishException(
                    "Mensaje no enrutable (exchange=%s, routingKey=%s): %d %s".formatted(
                            record.exchangeName(), record.routingKey(),
                            returned.getReplyCode(), returned.getReplyText())
            );
        }

        if (confirm == null || !confirm.ack()) {
            throw new OutboxPublishException(
                    "RabbitMQ ha rechazado el mensaje (nack): %s".formatted(
                            confirm == null ? "sin detalle" : confirm.reason())
            );
        }

        log.info(
                "Outbox message publicado y confirmado. id={}, eventType={}, exchange={}, routingKey={}",
                record.id(), record.eventType(), record.exchangeName(), record.routingKey()
        );
    }

    private CorrelationData.Confirm waitForConfirm(OutboxRecord record, CorrelationData correlationData) {
        try {
            return correlationData.getFuture()
                    .get(outboxProperties.getConfirmTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException exception) {
            throw new OutboxPublishException(
                    "RabbitMQ no ha confirmado el mensaje %s en %s".formatted(
                            record.id(), outboxProperties.getConfirmTimeout()),
                    exception
            );
        } catch (ExecutionException exception) {
            throw new OutboxPublishException(
                    "Error esperando la confirmacion del mensaje %s".formatted(record.id()),
                    exception.getCause() != null ? exception.getCause() : exception
            );
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new OutboxPublishException(
                    "Espera de confirmacion interrumpida para el mensaje %s".formatted(record.id()),
                    exception
            );
        }
    }

    private Message toMessage(OutboxRecord record) {
        byte[] body = record.payload().getBytes(StandardCharsets.UTF_8);

        MessageBuilderSupport<Message> builder = MessageBuilder
                .withBody(body)
                .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                .setContentEncoding(StandardCharsets.UTF_8.name())
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                .setMessageId(record.id().toString())
                .setHeader("eventType", record.eventType())
                .setHeader("aggregateType", record.aggregateType())
                .setHeader("aggregateId", record.aggregateId())
                // Defensa en profundidad del orden: el relay ya publica en orden por
                // agregado, pero la entrega es at-least-once y un redrive puede
                // reenviar algo antiguo. Con este numero el consumidor puede descartar
                // lo que sea anterior a lo que ya ha aplicado.
                .setHeader("sequenceNumber", record.sequenceNumber())
                // Firma sobre los bytes que viajan, no sobre el objeto: es lo unico
                // que el consumidor puede recomprobar sin reserializar, y por tanto lo
                // unico verificable de verdad. Va en cabecera porque un payload no
                // puede contener su propia firma.
                .setHeader(MessagePayloadSignature.HEADER_SIGNATURE,
                        messagePayloadSignature.sign(body))
                .setHeader(MessagePayloadSignature.HEADER_SIGNATURE_ALGORITHM,
                        messagePayloadSignature.algorithm());

        // Suelo de propagacion: con la instrumentacion de Spring AMQP activa, esta
        // cabecera se sobrescribe con la del span hijo (mismo trace-id), que enlaza
        // mejor todavia. Sin instrumentacion, esto es lo que hace que el consumidor
        // siga perteneciendo a la traza original en vez de empezar una nueva.
        if (record.traceParent() != null && !record.traceParent().isBlank()) {
            builder.setHeader(MicrometerOutboxTracing.TRACE_PARENT, record.traceParent());
        }
        if (record.traceState() != null && !record.traceState().isBlank()) {
            builder.setHeader(MicrometerOutboxTracing.TRACE_STATE, record.traceState());
        }

        return builder.build();
    }
}
