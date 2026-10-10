package com.alejandro.mtofield.infrastructure.messaging.outbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * El relay marcaba PUBLISHED justo despues de {@code rabbitTemplate.send(...)}, que es
 * fire-and-forget: si el broker moria antes de persistir el mensaje, el evento se
 * perdia y la tabla afirmaba que se habia enviado. Aqui se fija que solo un ack del
 * broker cuenta como publicacion, y que nack, mensaje no enrutable y silencio son
 * fallos explicitos.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class OutboxRabbitPublisherTest {

    @Mock
    private RabbitTemplate rabbitTemplate;

    private OutboxProperties outboxProperties;
    private OutboxRabbitPublisher publisher;
    private OutboxTracing outboxTracing;
    private com.alejandro.mtofield.configuration.messaging.MessagePayloadSignature messagePayloadSignature;

    private final OutboxRecord record = new OutboxRecord(
            UUID.randomUUID(),
            "station",
            "42",
            "MASTER_DATA_STATION_CREATED",
            "mto.master-data.exchange",
            "mto.master-data.station.created",
            "{\"id\":42}",
            0,
            7L,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "vendor=abc"
    );

    @BeforeEach
    void setUp() {
        outboxProperties = new OutboxProperties();
        outboxProperties.setConfirmTimeout(Duration.ofMillis(300));
        outboxTracing = org.mockito.Mockito.spy(new NoOpOutboxTracing());
        messagePayloadSignature = new com.alejandro.mtofield.configuration.messaging.MessagePayloadSignature(
                new com.alejandro.mtofield.configuration.messaging.MessageSignatureProperties(
                        "secreto-de-test", com.alejandro.mtofield.configuration.messaging.MessageSignatureMode.OPTIONAL));

        publisher = new OutboxRabbitPublisher(
                rabbitTemplate, outboxProperties, outboxTracing, messagePayloadSignature);
    }

    /** Simula la respuesta del broker sobre el CorrelationData que recibe el template. */
    private void brokerResponds(Consumer<CorrelationData> behaviour) {
        doAnswer(invocation -> {
            behaviour.accept(invocation.getArgument(3));
            return null;
        }).when(rabbitTemplate).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));
    }

    @Test
    void publicaCuandoElBrokerConfirmaConAck() {
        brokerResponds(correlation -> correlation.getFuture().complete(new CorrelationData.Confirm(true, null)));

        assertThatCode(() -> publisher.publish(record)).doesNotThrowAnyException();
    }

    @Test
    void fallaCuandoElBrokerRespondeNack() {
        brokerResponds(correlation ->
                correlation.getFuture().complete(new CorrelationData.Confirm(false, "disco lleno")));

        assertThatThrownBy(() -> publisher.publish(record))
                .isInstanceOf(OutboxPublishException.class)
                .hasMessageContaining("nack")
                .hasMessageContaining("disco lleno");
    }

    @Test
    void fallaCuandoElMensajeNoEsEnrutable() {
        // Con mandatory=true el basic.return llega SIEMPRE antes del ack: un mensaje
        // que no encaja en ninguna cola se confirma igualmente, y sin mirar el return
        // se daria por publicado algo que nadie ha recibido.
        brokerResponds(correlation -> {
            correlation.setReturned(new ReturnedMessage(
                    new Message(new byte[0], new MessageProperties()),
                    312, "NO_ROUTE", record.exchangeName(), record.routingKey()));
            correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
        });

        assertThatThrownBy(() -> publisher.publish(record))
                .isInstanceOf(OutboxPublishException.class)
                .hasMessageContaining("no enrutable")
                .hasMessageContaining("NO_ROUTE");
    }

    @Test
    void fallaCuandoElBrokerNoContestaDentroDelPlazo() {
        brokerResponds(correlation -> {
            // silencio deliberado: el future no se completa nunca
        });

        long inicio = System.nanoTime();

        assertThatThrownBy(() -> publisher.publish(record))
                .isInstanceOf(OutboxPublishException.class)
                .hasMessageContaining("no ha confirmado");

        Duration transcurrido = Duration.ofNanos(System.nanoTime() - inicio);
        assertThat(transcurrido)
                .as("la espera debe estar acotada por confirm-timeout, no ser indefinida")
                .isLessThan(Duration.ofSeconds(5));
    }

    @Test
    void elMensajeViajaComoJsonPersistenteYConSusCabeceras() {
        brokerResponds(correlation -> correlation.getFuture().complete(new CorrelationData.Confirm(true, null)));

        publisher.publish(record);

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        org.mockito.Mockito.verify(rabbitTemplate)
                .send(anyString(), anyString(), captor.capture(), any(CorrelationData.class));

        MessageProperties properties = captor.getValue().getMessageProperties();

        assertThat(new String(captor.getValue().getBody(), StandardCharsets.UTF_8)).isEqualTo(record.payload());
        assertThat(properties.getContentType()).isEqualTo(MessageProperties.CONTENT_TYPE_JSON);
        assertThat(properties.getDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
        // messageId permite al consumidor deduplicar: el outbox garantiza at-least-once
        assertThat(properties.getMessageId()).isEqualTo(record.id().toString());
        assertThat(properties.getHeaders())
                .containsEntry("eventType", record.eventType())
                .containsEntry("aggregateType", record.aggregateType())
                .containsEntry("aggregateId", record.aggregateId())
                // Permite al consumidor descartar lo que sea anterior a lo ya aplicado
                .containsEntry("sequenceNumber", record.sequenceNumber());
    }

    @Test
    void elMensajeViajaFirmadoSobreLosBytesQueSeEnvian() {
        brokerResponds(correlation -> correlation.getFuture().complete(new CorrelationData.Confirm(true, null)));

        publisher.publish(record);

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        org.mockito.Mockito.verify(rabbitTemplate)
                .send(anyString(), anyString(), captor.capture(), any(CorrelationData.class));

        Message enviado = captor.getValue();
        String firma = (String) enviado.getMessageProperties()
                .getHeaders()
                .get(com.alejandro.mtofield.configuration.messaging.MessagePayloadSignature.HEADER_SIGNATURE);

        // La firma tiene que cuadrar con el CUERPO tal cual sale, que es lo unico que
        // el consumidor puede recomprobar sin reserializar nada.
        assertThat(messagePayloadSignature.verify(enviado.getBody(), firma)).isTrue();

        assertThat(enviado.getMessageProperties().getHeaders())
                .containsEntry(
                        com.alejandro.mtofield.configuration.messaging.MessagePayloadSignature.HEADER_SIGNATURE_ALGORITHM,
                        "HMAC-SHA256");
    }

    @Test
    void elMensajeArrastraElContextoDeTrazaDeLaOperacionQueLoGenero() {
        brokerResponds(correlation -> correlation.getFuture().complete(new CorrelationData.Confirm(true, null)));

        publisher.publish(record);

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        org.mockito.Mockito.verify(rabbitTemplate)
                .send(anyString(), anyString(), captor.capture(), any(CorrelationData.class));

        // Sin estas cabeceras el consumidor empieza una traza nueva y el recorrido del
        // evento queda partido en dos mitades que nadie relaciona.
        assertThat(captor.getValue().getMessageProperties().getHeaders())
                .containsEntry("traceparent", record.traceParent())
                .containsEntry("tracestate", record.traceState());
    }

    @Test
    void laPublicacionOcurreDentroDelAmbitoDeTrazaDelMensaje() {
        brokerResponds(correlation -> correlation.getFuture().complete(new CorrelationData.Confirm(true, null)));
        OutboxTracing.Scope scope = org.mockito.Mockito.mock(OutboxTracing.Scope.class);
        org.mockito.Mockito.doReturn(scope).when(outboxTracing).startPublishScope(record);

        publisher.publish(record);

        // El envio tiene que caer DENTRO del ambito: si se abriera despues, el span de
        // publicacion seguiria colgando del scheduler y no de la operacion original.
        org.mockito.InOrder orden = org.mockito.Mockito.inOrder(outboxTracing, rabbitTemplate, scope);
        orden.verify(outboxTracing).startPublishScope(record);
        orden.verify(rabbitTemplate).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));
        orden.verify(scope).close();
    }

    @Test
    void elAmbitoSeCierraAunqueLaPublicacionFalle() {
        brokerResponds(correlation ->
                correlation.getFuture().complete(new CorrelationData.Confirm(false, "disco lleno")));
        OutboxTracing.Scope scope = org.mockito.Mockito.mock(OutboxTracing.Scope.class);
        org.mockito.Mockito.doReturn(scope).when(outboxTracing).startPublishScope(record);

        assertThatThrownBy(() -> publisher.publish(record)).isInstanceOf(OutboxPublishException.class);

        // El hilo del scheduler se reutiliza para el mensaje siguiente: dejarlo con un
        // span abierto haria que el siguiente heredase una traza que no es suya.
        org.mockito.Mockito.verify(scope).close();
    }

    @Test
    void unMensajeSinContextoDeTrazaNoInventaCabeceras() {
        brokerResponds(correlation -> correlation.getFuture().complete(new CorrelationData.Confirm(true, null)));

        OutboxRecord sinTraza = new OutboxRecord(
                record.id(), record.aggregateType(), record.aggregateId(), record.eventType(),
                record.exchangeName(), record.routingKey(), record.payload(), 0, 7L, null, null);

        publisher.publish(sinTraza);

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        org.mockito.Mockito.verify(rabbitTemplate)
                .send(anyString(), anyString(), captor.capture(), any(CorrelationData.class));

        assertThat(captor.getValue().getMessageProperties().getHeaders())
                .doesNotContainKey("traceparent")
                .doesNotContainKey("tracestate");
    }

    @Test
    void noArrancaSiLosPublisherConfirmsEstanDesactivados() {
        CachingConnectionFactory connectionFactory = org.mockito.Mockito.mock(CachingConnectionFactory.class);
        when(connectionFactory.isPublisherConfirms()).thenReturn(false);
        when(rabbitTemplate.getConnectionFactory()).thenReturn(connectionFactory);

        assertThatThrownBy(() -> publisher.verifyPublisherConfirmsEnabled())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("publisher-confirm-type=correlated");
    }

    @Test
    void arrancaConPublisherConfirmsActivados() {
        CachingConnectionFactory connectionFactory = org.mockito.Mockito.mock(CachingConnectionFactory.class);
        when(connectionFactory.isPublisherConfirms()).thenReturn(true);
        when(rabbitTemplate.getConnectionFactory()).thenReturn(connectionFactory);

        assertThatCode(() -> publisher.verifyPublisherConfirmsEnabled()).doesNotThrowAnyException();
    }

    @Test
    void noArrancaConUnaConnectionFactoryQueNoSoportaConfirms() {
        ConnectionFactory connectionFactory = org.mockito.Mockito.mock(ConnectionFactory.class);
        when(rabbitTemplate.getConnectionFactory()).thenReturn(connectionFactory);

        assertThatThrownBy(() -> publisher.verifyPublisherConfirmsEnabled())
                .isInstanceOf(IllegalStateException.class);
    }
}
