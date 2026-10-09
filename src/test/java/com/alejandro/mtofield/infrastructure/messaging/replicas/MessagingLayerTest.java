package com.alejandro.mtofield.infrastructure.messaging.replicas;

import com.alejandro.mtofield.application.replicas.ReplicaEnvelope;
import com.alejandro.mtofield.application.replicas.ReplicaId;
import com.alejandro.mtofield.application.replicas.ReplicaMessage;
import com.alejandro.mtofield.application.service.ReplicaBus;
import com.alejandro.mtofield.application.service.ReplicaMessageHandler;
import com.alejandro.mtofield.application.service.impl.NoOpReplicaBus;
import com.alejandro.mtofield.configuration.ClockConfiguration;
import com.alejandro.mtofield.configuration.grpc.FieldProperties;
import com.alejandro.mtofield.configuration.metrics.FieldMetrics;
import com.alejandro.mtofield.configuration.replicas.ReplicasConfiguration;
import com.alejandro.mtofield.configuration.replicas.ReplicasRabbitConfiguration;
import com.alejandro.mtofield.configuration.replicas.ReplicasRabbitProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * El bus de replicas sin broker: el sobre como JSON, la publicacion sobre un RabbitTemplate
 * simulado, el consumidor y la topologia con ApplicationContextRunner. Lo que necesita el broker
 * real es RabbitReplicaBusIT.
 */
class MessagingLayerTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-09T22:00:00Z"), ZoneOffset.UTC);
    private static final UUID POSSESSION = UUID.fromString("0d5bb548-8756-415e-a06d-ae8652e202ab");
    private static final UUID SHIFT = UUID.fromString("f63106a9-7b29-4691-a317-076315eb66a3");

    private final JsonMapper jsonMapper = JsonMapper.builder().build();
    private final ReplicaEnvelopeCodec codec = new ReplicaEnvelopeCodec(jsonMapper);
    private final FieldMetrics metrics = new FieldMetrics(new SimpleMeterRegistry());

    private static ReplicaMessage.Device device() {
        return new ReplicaMessage.Device("dev-1", SHIFT, POSSESSION, "EQ-NORTE", Instant.parse("2026-10-09T21:00:00Z"),
                Instant.parse("2026-10-09T21:59:50Z"), "34.271", 80, -70, true, 12);
    }

    @Nested
    @DisplayName("El sobre")
    class Envelope {

        @Test
        void everyKindOfMessageRoundTripsThroughJson() {
            for (ReplicaMessage message : List.of(new ReplicaMessage.Command(POSSESSION, 7), device(),
                    new ReplicaMessage.Closed(POSSESSION), new ReplicaMessage.Stopped())) {
                ReplicaEnvelope sent = ReplicaEnvelope.of("replica-a", CLOCK.instant(), message);
                ReplicaEnvelope received = codec.decode(codec.encode(sent));

                assertThat(received.replicaId()).isEqualTo("replica-a");
                assertThat(received.sentAt()).isEqualTo(CLOCK.instant());
                assertThat(received.kind()).isEqualTo(sent.kind());
                assertThat(received.message()).contains(message);
            }
        }

        @Test
        void theJsonCarriesOnlyTheFieldOfItsKind() {
            String json = new String(codec.encode(ReplicaEnvelope.of("replica-a", CLOCK.instant(), new ReplicaMessage.Command(POSSESSION, 7))),
                    StandardCharsets.UTF_8);

            assertThat(json).contains("\"kind\":\"COMMAND_COMMITTED\"").contains("\"sequence\":7").contains("\"sentAt\":\"2026-10-09T22:00:00Z\"")
                    .doesNotContain("\"device\"").doesNotContain("\"closed\"");
        }

        /** Un kind nuevo de una version mas reciente no es un error: se ignora, y los campos de mas tambien. */
        @Test
        void anUnknownKindOrAnExtraFieldIsIgnoredNotRejected() {
            ReplicaEnvelope newer = codec.decode(("{\"replicaId\":\"replica-b\",\"sentAt\":\"2026-10-09T22:00:00Z\",\"kind\":\"WEATHER\","
                    + "\"weather\":{\"rain\":true},\"schemaVersion\":2}").getBytes(StandardCharsets.UTF_8));
            assertThat(newer.message()).isEmpty();

            ReplicaEnvelope known = codec.decode(("{\"replicaId\":\"replica-b\",\"kind\":\"POSSESSION_CLOSED\","
                    + "\"closed\":{\"possessionId\":\"" + POSSESSION + "\",\"reason\":\"extra\"}}").getBytes(StandardCharsets.UTF_8));
            assertThat(known.message()).contains(new ReplicaMessage.Closed(POSSESSION));
        }

        @Test
        void aBodyThatIsNotAnEnvelopeIsRefused() {
            assertThatThrownBy(() -> codec.decode("not json".getBytes(StandardCharsets.UTF_8))).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> codec.decode("{\"kind\":\"REPLICA_STOPPED\"}".getBytes(StandardCharsets.UTF_8)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("replicaId");
        }

        @Test
        void aReplicaIdIsTheConfiguredOneOrTheHostWithARandomSuffix() {
            assertThat(ReplicaId.of(" field-1 ").value()).isEqualTo("field-1");
            ReplicaId generated = ReplicaId.of("");
            assertThat(generated.value()).matches(".+-[0-9a-f]{6}");
            assertThat(ReplicaId.of(null).value()).isNotEqualTo(generated.value());
            assertThatThrownBy(() -> new ReplicaId(" ")).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("Publicar")
    class Publishing {

        private final RabbitTemplate template = mock(RabbitTemplate.class);
        private final RabbitReplicaBus bus = new RabbitReplicaBus(template, codec, "mto.field.replicas.exchange", new ReplicaId("replica-a"), CLOCK, metrics);

        @Test
        void aMessageGoesToTheFanoutExchangeAsTransientJsonWithTheReplicaHeader() {
            bus.publish(device());

            ArgumentCaptor<Message> sent = ArgumentCaptor.forClass(Message.class);
            verify(template).send(eq("mto.field.replicas.exchange"), eq(""), sent.capture());
            Message message = sent.getValue();
            assertThat(message.getMessageProperties().getContentType()).isEqualTo("application/json");
            assertThat(message.getMessageProperties().getDeliveryMode()).isEqualTo(org.springframework.amqp.core.MessageDeliveryMode.NON_PERSISTENT);
            assertThat(message.getMessageProperties().getHeaders()).containsEntry(ReplicaRabbitMqNames.HEADER_REPLICA, "replica-a")
                    .containsEntry("kind", ReplicaEnvelope.KIND_DEVICE_STATE);
            ReplicaEnvelope envelope = codec.decode(message.getBody());
            assertThat(envelope.replicaId()).isEqualTo("replica-a");
            assertThat(envelope.sentAt()).isEqualTo(CLOCK.instant());
            assertThat(envelope.message()).contains(device());
            assertThat(metrics.registry().get(FieldMetrics.REPLICA_MESSAGES).tags("direction", "out", "kind", "DEVICE_STATE", "outcome", "sent")
                    .counter().count()).isEqualTo(1.0);
        }

        /** El negocio nunca espera al broker: un fallo se registra y se cuenta, y el tic hace el resto. */
        @Test
        void aBrokerThatIsDownDoesNotFailTheCaller() {
            doThrow(new AmqpConnectException(new java.net.ConnectException("refused"))).when(template).send(any(), any(), any(Message.class));

            bus.publish(new ReplicaMessage.Command(POSSESSION, 3));

            assertThat(metrics.registry().get(FieldMetrics.REPLICA_MESSAGES).tags("direction", "out", "kind", "COMMAND_COMMITTED", "outcome", "failed")
                    .counter().count()).isEqualTo(1.0);
        }
    }

    @Nested
    @DisplayName("Recibir")
    class Receiving {

        private final List<String> seen = new ArrayList<>();
        private final ReplicaMessageHandler recording = (from, message) -> seen.add(from + ":" + message.getClass().getSimpleName());
        private final ReplicaMessageHandler failing = (from, message) -> {
            throw new IllegalStateException("boom");
        };
        private final ReplicaMessageConsumer consumer = new ReplicaMessageConsumer(codec, new ReplicaId("replica-a"), List.of(failing, recording), metrics);

        private Message messageFrom(String replica, ReplicaMessage message) {
            return MessageBuilder.withBody(codec.encode(ReplicaEnvelope.of(replica, CLOCK.instant(), message))).build();
        }

        @Test
        void aMessageOfAnotherReplicaReachesEveryHandlerEvenIfOneFails() {
            consumer.onMessage(messageFrom("replica-b", new ReplicaMessage.Closed(POSSESSION)));

            assertThat(seen).containsExactly("replica-b:Closed");
            assertThat(metrics.registry().get(FieldMetrics.REPLICA_MESSAGES).tags("direction", "in", "kind", "POSSESSION_CLOSED", "outcome", "handled")
                    .counter().count()).isEqualTo(1.0);
        }

        /** El fanout devuelve lo propio a quien lo publico. */
        @Test
        void theOwnMessagesAreIgnored() {
            consumer.onMessage(messageFrom("replica-a", new ReplicaMessage.Command(POSSESSION, 1)));

            assertThat(seen).isEmpty();
            assertThat(metrics.registry().get(FieldMetrics.REPLICA_MESSAGES).tags("direction", "in", "kind", "COMMAND_COMMITTED", "outcome", "own")
                    .counter().count()).isEqualTo(1.0);
        }

        @Test
        void aBodyThatIsNotAnEnvelopeOrAnUnknownKindIsDroppedWithoutThrowing() {
            consumer.onMessage(MessageBuilder.withBody("garbage".getBytes(StandardCharsets.UTF_8)).build());
            consumer.onMessage(MessageBuilder.withBody("{\"replicaId\":\"replica-b\",\"kind\":\"WEATHER\"}".getBytes(StandardCharsets.UTF_8)).build());

            assertThat(seen).isEmpty();
            assertThat(metrics.registry().get(FieldMetrics.REPLICA_MESSAGES).tags("direction", "in", "outcome", "dropped").counters()).hasSize(2);
        }
    }

    @Nested
    @DisplayName("Topologia y cableado")
    class Wiring {

        private final ApplicationContextRunner runner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RabbitAutoConfiguration.class, JacksonAutoConfiguration.class))
                .withUserConfiguration(Support.class, ClockConfiguration.class, ReplicasConfiguration.class, ReplicasRabbitConfiguration.class)
                // Sin esto el contenedor arranca y se pone a reintentar la conexion contra un broker
                // que en un test no existe.
                .withPropertyValues("spring.rabbitmq.listener.simple.auto-startup=false",
                        "app.field.outbound-queue-capacity=256", "app.field.held-capacity=256", "app.field.work-queue-capacity=64",
                        "app.field.work-queue-idle-timeout=5m", "app.field.catch-up-page-size=200", "app.field.catch-up-put-timeout=30s",
                        "app.field.liveness.stale-after=30s", "app.field.liveness.disconnected-after=60s", "app.field.board.tick=5s",
                        "app.field.token-expiry.enabled=false", "app.field.token-expiry.sweep=30s",
                        "app.field.team-binding.enabled=false", "app.field.team-binding.claim=groups",
                        "app.field.replicas.id=replica-test", "app.field.replicas.catch-up=2s", "app.field.replicas.remote-ttl=90s");

        @Test
        void withTheBusOnThereIsAFanoutExchangeAndAnExclusiveAutoDeleteQueuePerReplica() {
            runner.withPropertyValues("app.rabbitmq.enabled=true").run(context -> {
                assertThat(context).hasSingleBean(FanoutExchange.class);
                FanoutExchange exchange = context.getBean(FanoutExchange.class);
                assertThat(exchange.getName()).isEqualTo(ReplicasRabbitProperties.DEFAULT_EXCHANGE);
                assertThat(exchange.isDurable()).isTrue();
                assertThat(exchange.isAutoDelete()).isFalse();

                Queue queue = context.getBean(Queue.class);
                assertThat(queue.getName()).isEqualTo("mto.field.replicas.replica-test");
                assertThat(queue.isExclusive()).isTrue();
                assertThat(queue.isAutoDelete()).isTrue();
                assertThat(queue.isDurable()).isFalse();

                Binding binding = context.getBean(Binding.class);
                assertThat(binding.getExchange()).isEqualTo(exchange.getName());
                assertThat(binding.getDestination()).isEqualTo(queue.getName());
                assertThat(binding.getDestinationType()).isEqualTo(Binding.DestinationType.QUEUE);

                assertThat(context.getBean(ReplicaBus.class)).isInstanceOf(RabbitReplicaBus.class);
                assertThat(((RabbitReplicaBus) context.getBean(ReplicaBus.class)).exchange()).isEqualTo(exchange.getName());
                SimpleMessageListenerContainer container = context.getBean(SimpleMessageListenerContainer.class);
                assertThat(container.getQueueNames()).containsExactly(queue.getName());
                assertThat(container.isAutoStartup()).isFalse();
                assertThat(container.getMessageListener()).isInstanceOf(ReplicaMessageConsumer.class);
            });
        }

        @Test
        void theExchangeCanBeRenamedAndABlankNameFallsBackToTheContract() {
            runner.withPropertyValues("app.rabbitmq.enabled=true", "app.rabbitmq.replicas.exchange=mto.field.test.exchange")
                    .run(context -> assertThat(context.getBean(FanoutExchange.class).getName()).isEqualTo("mto.field.test.exchange"));
            runner.withPropertyValues("app.rabbitmq.enabled=true", "app.rabbitmq.replicas.exchange=")
                    .run(context -> assertThat(context.getBean(FanoutExchange.class).getName()).isEqualTo(ReplicasRabbitProperties.DEFAULT_EXCHANGE));
        }

        @Test
        void withTheBusOffNothingOfRabbitIsDeclaredAndTheBusIsANoOp() {
            runner.withPropertyValues("app.rabbitmq.enabled=false").run(context -> {
                assertThat(context).doesNotHaveBean(FanoutExchange.class).doesNotHaveBean(Queue.class)
                        .doesNotHaveBean(SimpleMessageListenerContainer.class).doesNotHaveBean(ReplicaMessageConsumer.class);
                assertThat(context.getBean(ReplicaBus.class)).isInstanceOf(NoOpReplicaBus.class);
                assertThat(context.getBean(ReplicaId.class).value()).isEqualTo("replica-test");
            });
        }

        @Configuration(proxyBeanMethods = false)
        @EnableConfigurationProperties(FieldProperties.class)
        static class Support {

            @Bean
            FieldMetrics fieldMetrics() {
                return new FieldMetrics(new SimpleMeterRegistry());
            }
        }
    }
}
