package com.alejandro.mtofield.infrastructure.messaging.replicas;

import com.alejandro.mtofield.application.replicas.ReplicaEnvelope;
import com.alejandro.mtofield.application.replicas.ReplicaMessage;
import com.alejandro.mtofield.application.service.ReplicaBus;
import com.alejandro.mtofield.application.service.ReplicaMessageHandler;
import com.alejandro.mtofield.configuration.ClockConfiguration;
import com.alejandro.mtofield.configuration.grpc.FieldProperties;
import com.alejandro.mtofield.configuration.metrics.FieldMetrics;
import com.alejandro.mtofield.configuration.replicas.ReplicasConfiguration;
import com.alejandro.mtofield.configuration.replicas.ReplicasRabbitConfiguration;
import com.alejandro.mtofield.support.RabbitMqTestBroker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * El bus sobre un RabbitMQ de verdad ({@link RabbitMqTestBroker}): dos contextos con la
 * configuracion real del bus (el exchange fanout, la cola exclusiva de cada uno, el contenedor
 * de listeners), cada uno con su conexion. Lo que uno publica lo recibe el otro y no el mismo,
 * y un mensaje que no es un sobre no para la cola.
 */
class RabbitReplicaBusIT {

    private static final UUID POSSESSION = UUID.randomUUID();
    private static Map<String, Object> broker;

    @BeforeAll
    static void broker() {
        broker = RabbitMqTestBroker.properties();
    }

    private ApplicationContextRunner replica(String id) {
        ApplicationContextRunner runner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RabbitAutoConfiguration.class, JacksonAutoConfiguration.class))
                .withUserConfiguration(Support.class, ClockConfiguration.class, ReplicasConfiguration.class, ReplicasRabbitConfiguration.class)
                .withPropertyValues("app.rabbitmq.enabled=true", "app.rabbitmq.replicas.exchange=mto.field.replicas.it.exchange",
                        "app.field.outbound-queue-capacity=256", "app.field.held-capacity=256", "app.field.work-queue-capacity=64",
                        "app.field.work-queue-idle-timeout=5m", "app.field.catch-up-page-size=200", "app.field.catch-up-put-timeout=30s",
                        "app.field.liveness.stale-after=30s", "app.field.liveness.disconnected-after=60s", "app.field.board.tick=5s",
                        "app.field.token-expiry.enabled=false", "app.field.token-expiry.sweep=30s",
                        "app.field.team-binding.enabled=false", "app.field.team-binding.claim=groups",
                        "app.field.replicas.id=" + id, "app.field.replicas.catch-up=2s", "app.field.replicas.remote-ttl=90s");
        for (Map.Entry<String, Object> property : broker.entrySet()) {
            runner = runner.withPropertyValues(property.getKey() + "=" + property.getValue());
        }
        return runner;
    }

    @Test
    void whatOneReplicaPublishesTheOtherReceivesAndItselfDoesNot() {
        replica("it-a").run(contextA -> replica("it-b").run(contextB -> {
            Support.Recording onA = contextA.getBean(Support.Recording.class);
            Support.Recording onB = contextB.getBean(Support.Recording.class);
            ReplicaBus busA = contextA.getBean(ReplicaBus.class);
            ReplicaBus busB = contextB.getBean(ReplicaBus.class);
            assertThat(busA).isInstanceOf(RabbitReplicaBus.class);

            busA.publish(new ReplicaMessage.Command(POSSESSION, 7));
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(onB.received).containsExactly("it-a:" + new ReplicaMessage.Command(POSSESSION, 7)));
            assertThat(onA.received).as("the fanout gives the message back to its publisher, who ignores it").isEmpty();

            // Un cuerpo que no es un sobre no para la cola: lo siguiente sigue llegando.
            RabbitTemplate template = contextA.getBean(RabbitTemplate.class);
            template.send("mto.field.replicas.it.exchange", "", MessageBuilder.withBody("garbage".getBytes(StandardCharsets.UTF_8)).build());
            busB.publish(new ReplicaMessage.Closed(POSSESSION));
            busA.publish(new ReplicaMessage.Stopped());
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(onA.received).containsExactly("it-b:" + new ReplicaMessage.Closed(POSSESSION));
                assertThat(onB.received).containsExactly("it-a:" + new ReplicaMessage.Command(POSSESSION, 7), "it-a:" + new ReplicaMessage.Stopped());
            });
            assertThat(contextB.getBean(FieldMetrics.class).registry().get(FieldMetrics.REPLICA_MESSAGES)
                    .tags("direction", "in", "outcome", "dropped").counter().count()).isEqualTo(1.0);
        }));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(FieldProperties.class)
    static class Support {

        static final class Recording implements ReplicaMessageHandler {
            final List<String> received = new CopyOnWriteArrayList<>();

            @Override
            public void onReplicaMessage(String fromReplica, ReplicaMessage message) {
                received.add(fromReplica + ":" + message);
            }
        }

        @Bean
        FieldMetrics fieldMetrics() {
            return new FieldMetrics(new SimpleMeterRegistry());
        }

        @Bean
        Recording recording() {
            return new Recording();
        }
    }
}
