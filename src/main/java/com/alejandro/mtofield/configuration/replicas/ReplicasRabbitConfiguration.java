package com.alejandro.mtofield.configuration.replicas;

import com.alejandro.mtofield.application.replicas.ReplicaId;
import com.alejandro.mtofield.application.service.ReplicaBus;
import com.alejandro.mtofield.application.service.ReplicaMessageHandler;
import com.alejandro.mtofield.configuration.metrics.FieldMetrics;
import com.alejandro.mtofield.infrastructure.messaging.replicas.RabbitReplicaBus;
import com.alejandro.mtofield.infrastructure.messaging.replicas.ReplicaEnvelopeCodec;
import com.alejandro.mtofield.infrastructure.messaging.replicas.ReplicaMessageConsumer;
import com.alejandro.mtofield.infrastructure.messaging.replicas.ReplicaRabbitMqNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.amqp.autoconfigure.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.List;

/**
 * El bus de replicas sobre RabbitMQ, entero, y solo con {@code app.rabbitmq.enabled=true}: sin
 * estos beans nadie abre una conexion y la aplicacion arranca sin broker.
 *
 * <p>La topologia: un exchange <b>fanout</b> durable que toda replica redeclara igual, y por
 * replica una cola <b>exclusiva y auto-delete</b> ligada a el. Exclusiva porque solo la consume la
 * conexion que la declaro, y auto-delete porque una replica que muere no debe dejar una cola
 * acumulando lo que ya no va a leer nadie: lo que viaja es efimero. Al reconectar, el
 * {@code RabbitAdmin} la vuelve a declarar y el contenedor la vuelve a consumir.</p>
 *
 * <p>El contenedor de listeners se crea a traves del configurer de Spring Boot, que es quien
 * aplica {@code spring.rabbitmq.listener.simple.*} (auto-startup, reintentos); declararlo a mano
 * los descartaria en silencio. {@code defaultRequeueRejected=false}: un mensaje que falla no vuelve
 * a la cabeza de la cola, se registra y se pasa al siguiente.</p>
 */
@Configuration
@EnableConfigurationProperties(ReplicasRabbitProperties.class)
@ConditionalOnProperty(prefix = "app.rabbitmq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ReplicasRabbitConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicasRabbitConfiguration.class);

    private final ReplicasRabbitProperties properties;

    public ReplicasRabbitConfiguration(ReplicasRabbitProperties properties) {
        this.properties = properties;
    }

    @Bean
    public FanoutExchange replicasExchange() {
        return new FanoutExchange(properties.replicas().exchange(), true, false);
    }

    @Bean
    public Queue replicasQueue(ReplicaId replicaId) {
        return QueueBuilder.nonDurable(ReplicaRabbitMqNames.queueOf(replicaId.value())).exclusive().autoDelete().build();
    }

    @Bean
    public Binding replicasBinding(Queue replicasQueue, FanoutExchange replicasExchange) {
        return BindingBuilder.bind(replicasQueue).to(replicasExchange);
    }

    @Bean
    public ReplicaEnvelopeCodec replicaEnvelopeCodec(JsonMapper jsonMapper) {
        return new ReplicaEnvelopeCodec(jsonMapper);
    }

    @Bean
    public ReplicaBus rabbitReplicaBus(RabbitTemplate rabbitTemplate, ReplicaEnvelopeCodec codec, ReplicaId replicaId, Clock clock,
                                       FieldMetrics metrics) {
        LOGGER.info("Replica bus on: fanout exchange {}, queue {}", properties.replicas().exchange(), ReplicaRabbitMqNames.queueOf(replicaId.value()));
        return new RabbitReplicaBus(rabbitTemplate, codec, properties.replicas().exchange(), replicaId, clock, metrics);
    }

    @Bean
    public ReplicaMessageConsumer replicaMessageConsumer(ReplicaEnvelopeCodec codec, ReplicaId replicaId, List<ReplicaMessageHandler> handlers,
                                                         FieldMetrics metrics) {
        return new ReplicaMessageConsumer(codec, replicaId, handlers, metrics);
    }

    @Bean
    public SimpleMessageListenerContainer replicasListenerContainer(SimpleRabbitListenerContainerFactoryConfigurer configurer,
                                                                    ConnectionFactory connectionFactory, Queue replicasQueue,
                                                                    ReplicaMessageConsumer consumer) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setDefaultRequeueRejected(false);
        SimpleMessageListenerContainer container = factory.createListenerContainer();
        container.setQueueNames(replicasQueue.getName());
        container.setMessageListener(consumer);
        return container;
    }
}
