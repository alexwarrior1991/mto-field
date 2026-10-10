package com.alejandro.mtofield.configuration.outbox;

import com.alejandro.mtofield.configuration.messaging.MessagePayloadSignature;
import com.alejandro.mtofield.configuration.rabbitmq.FieldEventsProperties;
import com.alejandro.mtofield.configuration.security.CurrentUserService;
import com.alejandro.mtofield.infrastructure.messaging.outbox.AsynchronousMessageFactory;
import com.alejandro.mtofield.infrastructure.messaging.outbox.AsynchronousMessageHashService;
import com.alejandro.mtofield.infrastructure.messaging.outbox.MessageContextResolver;
import com.alejandro.mtofield.infrastructure.messaging.outbox.MicrometerOutboxTracing;
import com.alejandro.mtofield.infrastructure.messaging.outbox.NoOpOutboxTracing;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxAdminService;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxDispatchTrigger;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxDomainEventPublisher;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxEndpoint;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxImmediateDispatchListener;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxMessageRepository;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxMetrics;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxMetricsScheduler;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxProperties;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxPublisherScheduler;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxPurgeScheduler;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxPurgeService;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxRabbitPublisher;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxRelayService;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxRetryPolicy;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxService;
import com.alejandro.mtofield.infrastructure.messaging.outbox.OutboxTracing;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Wires the outbox that publishes this service's own events (docs/06-messaging.md).
 *
 * <p>Es la copia del {@code core/outbox} de {@code mto-configuration} que lleva {@code mto-maintenance}:
 * el evento se escribe en
 * {@code outbox_message} dentro de la transaccion de negocio y un relay lo publica despues,
 * esperando la confirmacion del broker, con reintentos, orden por agregado, metricas, purga y el
 * endpoint {@code /actuator/outbox}. Cada pieza es aqui un {@code @Bean} y no un {@code @Component}
 * para que todas hereden la condicion de esta clase: con {@code app.rabbitmq.enabled=false} no
 * existe ninguna, el publicador de eventos es el {@code NoOpDomainEventPublisher} y la aplicacion
 * arranca sin broker, que es lo que necesitan los tests. {@code app.outbox.enabled=false} deja el
 * outbox escribiendo y para solo el relay (los eventos esperan en la tabla).</p>
 *
 * <p>La plantilla de RabbitMQ es la que autoconfigura Spring Boot con
 * {@code spring.rabbitmq.publisher-confirm-type=correlated}, {@code publisher-returns=true} y
 * {@code template.mandatory=true}: sin confirms el relay no arranca
 * ({@link OutboxRabbitPublisher}), porque marcar PUBLISHED lo que el broker no ha aceptado es
 * justo el fallo que el outbox existe para evitar. El bus de replicas usa la misma plantilla y
 * sigue sin esperar confirmacion: su fanout siempre tiene al menos la cola de la propia replica, asi
 * que nada vuelve devuelto, y lo que publica se recupera de la base de todos modos.</p>
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties({OutboxProperties.class, FieldEventsProperties.class})
@ConditionalOnProperty(prefix = "app.rabbitmq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxConfiguration.class);

    @Bean
    public OutboxRetryPolicy outboxRetryPolicy(OutboxProperties outboxProperties) {
        return new OutboxRetryPolicy(outboxProperties);
    }

    @Bean
    public OutboxMetrics outboxMetrics(MeterRegistry meterRegistry) {
        return new OutboxMetrics(meterRegistry);
    }

    /**
     * Con trazabilidad configurada, el contexto de la operacion viaja con el mensaje. Sin ella se
     * usa la implementacion vacia: publicar eventos es el trabajo del outbox, trazarlos es un extra
     * que no puede condicionar su arranque.
     *
     * <p>Se resuelve con {@link ObjectProvider} y no con {@code @ConditionalOnBean}: esa condicion
     * se evalua cuando se registra este bean, antes de que las autoconfiguraciones hayan registrado
     * el {@code Tracer}, asi que en la aplicacion real seria siempre falsa y el outbox trazaria en
     * vacio sin que nadie lo notara.</p>
     */
    @Bean
    public OutboxTracing outboxTracing(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        Tracer availableTracer = tracer.getIfAvailable();
        Propagator availablePropagator = propagator.getIfAvailable();

        if (availableTracer != null && availablePropagator != null) {
            return new MicrometerOutboxTracing(availableTracer, availablePropagator);
        }

        LOGGER.info("No tracer in the context: outbox messages travel without trace context");
        return new NoOpOutboxTracing();
    }

    /**
     * Un solo hilo a proposito: agrupa los despertares y evita que varias pasadas inmediatas se
     * pisen entre si. El planificador sigue siendo la red de seguridad.
     */
    @Bean(destroyMethod = "shutdown")
    @ConditionalOnProperty(prefix = "app.outbox", name = "immediate-dispatch", havingValue = "true", matchIfMissing = true)
    public ExecutorService outboxDispatchExecutor() {
        return Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "outbox-dispatch");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.outbox", name = "immediate-dispatch", havingValue = "true", matchIfMissing = true)
    public OutboxDispatchTrigger outboxDispatchTrigger(ExecutorService outboxDispatchExecutor,
                                                       OutboxPublisherScheduler outboxPublisherScheduler) {
        return new OutboxDispatchTrigger(outboxDispatchExecutor, outboxPublisherScheduler::publishPendingMessages);
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.outbox", name = "immediate-dispatch", havingValue = "true", matchIfMissing = true)
    public OutboxImmediateDispatchListener outboxImmediateDispatchListener(OutboxDispatchTrigger outboxDispatchTrigger) {
        return new OutboxImmediateDispatchListener(outboxDispatchTrigger);
    }

    @Bean
    public OutboxService outboxService(OutboxMessageRepository outboxMessageRepository,
                                       OutboxProperties outboxProperties,
                                       ObjectMapper objectMapper,
                                       OutboxTracing outboxTracing,
                                       ApplicationEventPublisher applicationEventPublisher) {
        return new OutboxService(outboxMessageRepository, outboxProperties, objectMapper, outboxTracing, applicationEventPublisher);
    }

    @Bean
    public OutboxRelayService outboxRelayService(OutboxMessageRepository outboxMessageRepository,
                                                 OutboxProperties outboxProperties,
                                                 OutboxRetryPolicy outboxRetryPolicy) {
        return new OutboxRelayService(outboxMessageRepository, outboxProperties, outboxRetryPolicy);
    }

    @Bean
    public OutboxAdminService outboxAdminService(OutboxMessageRepository outboxMessageRepository) {
        return new OutboxAdminService(outboxMessageRepository);
    }

    @Bean
    public OutboxPurgeService outboxPurgeService(OutboxMessageRepository outboxMessageRepository) {
        return new OutboxPurgeService(outboxMessageRepository);
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
    public OutboxRabbitPublisher outboxRabbitPublisher(RabbitTemplate rabbitTemplate,
                                                       OutboxProperties outboxProperties,
                                                       OutboxTracing outboxTracing,
                                                       MessagePayloadSignature messagePayloadSignature) {
        return new OutboxRabbitPublisher(rabbitTemplate, outboxProperties, outboxTracing, messagePayloadSignature);
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
    public OutboxPublisherScheduler outboxPublisherScheduler(OutboxRelayService outboxRelayService,
                                                             OutboxRabbitPublisher outboxRabbitPublisher,
                                                             OutboxMetrics outboxMetrics) {
        return new OutboxPublisherScheduler(outboxRelayService, outboxRabbitPublisher, outboxMetrics);
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
    public OutboxMetricsScheduler outboxMetricsScheduler(OutboxAdminService outboxAdminService, OutboxMetrics outboxMetrics) {
        return new OutboxMetricsScheduler(outboxAdminService, outboxMetrics);
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.outbox.purge", name = "enabled", havingValue = "true", matchIfMissing = true)
    public OutboxPurgeScheduler outboxPurgeScheduler(OutboxPurgeService outboxPurgeService, OutboxProperties outboxProperties) {
        return new OutboxPurgeScheduler(outboxPurgeService, outboxProperties);
    }

    /** {@code GET /actuator/outbox} (estado) y {@code POST} (redrive de los FAILED); ver SecurityConfiguration. */
    @Bean
    public OutboxEndpoint outboxEndpoint(OutboxAdminService outboxAdminService) {
        return new OutboxEndpoint(outboxAdminService);
    }

    @Bean
    public AsynchronousMessageHashService asynchronousMessageHashService(ObjectMapper objectMapper) {
        return new AsynchronousMessageHashService(objectMapper);
    }

    @Bean
    public MessageContextResolver messageContextResolver(CurrentUserService currentUserService) {
        return new MessageContextResolver(currentUserService);
    }

    @Bean
    public AsynchronousMessageFactory asynchronousMessageFactory(AsynchronousMessageHashService hashService,
                                                                 MessageContextResolver contextResolver,
                                                                 @Value("${spring.application.name:mto-field}") String applicationName) {
        return new AsynchronousMessageFactory(hashService, contextResolver, applicationName);
    }

    /** La puerta por la que los servicios cuentan lo que hacen; su NoOp vive con el resto de impls. */
    @Bean
    public OutboxDomainEventPublisher domainEventPublisher(AsynchronousMessageFactory messageFactory,
                                                           OutboxService outboxService,
                                                           FieldEventsProperties eventsProperties) {
        return new OutboxDomainEventPublisher(messageFactory, outboxService, eventsProperties);
    }
}
