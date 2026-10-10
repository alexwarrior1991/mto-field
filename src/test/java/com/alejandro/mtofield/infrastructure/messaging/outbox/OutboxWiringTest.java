package com.alejandro.mtofield.infrastructure.messaging.outbox;

import com.alejandro.mtofield.application.service.DomainEventPublisher;
import com.alejandro.mtofield.configuration.messaging.MessagePayloadSignature;
import com.alejandro.mtofield.configuration.messaging.MessageSignatureMode;
import com.alejandro.mtofield.configuration.messaging.MessageSignatureProperties;
import com.alejandro.mtofield.configuration.outbox.OutboxConfiguration;
import com.alejandro.mtofield.configuration.security.CurrentUserService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Cableado real de Spring para el outbox: aqui se ve lo que un test con mocks sueltos no ve, como
 * un {@code @PostConstruct} que impide arrancar o una condicion mal puesta. Cada pieza es un
 * {@code @Bean} de {@link OutboxConfiguration} y no un componente escaneado, para que todas hereden
 * la condicion {@code app.rabbitmq.enabled}; lo que se fija aqui es que con ella encendida esta
 * todo, y con ella apagada no queda nada.
 */
class OutboxWiringTest {

    private ApplicationContextRunner runner(boolean publisherConfirms) {
        return new ApplicationContextRunner()
                .withUserConfiguration(RabbitStubConfiguration.class, OutboxConfiguration.class)
                // El acceso a base de datos no entra aqui: lo que se comprueba es el cableado.
                .withBean(OutboxMessageRepository.class, () -> mock(OutboxMessageRepository.class))
                // En produccion los aportan Boot y actuator; aqui basta con lo minimo.
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(CurrentUserService.class, CurrentUserService::new)
                .withBean(MessagePayloadSignature.class, () -> new MessagePayloadSignature(
                        new MessageSignatureProperties(null, MessageSignatureMode.OPTIONAL)))
                .withPropertyValues("mto.test.publisher-confirms=" + publisherConfirms);
    }

    @Test
    void elRelayArrancaConPublisherConfirmsActivados() {
        runner(true).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(OutboxRabbitPublisher.class);
            assertThat(context).hasSingleBean(OutboxPublisherScheduler.class);
            assertThat(context).hasSingleBean(OutboxRetryPolicy.class);
            assertThat(context).hasSingleBean(OutboxMetrics.class);
            assertThat(context).hasSingleBean(OutboxTracing.class);
            assertThat(context).hasSingleBean(OutboxService.class);
            assertThat(context).hasSingleBean(OutboxEndpoint.class);
            assertThat(context).hasSingleBean(OutboxPurgeScheduler.class);
            assertThat(context).hasSingleBean(OutboxMetricsScheduler.class);
            assertThat(context).hasSingleBean(OutboxImmediateDispatchListener.class);
            assertThat(context).getBean(DomainEventPublisher.class).isInstanceOf(OutboxDomainEventPublisher.class);
            assertThat(context.getBean(DomainEventPublisher.class).isEnabled()).isTrue();
        });
    }

    @Test
    void elRelayNoArrancaSiFaltanLosPublisherConfirms() {
        // Degradar en silencio aqui significa volver a marcar PUBLISHED mensajes que
        // el broker no ha aceptado, que es justo el fallo que el outbox existe para evitar.
        runner(false).run(context -> assertThat(context)
                .getFailure()
                .hasMessageContaining("outboxRabbitPublisher")
                .rootCause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("publisher-confirm-type=correlated"));
    }

    @Test
    void sinTrazabilidadConfiguradaElOutboxUsaLaImplementacionVacia() {
        // Publicar eventos es el trabajo del outbox; trazarlos es un extra que no puede
        // condicionar su arranque.
        runner(true).run(context -> assertThat(context)
                .getBean(OutboxTracing.class)
                .isInstanceOf(NoOpOutboxTracing.class));
    }

    @Test
    void conTracerYPropagatorElOutboxPropagaLaTraza() {
        runner(true)
                .withBean(Tracer.class, () -> mock(Tracer.class))
                .withBean(Propagator.class, () -> mock(Propagator.class))
                .run(context -> assertThat(context)
                        .getBean(OutboxTracing.class)
                        .isInstanceOf(MicrometerOutboxTracing.class));
    }

    /** Sin broker no hay outbox: ni relay, ni tabla que escribir, ni endpoint; el publicador es el NoOp de los servicios. */
    @Test
    void conElBrokerApagadoNoQuedaNingunaPiezaDelOutbox() {
        runner(true)
                .withPropertyValues("app.rabbitmq.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(OutboxService.class);
                    assertThat(context).doesNotHaveBean(OutboxRabbitPublisher.class);
                    assertThat(context).doesNotHaveBean(OutboxEndpoint.class);
                    assertThat(context).doesNotHaveBean(OutboxDomainEventPublisher.class);
                    assertThat(context).doesNotHaveBean(OutboxProperties.class);
                });
    }

    /** app.outbox.enabled=false para el relay y deja el outbox escribiendo: los eventos esperan en la tabla. */
    @Test
    void conElRelayApagadoElOutboxSigueEscribiendo() {
        runner(true)
                .withPropertyValues("app.outbox.enabled=false", "app.outbox.immediate-dispatch=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(OutboxService.class);
                    assertThat(context).hasSingleBean(OutboxDomainEventPublisher.class);
                    assertThat(context).doesNotHaveBean(OutboxRabbitPublisher.class);
                    assertThat(context).doesNotHaveBean(OutboxPublisherScheduler.class);
                    assertThat(context).doesNotHaveBean(OutboxDispatchTrigger.class);
                });
    }

    @Test
    void lasPropiedadesDelOutboxTraenValoresPorDefectoRazonables() {
        runner(true).run(context -> {
            OutboxProperties properties = context.getBean(OutboxProperties.class);

            assertThat(properties.getMaxAttempts()).isEqualTo(20);
            assertThat(properties.getInitialRetryDelay()).hasSeconds(5);
            assertThat(properties.getMaxRetryDelay()).hasMinutes(5);
            assertThat(properties.getBatchSize()).isEqualTo(50);
            assertThat(properties.getConfirmTimeout()).hasSeconds(10);
            assertThat(properties.getClaimVisibilityTimeout())
                    .as("la visibilidad debe superar con holgura la espera de confirmacion de todo un lote")
                    .isGreaterThan(properties.getConfirmTimeout());
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class RabbitStubConfiguration {

        @Bean
        @ConditionalOnMissingBean
        RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
            RabbitTemplate template = mock(RabbitTemplate.class);
            when(template.getConnectionFactory()).thenReturn(connectionFactory);
            return template;
        }

        @Bean
        ConnectionFactory connectionFactory(@Value("${mto.test.publisher-confirms}") boolean publisherConfirms) {
            CachingConnectionFactory connectionFactory = mock(CachingConnectionFactory.class);
            when(connectionFactory.isPublisherConfirms()).thenReturn(publisherConfirms);
            return connectionFactory;
        }
    }
}
