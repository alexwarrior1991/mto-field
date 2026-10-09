package com.alejandro.mtofield.configuration.maintenance;

import com.alejandro.mtofield.infrastructure.maintenance.RestClientMaintenanceClient;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JCircuitBreakerFactory;
import org.springframework.cloud.client.circuitbreaker.CircuitBreakerFactory;
import org.springframework.cloud.client.circuitbreaker.Customizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.client.AuthorizedClientServiceOAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.OAuth2AuthorizeRequest;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.web.client.RestClient;

/**
 * El cliente HTTP hacia mto-maintenance, activo con {@code app.maintenance.enabled=true} (el valor
 * por defecto). Con {@code false} no se crea nada de esto y queda el {@code NoOpMaintenanceClient}.
 *
 * <p>La autenticacion es una cuenta de servicio ({@code client_credentials}, {@code mto-field-svc})
 * contra el mismo Keycloak: el token representa a este servicio, no a una persona. Se usa el
 * manager basado en {@link OAuth2AuthorizedClientService} y no el ligado a la peticion HTTP porque
 * ninguna de estas llamadas sale de una peticion HTTP: salen de una llamada gRPC, de la cola de
 * trabajo de un dispositivo o del reintento programado. Sin registro de cliente configurado, las
 * llamadas salen sin token y mantenimiento las rechaza con 401: el evento queda FAILED con ese
 * motivo, que es lo que se quiere ver.</p>
 */
@Configuration
@EnableConfigurationProperties(MaintenanceProperties.class)
public class MaintenanceClientConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaintenanceClientConfiguration.class);

    public static final String CIRCUIT_BREAKER = "maintenance";

    /** Principal nominal de la cuenta de servicio: solo identifica la autorizacion en el servicio de clientes. */
    static final String SERVICE_PRINCIPAL = "mto-field";

    @Bean
    @ConditionalOnProperty(prefix = "app.maintenance", name = "enabled", havingValue = "true", matchIfMissing = true)
    public RestClient maintenanceRestClient(RestClient.Builder builder, MaintenanceProperties properties,
                                            ObjectProvider<ClientRegistrationRepository> clientRegistrations,
                                            ObjectProvider<OAuth2AuthorizedClientService> authorizedClients) {
        RestClient.Builder maintenanceBuilder = builder.clone().baseUrl(properties.baseUrl());
        ClientRegistrationRepository registrations = clientRegistrations.getIfAvailable();
        OAuth2AuthorizedClientService clients = authorizedClients.getIfAvailable();
        OAuth2AuthorizedClientManager manager = registrations == null || clients == null ? null
                : new AuthorizedClientServiceOAuth2AuthorizedClientManager(registrations, clients);
        if (manager == null) {
            LOGGER.warn("No OAuth2 client registration is configured: calls to mto-maintenance will go out without a bearer token");
        } else {
            maintenanceBuilder.requestInterceptor((request, body, execution) -> {
                OAuth2AuthorizedClient client = manager.authorize(OAuth2AuthorizeRequest
                        .withClientRegistrationId(properties.clientRegistrationId())
                        .principal(SERVICE_PRINCIPAL)
                        .build());
                if (client != null) {
                    request.getHeaders().setBearerAuth(client.getAccessToken().getTokenValue());
                }
                return execution.execute(request, body);
            });
        }
        return maintenanceBuilder.build();
    }

    /**
     * Umbrales del circuito 'maintenance'. Spring Cloud CircuitBreaker no lee las propiedades
     * resilience4j.* del starter de Boot (no esta en el classpath), asi que se configura aqui con
     * los valores de app.maintenance.circuit-breaker.
     *
     * <p>Un rechazo de mantenimiento (un 4xx de negocio, {@link RestClientMaintenanceClient#isRejection})
     * no cuenta ni como fallo ni como exito: mantenimiento ha respondido, asi que no dice nada de
     * si esta caido.</p>
     */
    @Bean
    @ConditionalOnProperty(prefix = "app.maintenance", name = "enabled", havingValue = "true", matchIfMissing = true)
    public Customizer<Resilience4JCircuitBreakerFactory> maintenanceCircuitBreakerCustomizer(MaintenanceProperties properties) {
        MaintenanceProperties.CircuitBreaker settings = properties.circuitBreaker();
        return factory -> factory.configure(builder -> builder
                .circuitBreakerConfig(CircuitBreakerConfig.custom()
                        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                        .slidingWindowSize(settings.slidingWindowSize())
                        .minimumNumberOfCalls(settings.minimumNumberOfCalls())
                        .failureRateThreshold(settings.failureRateThreshold())
                        .waitDurationInOpenState(settings.waitDurationInOpenState())
                        .automaticTransitionFromOpenToHalfOpenEnabled(true)
                        .ignoreException(RestClientMaintenanceClient::isRejection)
                        .build())
                .timeLimiterConfig(TimeLimiterConfig.custom()
                        .timeoutDuration(settings.timeout())
                        .build()), CIRCUIT_BREAKER);
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.maintenance", name = "enabled", havingValue = "true", matchIfMissing = true)
    public RestClientMaintenanceClient restClientMaintenanceClient(RestClient maintenanceRestClient, CircuitBreakerFactory<?, ?> circuitBreakerFactory) {
        return new RestClientMaintenanceClient(maintenanceRestClient, circuitBreakerFactory.create(CIRCUIT_BREAKER));
    }
}
