package com.alejandro.mtofield.infrastructure.maintenance;

import com.alejandro.mtofield.application.dto.CompleteTaskCommand;
import com.alejandro.mtofield.application.exception.MaintenanceRejectedException;
import com.alejandro.mtofield.application.exception.MaintenanceUnavailableException;
import com.alejandro.mtofield.configuration.maintenance.MaintenanceClientConfiguration;
import com.alejandro.mtofield.configuration.maintenance.MaintenanceProperties;
import com.alejandro.mtofield.domain.model.ShiftSnapshot;
import com.alejandro.mtofield.domain.model.TaskSnapshot;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.security.oauth2.client.autoconfigure.OAuth2ClientAutoConfiguration;
import org.springframework.boot.security.oauth2.client.autoconfigure.servlet.OAuth2ClientWebSecurityAutoConfiguration;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JCircuitBreakerFactory;
import org.springframework.cloud.client.circuitbreaker.CircuitBreakerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.test.json.JsonCompareMode;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClient;

import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * El cliente de mto-maintenance contra un servidor simulado: las rutas y los cuerpos del contrato
 * de mantenimiento, lo que se lee de sus respuestas, y que un rechazo suyo y una caida se
 * distinguen tambien en el circuito.
 */
class MaintenanceClientTest {

    private static final String BASE = "http://maintenance";

    private MockRestServiceServer server;
    private RestClientMaintenanceClient client;
    private CircuitBreakerRegistry registry;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        // El circuito de produccion, tal como lo configura MaintenanceClientConfiguration, con
        // umbrales bajos: dos fallos seguidos lo abren.
        registry = CircuitBreakerRegistry.ofDefaults();
        Resilience4JCircuitBreakerFactory factory = new Resilience4JCircuitBreakerFactory(registry, TimeLimiterRegistry.ofDefaults(), null);
        MaintenanceProperties properties = new MaintenanceProperties(true, BASE, "mto-services",
                new MaintenanceProperties.CircuitBreaker(2, 2, 50, Duration.ofSeconds(30), Duration.ofSeconds(5)), null);
        new MaintenanceClientConfiguration().maintenanceCircuitBreakerCustomizer(properties).customize(factory);
        client = new RestClientMaintenanceClient(builder.baseUrl(BASE).build(), factory.create(MaintenanceClientConfiguration.CIRCUIT_BREAKER));
    }

    /** El estado del circuito que creo la primera llamada; pedirlo antes lo crearia con la configuracion por defecto. */
    private CircuitBreaker.State circuit() {
        return registry.find(MaintenanceClientConfiguration.CIRCUIT_BREAKER).orElseThrow().getState();
    }

    private static String taskJson(UUID taskId, UUID orderId, String status, UUID shiftId) {
        return """
                {"id":"%s","orderId":"%s","sequence":1,"description":"Check the cantilever","status":"%s","assignedUser":"campo.tecnico1",
                 "asset":{"id":"%s","code":"P-1"},"shiftId":%s,"startedAt":null,"completedAt":null,"photoRefs":[],"taskTypeCodes":["RG-01"],
                 "checkItems":[],"version":2,"audit":{"createdBy":"x"}}"""
                .formatted(taskId, orderId, status, UUID.randomUUID(), shiftId == null ? "null" : "\"" + shiftId + "\"");
    }

    @Test
    void aShiftIsReadAsMaintenanceHasItAndOneItDoesNotKnowIsEmpty() {
        UUID shiftId = UUID.randomUUID();
        UUID unknown = UUID.randomUUID();
        server.expect(requestTo(BASE + "/api/v1/maintenance/shifts/" + shiftId))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("""
                        {"id":"%s","code":"SH-000012","shiftDate":"2026-10-09","team":{"id":"%s","code":"EQ-NORTE","name":"Equipo Norte","baseName":"Base"},
                         "baseName":"Base","possessionType":"FULL","plannedStart":"2026-10-09T22:00:00Z","plannedEnd":"2026-10-10T05:00:00Z",
                         "trackIds":[7,8],"startKp":34.100,"endKp":36.250,"status":"IN_PROGRESS","version":3,"somethingNew":true}"""
                        .formatted(shiftId, UUID.randomUUID()), MediaType.APPLICATION_JSON));
        server.expect(requestTo(BASE + "/api/v1/maintenance/shifts/" + unknown))
                .andRespond(withStatus(HttpStatus.NOT_FOUND).contentType(MediaType.APPLICATION_JSON)
                        .body("{\"status\":404,\"errorCode\":\"SHF-404\",\"message\":\"Maintenance shift with id ... was not found\"}"));

        ShiftSnapshot shift = client.findShift(shiftId).orElseThrow();

        assertThat(shift).isEqualTo(new ShiftSnapshot(shiftId, "SH-000012", LocalDate.of(2026, 10, 9), "IN_PROGRESS", "EQ-NORTE", "Equipo Norte",
                Instant.parse("2026-10-09T22:00:00Z"), Instant.parse("2026-10-10T05:00:00Z"), List.of(7L, 8L)));
        assertThat(client.findShift(unknown)).isEmpty();
        assertThat(client.isEnabled()).isTrue();
        server.verify();
    }

    @Test
    void aTaskIsReadFromItsOrderAndAnUnknownOneIsEmpty() {
        UUID orderId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        UUID shiftId = UUID.randomUUID();
        server.expect(requestTo(BASE + "/api/v1/maintenance/orders/" + orderId + "/tasks/" + taskId))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(taskJson(taskId, orderId, "COMPLETED", shiftId), MediaType.APPLICATION_JSON));
        server.expect(requestTo(BASE + "/api/v1/maintenance/orders/" + orderId + "/tasks/" + taskId))
                .andRespond(withStatus(HttpStatus.NOT_FOUND).contentType(MediaType.APPLICATION_JSON)
                        .body("{\"status\":404,\"errorCode\":\"TSK-404\",\"message\":\"Maintenance task with id ... was not found\"}"));

        TaskSnapshot task = client.findTask(orderId, taskId).orElseThrow();

        assertThat(task).isEqualTo(new TaskSnapshot(taskId, orderId, "COMPLETED", shiftId, "campo.tecnico1"));
        assertThat(task.isCompleted()).isTrue();
        assertThat(client.findTask(orderId, taskId)).isEmpty();
        server.verify();
    }

    @Test
    void aStartCarriesTheShiftAndThePersonOfTheDevice() {
        UUID orderId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        UUID shiftId = UUID.randomUUID();
        server.expect(requestTo(BASE + "/api/v1/maintenance/orders/" + orderId + "/tasks/" + taskId + "/start"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().json("{\"shiftId\":\"%s\",\"assignedUser\":\"campo.tecnico1\"}".formatted(shiftId), JsonCompareMode.STRICT))
                .andRespond(withSuccess(taskJson(taskId, orderId, "IN_PROGRESS", shiftId), MediaType.APPLICATION_JSON));
        server.expect(requestTo(BASE + "/api/v1/maintenance/orders/" + orderId + "/tasks/" + taskId + "/start"))
                .andExpect(content().json("{\"shiftId\":\"%s\"}".formatted(shiftId), JsonCompareMode.STRICT))
                .andRespond(withSuccess(taskJson(taskId, orderId, "IN_PROGRESS", shiftId), MediaType.APPLICATION_JSON));

        TaskSnapshot task = client.startTask(orderId, taskId, shiftId, "campo.tecnico1");
        TaskSnapshot anonymous = client.startTask(orderId, taskId, shiftId, " ");

        assertThat(task.isInProgress()).isTrue();
        assertThat(anonymous.shiftId()).isEqualTo(shiftId);
        server.verify();
    }

    /** El cuerpo es el CompleteTaskRequest de mantenimiento con lo que el contrato de campo trae; lo vacio no viaja. */
    @Test
    void aCompletionCarriesTheTypesTheNotesTheInlineDefectsAndThePhotosAndLeavesOutWhatIsEmpty() {
        UUID orderId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        UUID shiftId = UUID.randomUUID();
        server.expect(requestTo(BASE + "/api/v1/maintenance/orders/" + orderId + "/tasks/" + taskId + "/complete"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().json("""
                        {"shiftId":"%s","taskTypeCodes":["RG-01","RP-03"],"notes":"tightened","workComplete":true,
                         "inlineDefects":[{"severity":"HIGH","description":"worn contact wire","correctionType":"replacement","partsReplaced":"CW 107"}],
                         "photoRefs":["photo-1"]}""".formatted(shiftId), JsonCompareMode.STRICT))
                .andRespond(withSuccess(taskJson(taskId, orderId, "COMPLETED", shiftId), MediaType.APPLICATION_JSON));
        server.expect(requestTo(BASE + "/api/v1/maintenance/orders/" + orderId + "/tasks/" + taskId + "/complete"))
                .andExpect(content().json("{\"shiftId\":\"%s\",\"workComplete\":false}".formatted(shiftId), JsonCompareMode.STRICT))
                .andExpect(jsonPath("$.taskTypeCodes").doesNotExist())
                .andRespond(withSuccess(taskJson(taskId, orderId, "COMPLETED", shiftId), MediaType.APPLICATION_JSON));

        TaskSnapshot task = client.completeTask(orderId, taskId, new CompleteTaskCommand(shiftId, List.of("RG-01", "RP-03"), "tightened", true,
                List.of(new CompleteTaskCommand.InlineDefect("HIGH", "worn contact wire", null, "replacement", "CW 107")), List.of("photo-1")));
        TaskSnapshot bare = client.completeTask(orderId, taskId, new CompleteTaskCommand(shiftId, List.of(), "", false, List.of(), List.of()));

        assertThat(task.isCompleted()).isTrue();
        assertThat(bare.isCompleted()).isTrue();
        server.verify();
    }

    @Test
    void aBusinessRejectionCarriesTheCodeOfMaintenanceAndDoesNotOpenTheCircuit() {
        UUID orderId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        UUID shiftId = UUID.randomUUID();
        String start = BASE + "/api/v1/maintenance/orders/" + orderId + "/tasks/" + taskId + "/start";
        server.expect(requestTo(start)).andRespond(withStatus(HttpStatus.CONFLICT).contentType(MediaType.APPLICATION_JSON)
                .body("{\"status\":409,\"errorCode\":\"TRN-001\",\"message\":\"Task 1 is IN_PROGRESS and cannot be started\",\"path\":\"/x\",\"method\":\"POST\"}"));
        server.expect(requestTo(start)).andRespond(withStatus(HttpStatus.CONFLICT).contentType(MediaType.APPLICATION_JSON)
                .body("{\"status\":409,\"errorCode\":\"SHF-001\",\"message\":\"Shift SH-000003 is not IN_PROGRESS\"}"));
        server.expect(requestTo(start)).andRespond(withStatus(HttpStatus.NOT_FOUND).contentType(MediaType.TEXT_HTML).body("<html>not here</html>"));
        server.expect(requestTo(start)).andRespond(withSuccess(taskJson(taskId, orderId, "IN_PROGRESS", shiftId), MediaType.APPLICATION_JSON));

        MaintenanceRejectedException conflict = catchThrowableOfType(MaintenanceRejectedException.class,
                () -> client.startTask(orderId, taskId, shiftId, "campo.tecnico1"));
        assertThat(conflict.getStatus()).isEqualTo(409);
        assertThat(conflict.getErrorCode()).isEqualTo("TRN-001");
        assertThat(conflict.isTransitionConflict()).isTrue();
        assertThat(conflict.reason()).isEqualTo("TRN-001: Task 1 is IN_PROGRESS and cannot be started");
        assertThat(conflict.getMessage()).contains("409 TRN-001: Task 1 is IN_PROGRESS and cannot be started");

        MaintenanceRejectedException shift = catchThrowableOfType(MaintenanceRejectedException.class,
                () -> client.startTask(orderId, taskId, shiftId, "campo.tecnico1"));
        assertThat(shift.getErrorCode()).isEqualTo("SHF-001");
        assertThat(shift.isTransitionConflict()).isFalse();

        // Un cuerpo que no es el JSON de mantenimiento (un proxy, por ejemplo) sigue siendo un rechazo, sin codigo.
        MaintenanceRejectedException unreadable = catchThrowableOfType(MaintenanceRejectedException.class,
                () -> client.startTask(orderId, taskId, shiftId, "campo.tecnico1"));
        assertThat(unreadable.getErrorCode()).isNull();
        assertThat(unreadable.reason()).isEqualTo("HTTP 404: <html>not here</html>");

        // Tres rechazos seguidos con un circuito que abre con dos fallos: sigue cerrado, porque mantenimiento respondio.
        assertThat(circuit()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(client.startTask(orderId, taskId, shiftId, "campo.tecnico1").isInProgress()).isTrue();
        server.verify();
    }

    @Test
    void anOpenCircuitFailsFastWithoutCallingMaintenance() {
        UUID shiftId = UUID.randomUUID();
        server.expect(requestTo(BASE + "/api/v1/maintenance/shifts/" + shiftId)).andRespond(withStatus(HttpStatus.SERVICE_UNAVAILABLE));
        server.expect(requestTo(BASE + "/api/v1/maintenance/shifts/" + shiftId)).andRespond(withStatus(HttpStatus.BAD_GATEWAY)
                .contentType(MediaType.TEXT_PLAIN).body("upstream down"));

        assertThatThrownBy(() -> client.findShift(shiftId)).isInstanceOf(MaintenanceUnavailableException.class);
        assertThatThrownBy(() -> client.findShift(shiftId)).isInstanceOf(MaintenanceUnavailableException.class).hasMessageContaining("upstream down");
        assertThat(circuit()).isEqualTo(CircuitBreaker.State.OPEN);

        // Tercera llamada: el servidor simulado no espera nada mas, asi que si llegara fallaria el test.
        assertThatThrownBy(() -> client.findShift(shiftId)).isInstanceOf(MaintenanceUnavailableException.class).hasMessageContaining("unavailable");
        server.verify();
    }

    @Test
    void theServiceAccountBeingRefusedIsAnOutageAndCountsForTheCircuit() {
        UUID shiftId = UUID.randomUUID();
        server.expect(requestTo(BASE + "/api/v1/maintenance/shifts/" + shiftId)).andRespond(withStatus(HttpStatus.FORBIDDEN));
        server.expect(requestTo(BASE + "/api/v1/maintenance/shifts/" + shiftId)).andRespond(withStatus(HttpStatus.UNAUTHORIZED));

        // 403 y 401 hablan de la cuenta de servicio, no del turno: se arreglan sin tocar nada aqui.
        assertThatThrownBy(() -> client.findShift(shiftId)).isInstanceOf(MaintenanceUnavailableException.class);
        assertThatThrownBy(() -> client.findShift(shiftId)).isInstanceOf(MaintenanceUnavailableException.class);
        assertThat(circuit()).isEqualTo(CircuitBreaker.State.OPEN);
        server.verify();
    }

    @Test
    void onlyA4xxOtherThanTheCredentialsTimeoutsAndThrottlingIsARejection() {
        assertThat(RestClientMaintenanceClient.isRejection(HttpClientErrorException.create(HttpStatus.CONFLICT, "Conflict", null, null, null))).isTrue();
        assertThat(RestClientMaintenanceClient.isRejection(HttpClientErrorException.create(HttpStatus.BAD_REQUEST, "Bad Request", null, null, null))).isTrue();
        assertThat(RestClientMaintenanceClient.isRejection(HttpClientErrorException.create(HttpStatus.NOT_FOUND, "Not Found", null, null, null))).isTrue();
        for (HttpStatus status : new HttpStatus[] {HttpStatus.UNAUTHORIZED, HttpStatus.FORBIDDEN, HttpStatus.REQUEST_TIMEOUT, HttpStatus.TOO_MANY_REQUESTS}) {
            assertThat(RestClientMaintenanceClient.isRejection(HttpClientErrorException.create(status, status.getReasonPhrase(), null, null, null)))
                    .as(status.toString()).isFalse();
        }
        assertThat(RestClientMaintenanceClient.isRejection(HttpServerErrorException.create(HttpStatus.SERVICE_UNAVAILABLE, "Unavailable", null, null, null))).isFalse();
        assertThat(RestClientMaintenanceClient.isRejection(new SocketTimeoutException("read timed out"))).isFalse();
    }

    /**
     * Las llamadas a mantenimiento salen de una llamada gRPC, de la cola de trabajo de un dispositivo
     * o del reintento programado, nunca de una peticion HTTP. El contexto real trae el gestor OAuth2
     * que registra Spring Security por defecto, que exige una ({@code servletRequest cannot be null}):
     * la cuenta de servicio tiene que autorizarse con el suyo propio, y aqui se comprueba con ese
     * contexto, no con el cliente montado a mano.
     */
    @Test
    void theServiceAccountTokenGoesOutWithoutAnHttpRequestInCourse() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer maintenance = MockRestServiceServer.bindTo(builder).build();
        UUID shiftId = UUID.randomUUID();
        maintenance.expect(requestTo(BASE + "/api/v1/maintenance/shifts/" + shiftId))
                .andExpect(header(HttpHeaders.AUTHORIZATION, "Bearer service-account-token"))
                .andRespond(withSuccess("""
                        {"id":"%s","code":"SH-000001","shiftDate":"2026-10-09","team":{"code":"EQ","name":"Equipo"},"status":"IN_PROGRESS"}"""
                        .formatted(shiftId), MediaType.APPLICATION_JSON));

        new WebApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OAuth2ClientAutoConfiguration.class, OAuth2ClientWebSecurityAutoConfiguration.class))
                .withUserConfiguration(MaintenanceClientConfiguration.class, ResourceServerSecurity.class)
                .withBean(RestClient.Builder.class, () -> builder)
                .withBean(CircuitBreakerFactory.class, () -> new Resilience4JCircuitBreakerFactory(
                        CircuitBreakerRegistry.ofDefaults(), TimeLimiterRegistry.ofDefaults(), null))
                .withPropertyValues(
                        "app.maintenance.enabled=true",
                        "app.maintenance.base-url=" + BASE,
                        "app.maintenance.client-registration-id=mto-services",
                        "spring.security.oauth2.client.registration.mto-services.client-id=mto-field-svc",
                        "spring.security.oauth2.client.registration.mto-services.client-secret=secret",
                        "spring.security.oauth2.client.registration.mto-services.authorization-grant-type=client_credentials",
                        "spring.security.oauth2.client.registration.mto-services.provider=keycloak",
                        "spring.security.oauth2.client.provider.keycloak.token-uri=http://auth/token")
                .run(context -> {
                    // Un token vigente de la cuenta de servicio, para no tener que pedirlo a Keycloak.
                    ClientRegistration registration = context.getBean(ClientRegistrationRepository.class).findByRegistrationId("mto-services");
                    OAuth2AccessToken token = new OAuth2AccessToken(OAuth2AccessToken.TokenType.BEARER, "service-account-token", Instant.now(),
                            Instant.now().plus(Duration.ofHours(1)));
                    context.getBean(OAuth2AuthorizedClientService.class).saveAuthorizedClient(
                            new OAuth2AuthorizedClient(registration, "mto-field", token), new TestingAuthenticationToken("mto-field", null));

                    assertThat(context.getBean(RestClientMaintenanceClient.class).findShift(shiftId)).isPresent();
                    maintenance.verify();
                });
    }

    /** Como la del servicio: {@code @EnableWebSecurity} y su propia cadena, que es lo que trae el gestor por defecto. */
    @Configuration(proxyBeanMethods = false)
    @EnableWebSecurity
    static class ResourceServerSecurity {

        @Bean
        SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
            return http.authorizeHttpRequests(requests -> requests.anyRequest().authenticated()).build();
        }
    }
}
