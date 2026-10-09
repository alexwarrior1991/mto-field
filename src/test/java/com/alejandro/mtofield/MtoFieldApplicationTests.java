package com.alejandro.mtofield;

import com.alejandro.mtofield.application.service.FieldCodeGenerator;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.application.service.FieldEventService;
import com.alejandro.mtofield.application.service.FieldEventSynchronizer;
import com.alejandro.mtofield.application.service.LivenessRegistry;
import com.alejandro.mtofield.application.service.MaintenanceClient;
import com.alejandro.mtofield.application.service.PossessionBoardService;
import com.alejandro.mtofield.application.service.PossessionService;
import com.alejandro.mtofield.support.PostgreSQLTestContainer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;
import io.grpc.reflection.v1.ServerReflectionResponse;
import io.grpc.reflection.v1.ServiceResponse;
import io.grpc.stub.StreamObserver;
import io.micrometer.tracing.Tracer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.grpc.test.autoconfigure.LocalGrpcServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El contexto entero contra un PostgreSQL real, sin ningun servicio sustituido por un doble, con el
 * servidor gRPC de Netty escuchando en un puerto libre.
 *
 * <p>Los dos tests de salud y reflexion son el equivalente local de
 * {@code grpcurl -plaintext localhost:9094 list}: sin token, el servidor tiene que listar
 * {@code mto.field.v1.FieldService} y responder {@code SERVING}.</p>
 */
@SpringBootTest(properties = {
        "server.port=0",
        "spring.grpc.server.port=0",
        "app.maintenance.enabled=false"
})
class MtoFieldApplicationTests extends PostgreSQLTestContainer {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registerPostgreSQLProperties(registry);
    }

    @Autowired
    private ApplicationContext context;

    @Autowired(required = false)
    private Tracer tracer;

    @LocalGrpcServerPort
    private int grpcPort;

    private ManagedChannel channel;

    @BeforeEach
    void openChannel() {
        channel = ManagedChannelBuilder.forAddress("localhost", grpcPort).usePlaintext().build();
    }

    @AfterEach
    void closeChannel() {
        channel.shutdownNow();
    }

    @Test
    void contextLoads() {
        assertThat(context).isNotNull();
    }

    @Test
    void everyBusinessServiceIsInTheContext() {
        for (Class<?> service : BusinessServices.ALL) {
            assertThat(context.getBeanNamesForType(service))
                    .withFailMessage("""
                            No hay ningun bean de %s en el contexto. El servicio gRPC lo pide por \
                            constructor, asi que la aplicacion no arranca. Comprobar que su impl \
                            sigue anotada con @Service y que no ha vuelto un @ConditionalOnBean.""",
                            service.getSimpleName())
                    .isNotEmpty();
        }
    }

    @Test
    void withMaintenanceOffTheClientIsTheDisconnectedOne() {
        assertThat(context.getBean(MaintenanceClient.class).isEnabled()).isFalse();
    }

    @Test
    void theTracingBridgeIsInTheContext() {
        assertThat(tracer).isNotNull();
    }

    @Test
    void theHealthServiceAnswersServingWithoutAToken() {
        HealthCheckResponse response = HealthGrpc.newBlockingStub(channel)
                .withDeadlineAfter(10, TimeUnit.SECONDS)
                .check(HealthCheckRequest.newBuilder().setService("").build());

        assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
    }

    @Test
    void theReflectionServiceListsTheFieldServiceWithoutAToken() throws Exception {
        CompletableFuture<List<String>> services = new CompletableFuture<>();
        StreamObserver<ServerReflectionRequest> requests = ServerReflectionGrpc.newStub(channel)
                .withDeadlineAfter(10, TimeUnit.SECONDS)
                .serverReflectionInfo(new StreamObserver<>() {
                    @Override
                    public void onNext(ServerReflectionResponse response) {
                        services.complete(response.getListServicesResponse().getServiceList().stream()
                                .map(ServiceResponse::getName)
                                .toList());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        services.completeExceptionally(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        services.completeExceptionally(new IllegalStateException("No reflection response"));
                    }
                });
        requests.onNext(ServerReflectionRequest.newBuilder().setListServices("").build());
        requests.onCompleted();

        assertThat(services.get(10, TimeUnit.SECONDS))
                .contains("mto.field.v1.FieldService", "grpc.health.v1.Health");
    }

    /** Lista viva: cada fase que anade un servicio lo anade aqui. */
    static final class BusinessServices {
        static final List<Class<?>> ALL = List.of(
                FieldCodeGenerator.class,
                FieldCommandService.class,
                FieldEventService.class,
                FieldEventSynchronizer.class,
                LivenessRegistry.class,
                MaintenanceClient.class,
                PossessionBoardService.class,
                PossessionService.class);

        private BusinessServices() {
        }
    }
}
