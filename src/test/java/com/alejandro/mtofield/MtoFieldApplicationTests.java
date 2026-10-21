package com.alejandro.mtofield;

import com.alejandro.mtofield.application.service.DomainEventPublisher;
import com.alejandro.mtofield.application.service.EvacuationAckWatchdog;
import com.alejandro.mtofield.application.service.FieldCodeGenerator;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.application.service.FieldEventService;
import com.alejandro.mtofield.application.service.FieldEventSyncRetryService;
import com.alejandro.mtofield.application.service.FieldEventSynchronizer;
import com.alejandro.mtofield.application.service.LivenessRegistry;
import com.alejandro.mtofield.application.service.MaintenanceClient;
import com.alejandro.mtofield.application.service.PossessionBoardService;
import com.alejandro.mtofield.application.service.PossessionService;
import com.alejandro.mtofield.application.service.RemoteDeviceStates;
import com.alejandro.mtofield.application.service.ReplicaBus;
import com.alejandro.mtofield.support.PostgreSQLTestContainer;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.grpc.test.autoconfigure.LocalGrpcServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * El contexto entero contra un PostgreSQL real, sin ningun servicio sustituido por un doble, con el
 * servidor gRPC de Netty escuchando en un puerto libre.
 *
 * <p>Los dos tests de salud y reflexion son el equivalente local de
 * {@code grpcurl -plaintext localhost:9094 list}: sin token, el servidor tiene que listar
 * {@code mto.field.v1.FieldService} y responder {@code SERVING}.</p>
 *
 * <p>Sin el perfil {@code grpc-tls} el servidor habla en claro (el canal de arriba es
 * {@code usePlaintext}); {@link WithTls} arranca otro contexto con el perfil y comprueba el TLS.</p>
 */
@SpringBootTest(properties = {
        "server.port=0",
        "spring.grpc.server.port=0",
        "app.maintenance.enabled=false",
        // Sin broker en los tests: el bus de replicas es el NoOp (RabbitReplicaBusIT prueba el real).
        "app.rabbitmq.enabled=false", "app.field.evacuation.enabled=false"
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

    /**
     * El perfil {@code grpc-tls} tal cual: el bundle PEM de {@code application-grpc-tls.yml} con un
     * certificado autofirmado para {@code localhost}, generado aqui con el {@code keytool} del JDK
     * para no guardar ninguna clave en el repositorio. Es el {@code grpcurl -cacert} de la imagen:
     * salud por TLS confiando en ese certificado, y un cliente en claro que ya no entra.
     */
    @Nested
    @ActiveProfiles({"dev", "grpc-tls"})
    class WithTls {

        private static Path certificate;

        @DynamicPropertySource
        static void tlsProperties(DynamicPropertyRegistry registry) throws Exception {
            Path directory = Files.createTempDirectory("mto-field-tls");
            Path[] pem = selfSignedForLocalhost(directory);
            certificate = pem[0];
            registry.add("GRPC_TLS_CERTIFICATE", () -> "file:" + pem[0]);
            registry.add("GRPC_TLS_PRIVATE_KEY", () -> "file:" + pem[1]);
        }

        @LocalGrpcServerPort
        private int tlsPort;

        @Test
        void theHealthServiceAnswersOverTlsTrustingTheServerCertificate() throws IOException {
            ManagedChannel tls = Grpc.newChannelBuilderForAddress("localhost", tlsPort,
                    TlsChannelCredentials.newBuilder().trustManager(certificate.toFile()).build()).build();
            try {
                HealthCheckResponse response = HealthGrpc.newBlockingStub(tls)
                        .withDeadlineAfter(10, TimeUnit.SECONDS)
                        .check(HealthCheckRequest.newBuilder().setService("").build());

                assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
            } finally {
                tls.shutdownNow();
            }
        }

        @Test
        void aPlaintextClientNoLongerGetsIn() {
            ManagedChannel plaintext = ManagedChannelBuilder.forAddress("localhost", tlsPort).usePlaintext().build();
            try {
                assertThatThrownBy(() -> HealthGrpc.newBlockingStub(plaintext)
                        .withDeadlineAfter(10, TimeUnit.SECONDS)
                        .check(HealthCheckRequest.newBuilder().setService("").build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                                assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
            } finally {
                plaintext.shutdownNow();
            }
        }

        /** El certificado y la clave en PEM, como los deja el secreto de serving-cert de OpenShift. */
        private static Path[] selfSignedForLocalhost(Path directory) throws Exception {
            Path keystore = directory.resolve("grpc.p12");
            Process keytool = new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                    "-genkeypair", "-alias", "grpc", "-keyalg", "RSA", "-keysize", "2048", "-validity", "1",
                    "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                    "-storetype", "PKCS12", "-keystore", keystore.toString(),
                    "-storepass", "changeit", "-keypass", "changeit")
                    .redirectErrorStream(true)
                    .start();
            String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertThat(keytool.waitFor()).withFailMessage("keytool fallo: %s", output).isZero();

            KeyStore store = KeyStore.getInstance("PKCS12");
            try (var in = Files.newInputStream(keystore)) {
                store.load(in, "changeit".toCharArray());
            }
            Certificate cert = store.getCertificate("grpc");
            var key = store.getKey("grpc", "changeit".toCharArray());
            Path certificatePem = directory.resolve("tls.crt");
            Path keyPem = directory.resolve("tls.key");
            Files.writeString(certificatePem, pem("CERTIFICATE", cert.getEncoded()));
            Files.writeString(keyPem, pem("PRIVATE KEY", key.getEncoded()));
            return new Path[] {certificatePem, keyPem};
        }

        private static String pem(String type, byte[] der) {
            return "-----BEGIN " + type + "-----\n"
                    + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)
                    + "\n-----END " + type + "-----\n";
        }
    }

    /** Lista viva: cada fase que anade un servicio lo anade aqui. */
    static final class BusinessServices {
        static final List<Class<?>> ALL = List.of(
                DomainEventPublisher.class,
                EvacuationAckWatchdog.class,
                FieldCodeGenerator.class,
                FieldCommandService.class,
                FieldEventService.class,
                FieldEventSyncRetryService.class,
                FieldEventSynchronizer.class,
                LivenessRegistry.class,
                MaintenanceClient.class,
                PossessionBoardService.class,
                PossessionService.class,
                RemoteDeviceStates.class,
                ReplicaBus.class);

        private BusinessServices() {
        }
    }
}
