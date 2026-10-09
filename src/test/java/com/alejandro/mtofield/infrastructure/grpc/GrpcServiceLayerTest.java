package com.alejandro.mtofield.infrastructure.grpc;

import com.alejandro.mtofield.grpc.v1.ClosePossessionRequest;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.grpc.v1.FieldServiceGrpc;
import com.alejandro.mtofield.grpc.v1.IssueCommandRequest;
import com.alejandro.mtofield.grpc.v1.OpenPossessionRequest;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.alejandro.mtofield.grpc.v1.WatchPossessionBoardRequest;
import com.alejandro.mtofield.support.PostgreSQLTestContainer;
import com.alejandro.mtofield.support.TestJwtDecoderConfiguration;
import com.alejandro.mtofield.support.TestTokens;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;
import io.grpc.reflection.v1.ServerReflectionResponse;
import io.grpc.reflection.v1.ServiceResponse;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.grpc.test.autoconfigure.AutoConfigureTestGrpcTransport;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.grpc.client.GrpcChannelFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * El servicio gRPC entero sobre el transporte in-process, con el contexto real (seguridad incluida)
 * y un PostgreSQL de verdad. Los tokens los acuna {@link TestTokens} y los valida la misma cadena
 * que en produccion ({@link TestJwtDecoderConfiguration}), con la audiencia encendida.
 */
@SpringBootTest(properties = {
        "server.port=0",
        "app.maintenance.enabled=false",
        "app.security.audience-validation-enabled=true",
        "app.security.required-audience=" + TestTokens.AUDIENCE,
        "spring.security.oauth2.resourceserver.jwt.issuer-uri=" + TestTokens.ISSUER,
        "app.field.board.tick=500ms"
})
@AutoConfigureTestGrpcTransport
@Import(TestJwtDecoderConfiguration.class)
class GrpcServiceLayerTest extends PostgreSQLTestContainer {

    private static final String TECHNICIAN = "campo.tecnico1";
    private static final String SUPERVISOR = "campo.responsable";

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registerPostgreSQLProperties(registry);
    }

    @Autowired
    private GrpcChannelFactory channels;

    private ManagedChannel channel;

    @BeforeEach
    void openChannel() {
        channel = channels.createChannel("in-process");
    }

    @AfterEach
    void closeChannel() {
        channel.shutdownNow();
    }

    @Nested
    @DisplayName("Autenticacion")
    class Authentication {

        @Test
        void theHealthServiceAnswersWithoutAToken() {
            HealthCheckResponse response = HealthGrpc.newBlockingStub(channel)
                    .check(HealthCheckRequest.newBuilder().setService("").build());

            assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
        }

        @Test
        void theReflectionServiceListsTheFieldServiceWithoutAToken() throws Exception {
            assertThat(listServices()).contains("mto.field.v1.FieldService", "grpc.health.v1.Health");
        }

        @Test
        void aCallWithoutATokenIsUnauthenticated() {
            assertThat(statusOf(() -> blockingStub().openPossession(OpenPossessionRequest.getDefaultInstance())))
                    .isEqualTo(Status.Code.UNAUTHENTICATED);
        }

        /**
         * Un token del realm emitido para otra API esta igual de bien firmado y lleva el mismo
         * emisor: solo la audiencia lo distingue. Sin esta comprobacion bastaria con tener cuenta
         * en el realm para entrar.
         */
        @Test
        void aTokenForAnotherAudienceIsUnauthenticated() {
            assertThat(statusOf(() -> withToken(TestTokens.forAnotherAudience(SUPERVISOR))
                    .openPossession(OpenPossessionRequest.getDefaultInstance())))
                    .isEqualTo(Status.Code.UNAUTHENTICATED);
        }

        @Test
        void anExpiredTokenIsUnauthenticated() {
            assertThat(statusOf(() -> withToken(TestTokens.expired(SUPERVISOR))
                    .openPossession(OpenPossessionRequest.getDefaultInstance())))
                    .isEqualTo(Status.Code.UNAUTHENTICATED);
        }

        @Test
        void aTokenSignedByAnotherKeyIsUnauthenticated() {
            assertThat(statusOf(() -> withToken(TestTokens.signedByAnotherKey(SUPERVISOR))
                    .openPossession(OpenPossessionRequest.getDefaultInstance())))
                    .isEqualTo(Status.Code.UNAUTHENTICATED);
        }
    }

    @Nested
    @DisplayName("Autorizacion por RPC")
    class Authorization {

        @Test
        void aTechnicianCannotOpenClosOrIssueOrWatch() {
            FieldServiceGrpc.FieldServiceBlockingStub technician = withToken(TestTokens.technician(TECHNICIAN));

            assertThat(statusOf(() -> technician.openPossession(OpenPossessionRequest.getDefaultInstance())))
                    .isEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(statusOf(() -> technician.closePossession(ClosePossessionRequest.getDefaultInstance())))
                    .isEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(statusOf(() -> technician.issueCommand(IssueCommandRequest.getDefaultInstance())))
                    .isEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(statusOf(() -> technician.watchPossessionBoard(WatchPossessionBoardRequest.getDefaultInstance()).hasNext()))
                    .isEqualTo(Status.Code.PERMISSION_DENIED);
        }

        /** El permiso del stream se comprueba al abrirlo, como en una RPC unaria. */
        @Test
        void aSupervisorWithoutTheTeamRoleCannotOpenTheTeamChannel() throws Exception {
            assertThat(teamChannelStatus(TestTokens.supervisorWithoutTeam(SUPERVISOR)))
                    .isEqualTo(Status.Code.PERMISSION_DENIED);
        }

        /** Con el rol correcto la llamada llega al servicio; en la Fase 0 este aun no la implementa. */
        @Test
        void aTechnicianReachesTheTeamChannel() throws Exception {
            assertThat(teamChannelStatus(TestTokens.technician(TECHNICIAN)))
                    .isEqualTo(Status.Code.UNIMPLEMENTED);
        }

        @Test
        void aSupervisorReachesThePossessionCalls() {
            FieldServiceGrpc.FieldServiceBlockingStub supervisor = withToken(TestTokens.supervisor(SUPERVISOR));

            assertThat(statusOf(() -> supervisor.openPossession(OpenPossessionRequest.getDefaultInstance())))
                    .isEqualTo(Status.Code.UNIMPLEMENTED);
            assertThat(statusOf(() -> supervisor.watchPossessionBoard(WatchPossessionBoardRequest.getDefaultInstance()).hasNext()))
                    .isEqualTo(Status.Code.UNIMPLEMENTED);
        }
    }

    private FieldServiceGrpc.FieldServiceBlockingStub blockingStub() {
        return FieldServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    private FieldServiceGrpc.FieldServiceBlockingStub withToken(String token) {
        return TestTokens.withToken(blockingStub(), token);
    }

    private Status.Code teamChannelStatus(String token) throws Exception {
        CompletableFuture<Status.Code> outcome = new CompletableFuture<>();
        StreamObserver<TeamMessage> requests = TestTokens.withToken(FieldServiceGrpc.newStub(channel), token)
                .withDeadlineAfter(10, TimeUnit.SECONDS)
                .teamChannel(new StreamObserver<>() {
                    @Override
                    public void onNext(FieldCommand command) {
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        outcome.complete(Status.fromThrowable(throwable).getCode());
                    }

                    @Override
                    public void onCompleted() {
                        outcome.complete(Status.Code.OK);
                    }
                });
        requests.onNext(TeamMessage.newBuilder().setDeviceId("device-1").build());
        return outcome.get(10, TimeUnit.SECONDS);
    }

    private List<String> listServices() throws Exception {
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
        return services.get(10, TimeUnit.SECONDS);
    }

    private static Status.Code statusOf(ThrowingCall call) {
        Throwable thrown = catchThrowable(call::run);
        assertThat(thrown).as("la llamada tenia que fallar").isInstanceOf(StatusRuntimeException.class);
        return ((StatusRuntimeException) thrown).getStatus().getCode();
    }

    @FunctionalInterface
    private interface ThrowingCall {
        void run() throws Exception;
    }
}
