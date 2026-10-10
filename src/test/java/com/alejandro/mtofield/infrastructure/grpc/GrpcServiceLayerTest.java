package com.alejandro.mtofield.infrastructure.grpc;

import com.alejandro.mtofield.application.dto.messaging.DomainEvent;
import com.alejandro.mtofield.application.service.DomainEventPublisher;
import com.alejandro.mtofield.configuration.security.CurrentUserService;
import com.alejandro.mtofield.infrastructure.messaging.outbox.MessagingCorrelation;
import org.assertj.core.api.InstanceOfAssertFactories;
import java.util.concurrent.CopyOnWriteArrayList;
import com.alejandro.mtofield.application.dto.CompleteTaskCommand;
import com.alejandro.mtofield.application.dto.SyncJob;
import com.alejandro.mtofield.application.exception.MaintenanceRejectedException;
import com.alejandro.mtofield.application.exception.MaintenanceUnavailableException;
import com.alejandro.mtofield.application.service.FieldEventService;
import com.alejandro.mtofield.application.service.FieldEventSyncRetryService;
import com.alejandro.mtofield.application.service.FieldEventSynchronizer;
import com.alejandro.mtofield.application.service.MaintenanceClient;
import com.alejandro.mtofield.domain.model.ShiftSnapshot;
import com.alejandro.mtofield.domain.model.TaskSnapshot;
import com.alejandro.mtofield.grpc.v1.ClosePossessionRequest;
import com.alejandro.mtofield.grpc.v1.EvacuateNow;
import com.alejandro.mtofield.grpc.v1.EventResult;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.grpc.v1.FieldServiceGrpc;
import com.alejandro.mtofield.grpc.v1.IssueCommandRequest;
import com.alejandro.mtofield.grpc.v1.IssueCommandResponse;
import com.alejandro.mtofield.grpc.v1.Join;
import com.alejandro.mtofield.grpc.v1.OpenPossessionRequest;
import com.alejandro.mtofield.grpc.v1.Possession;
import com.alejandro.mtofield.grpc.v1.PossessionBoard;
import com.alejandro.mtofield.grpc.v1.TeamState;
import com.alejandro.mtofield.grpc.v1.SupervisorMessage;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.alejandro.mtofield.grpc.v1.SyncResult;
import com.alejandro.mtofield.grpc.v1.WatchPossessionBoardRequest;
import com.alejandro.mtofield.infrastructure.grpc.advice.FieldGrpcExceptionAdvice;
import com.alejandro.mtofield.infrastructure.grpc.stream.CatchUpProbe;
import com.alejandro.mtofield.infrastructure.grpc.stream.DeviceStream;
import com.alejandro.mtofield.infrastructure.grpc.stream.SyncSessions;
import com.alejandro.mtofield.infrastructure.grpc.stream.TeamBinding;
import com.alejandro.mtofield.infrastructure.grpc.stream.TeamChannels;
import com.alejandro.mtofield.infrastructure.grpc.stream.TokenExpirySweeper;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldEventRecord;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldEventSyncStatus;
import com.alejandro.mtofield.infrastructure.persistence.repository.FieldEventRepository;
import com.alejandro.mtofield.support.BoardClient;
import com.alejandro.mtofield.support.DeviceClient;
import com.alejandro.mtofield.support.PostgreSQLTestContainer;
import com.alejandro.mtofield.support.SyncClient;
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
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.grpc.test.autoconfigure.AutoConfigureTestGrpcTransport;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.grpc.client.GrpcChannelFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        "app.field.board.tick=500ms",
        "app.field.token-expiry.sweep=300ms",
        "app.field.team-binding.enabled=false",
        "app.rabbitmq.enabled=false", "app.field.evacuation.enabled=false"
})
@AutoConfigureTestGrpcTransport
@Import({TestJwtDecoderConfiguration.class, GrpcServiceLayerTest.Probes.class})
class GrpcServiceLayerTest extends PostgreSQLTestContainer {

    private static final String TECHNICIAN = "campo.tecnico1";
    private static final String SUPERVISOR = "campo.responsable";
    private static final Duration QUIET = Duration.ofMillis(400);

    /**
     * Las dos costuras de los escenarios de carrera: paran el catch-up de un dispositivo concreto
     * en un punto, o la sincronizacion de sus eventos de tarea, hasta que el test las suelta.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class Probes {

        @Bean
        @Primary
        BlockingCatchUpProbe blockingCatchUpProbe() {
            return new BlockingCatchUpProbe();
        }

        /** Lo que los ganchos publican hacia fuera, con el actor y la correlacion que ven al publicar. */
        @Bean
        @Primary
        RecordingDomainEventPublisher recordingDomainEventPublisher(CurrentUserService currentUser) {
            return new RecordingDomainEventPublisher(currentUser);
        }

        /** Delega en el sincronizador que tenga el contexto (el de PENDING_SYNC con el cliente apagado, el de mantenimiento con el encendido). */
        @Bean
        @Primary
        GatedSynchronizer gatedSynchronizer(ObjectProvider<FieldEventSynchronizer> synchronizers) {
            return new GatedSynchronizer(() -> synchronizers.stream().filter(candidate -> !(candidate instanceof GatedSynchronizer)).findFirst()
                    .orElseThrow(() -> new IllegalStateException("no FieldEventSynchronizer to delegate to")));
        }
    }

    static final class BlockingCatchUpProbe implements CatchUpProbe {

        enum Phase { AFTER_REGISTERED, BEFORE_FLUSH }

        private volatile String armedDevice;
        private volatile Phase armedPhase;
        private volatile CountDownLatch arrived;
        private volatile CountDownLatch release;

        void arm(String deviceId, Phase phase) {
            arrived = new CountDownLatch(1);
            release = new CountDownLatch(1);
            armedPhase = phase;
            armedDevice = deviceId;
        }

        void awaitArrival() throws InterruptedException {
            assertThat(arrived.await(10, TimeUnit.SECONDS)).as("el catch-up tenia que llegar a la costura").isTrue();
        }

        void release() {
            armedDevice = null;
            release.countDown();
        }

        @Override
        public void afterRegistered(DeviceStream stream) {
            hold(stream, Phase.AFTER_REGISTERED);
        }

        @Override
        public void beforeFlush(DeviceStream stream) {
            hold(stream, Phase.BEFORE_FLUSH);
        }

        private void hold(DeviceStream stream, Phase phase) {
            if (!stream.deviceId().equals(armedDevice) || armedPhase != phase) {
                return;
            }
            arrived.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static final class GatedSynchronizer implements FieldEventSynchronizer {

        private final Supplier<FieldEventSynchronizer> delegate;
        private final Map<String, CountDownLatch> gates = new ConcurrentHashMap<>();

        GatedSynchronizer(Supplier<FieldEventSynchronizer> delegate) {
            this.delegate = delegate;
        }

        void block(String deviceId) {
            gates.put(deviceId, new CountDownLatch(1));
        }

        void release(String deviceId) {
            CountDownLatch gate = gates.remove(deviceId);
            if (gate != null) {
                gate.countDown();
            }
        }

        @Override
        public Outcome process(SyncJob job) {
            CountDownLatch gate = gates.get(job.context().deviceId());
            if (gate != null) {
                try {
                    gate.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return Outcome.PENDING;
                }
            }
            return delegate.get().process(job);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registerPostgreSQLProperties(registry);
    }

    @Autowired
    private RecordingDomainEventPublisher recorder;

    @Autowired
    private GrpcChannelFactory channels;

    @Autowired
    private BlockingCatchUpProbe catchUpProbe;

    @Autowired
    private GatedSynchronizer synchronizer;

    @Autowired
    private FieldEventRepository eventRepository;

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

        /** Con el rol correcto la llamada llega al servicio, que exige un Join como primer mensaje. */
        @Test
        void aTechnicianReachesTheTeamChannel() throws Exception {
            assertThat(teamChannelStatus(TestTokens.technician(TECHNICIAN)))
                    .isEqualTo(Status.Code.FAILED_PRECONDITION);
        }

        /** Con el rol correcto la llamada llega al servicio, que valida la peticion (sin turnos). */
        @Test
        void aSupervisorReachesThePossessionCalls() {
            FieldServiceGrpc.FieldServiceBlockingStub supervisor = withToken(TestTokens.supervisor(SUPERVISOR));

            StatusRuntimeException error = errorOf(() -> supervisor.openPossession(OpenPossessionRequest.getDefaultInstance()));
            assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
            assertThat(GrpcErrors.reasonOf(error)).isEqualTo("INVALID_POSSESSION_REQUEST");
            assertThat(statusOf(() -> supervisor.watchPossessionBoard(WatchPossessionBoardRequest.getDefaultInstance()).hasNext()))
                    .as("sin possession_id")
                    .isEqualTo(Status.Code.INVALID_ARGUMENT);
        }
    }

    @Nested
    @DisplayName("Posesiones y ordenes (unarias)")
    class Possessions {

        @Test
        void aPossessionIsOpenedWithItsShiftsAndClosedWhenEveryoneIsClear() {
            UUID shiftA = UUID.randomUUID();
            UUID shiftB = UUID.randomUUID();

            Possession possession = open(shiftA, shiftB);

            assertThat(possession.getCode()).startsWith("PO-");
            assertThat(possession.getShiftIdsList()).containsExactly(shiftA.toString(), shiftB.toString());
            assertThat(possession.getStatus()).isEqualTo(Possession.Status.OPEN);
            assertThat(possession.hasEndsAt()).isTrue();

            StatusRuntimeException notAllClear = errorOf(() -> supervisor().closePossession(close(possession, false, "")));
            assertThat(notAllClear.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
            assertThat(GrpcErrors.reasonOf(notAllClear)).isEqualTo("POSSESSION_NOT_ALL_CLEAR");
            assertThat(GrpcErrors.metadataOf(notAllClear).get(FieldGrpcExceptionAdvice.PENDING_TEAMS).split(","))
                    .containsExactlyInAnyOrder(teamCode(shiftA), teamCode(shiftB));

            StatusRuntimeException forcedWithoutReason = errorOf(() -> supervisor().closePossession(close(possession, true, " ")));
            assertThat(forcedWithoutReason.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);

            Possession closed = supervisor().closePossession(close(possession, true, "Fin de la ventana"));
            assertThat(closed.getStatus()).isEqualTo(Possession.Status.CLOSED);
            StatusRuntimeException again = errorOf(() -> supervisor().closePossession(close(possession, true, "otra vez")));
            assertThat(again.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
            assertThat(GrpcErrors.reasonOf(again)).isEqualTo("POSSESSION_NOT_OPEN");
        }

        @Test
        void aShiftCannotBeInTwoOpenPossessionsAndIdsAreValidated() {
            UUID shift = UUID.randomUUID();
            Possession first = open(shift);

            StatusRuntimeException twice = errorOf(() -> supervisor().openPossession(OpenPossessionRequest.newBuilder().addShiftIds(shift.toString()).build()));
            assertThat(twice.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
            assertThat(GrpcErrors.reasonOf(twice)).isEqualTo("SHIFT_ALREADY_IN_OPEN_POSSESSION");

            StatusRuntimeException garbage = errorOf(() -> supervisor().openPossession(OpenPossessionRequest.newBuilder().addShiftIds("not-a-uuid").build()));
            assertThat(garbage.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);

            StatusRuntimeException unknown = errorOf(() -> supervisor().closePossession(close(UUID.randomUUID().toString(), true, "x")));
            assertThat(unknown.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
            assertThat(GrpcErrors.reasonOf(unknown)).isEqualTo("POSSESSION_NOT_FOUND");

            supervisor().closePossession(close(first, true, "limpieza"));
        }

        @Test
        void issueCommandTakesConsecutiveSequencesAndTheIdempotencyKeyReturnsTheSameCommand() {
            Possession possession = open(UUID.randomUUID());

            IssueCommandResponse first = say(possession.getId(), "Pausa");
            IssueCommandResponse second = evacuate(possession.getId(), "k-1", "Tren");
            IssueCommandResponse retried = evacuate(possession.getId(), "k-1", "Tren (reintento)");

            assertThat(first.getSequence()).isEqualTo(1L);
            assertThat(second.getSequence()).isEqualTo(2L);
            assertThat(retried).isEqualTo(second);
            StatusRuntimeException empty = errorOf(() -> supervisor().issueCommand(IssueCommandRequest.newBuilder().setPossessionId(possession.getId()).build()));
            assertThat(empty.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);

            supervisor().closePossession(close(possession, true, "limpieza"));
            StatusRuntimeException closed = errorOf(() -> say(possession.getId(), "tarde"));
            assertThat(closed.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
            assertThat(GrpcErrors.reasonOf(closed)).isEqualTo("POSSESSION_NOT_OPEN");
        }
    }

    @Nested
    @DisplayName("TeamChannel: Join y Welcome")
    class JoinAndWelcome {

        @Test
        void aJoinIsAnsweredWithAWelcomeCarryingThePossessionAndTheWatermark() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);

            DeviceClient device = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), "dev-welcome", shift, 0);
            FieldCommand welcome = device.awaitWelcome();

            assertThat(welcome.getWelcome().getPossessionId()).isEqualTo(possession.getId());
            assertThat(welcome.getWelcome().getLastAppliedSequence()).isZero();
            assertThat(welcome.getWelcome().hasServerTime()).isTrue();
            assertThat(welcome.getWelcome().getEndsAt()).isEqualTo(possession.getEndsAt());
            assertThat(welcome.getRequiresAck()).isFalse();

            device.halfClose();
            assertThat(device.outcome().getCode()).as("despedirse es un fin normal").isEqualTo(Status.Code.OK);
            supervisor().closePossession(close(possession, true, "limpieza"));
        }

        @Test
        void theFirstMessageMustBeAJoinWithADeviceId() {
            DeviceClient noJoin = DeviceClient.open(channel, TestTokens.technician(TECHNICIAN), "dev-nojoin");
            noJoin.heartbeat("1.000", 50, -80);
            assertThat(noJoin.outcome().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
            assertThat(GrpcErrors.reasonOf(noJoin.outcomeError())).isEqualTo(TeamChannels.REASON_JOIN_REQUIRED);

            DeviceClient noDevice = DeviceClient.open(channel, TestTokens.technician(TECHNICIAN), "");
            noDevice.join(UUID.randomUUID(), 0);
            assertThat(noDevice.outcome().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        }

        @Test
        void aShiftOutsideAnOpenPossessionCannotJoin() {
            DeviceClient device = DeviceClient.open(channel, TestTokens.technician(TECHNICIAN), "dev-orphan");
            device.join(UUID.randomUUID(), 0);

            assertThat(device.outcome().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
            assertThat(GrpcErrors.reasonOf(device.outcomeError())).isEqualTo(TeamChannels.REASON_SHIFT_NOT_IN_OPEN_POSSESSION);
        }
    }

    @Nested
    @DisplayName("Desalojo con acuses")
    class Evacuation {

        @Test
        void evacuateNowReachesEveryTeamOnceAcksArePerTeamAndCloseNeedsEveryoneClear() {
            UUID shiftA = UUID.randomUUID();
            UUID shiftB = UUID.randomUUID();
            UUID shiftC = UUID.randomUUID();
            Possession possession = open(shiftA, shiftB, shiftC);
            DeviceClient a = DeviceClient.join(channel, TestTokens.technician("tecnico.a"), "dev-a", shiftA, 0);
            DeviceClient b = DeviceClient.join(channel, TestTokens.technician("tecnico.b"), "dev-b", shiftB, 0);
            DeviceClient c = DeviceClient.join(channel, TestTokens.technician("tecnico.c"), "dev-c", shiftC, 0);
            a.awaitWelcome();
            b.awaitWelcome();
            c.awaitWelcome();

            IssueCommandResponse evacuation = evacuate(possession.getId(), "evac-1", "Tren de trabajos en aproximacion");
            for (DeviceClient device : List.of(a, b, c)) {
                FieldCommand command = device.nextCommand();
                assertThat(command.getCommandId()).isEqualTo(evacuation.getCommandId());
                assertThat(command.getSequence()).isEqualTo(evacuation.getSequence());
                assertThat(command.getRequiresAck()).isTrue();
                assertThat(command.getEvacuateNow().getReason()).isEqualTo("Tren de trabajos en aproximacion");
            }

            long ackA = a.ack(evacuation.getCommandId());
            long ackB = b.ack(evacuation.getCommandId());
            assertThat(a.nextEventResult().getEventResult()).satisfies(result -> {
                assertThat(result.getSequence()).isEqualTo(ackA);
                assertThat(result.getOutcome()).isEqualTo(EventResult.Outcome.APPLIED);
            });
            assertThat(b.nextEventResult().getEventResult().getSequence()).isEqualTo(ackB);
            long badAck = c.ack(UUID.randomUUID().toString());
            FieldCommand rejected = c.nextEventResult();
            assertThat(rejected.getEventResult().getSequence()).isEqualTo(badAck);
            assertThat(rejected.getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.REJECTED);
            assertThat(rejected.getEventResult().getReason()).isEqualTo("unknown command");

            IssueCommandResponse retried = evacuate(possession.getId(), "evac-1", "Tren de trabajos en aproximacion");
            assertThat(retried).isEqualTo(evacuation);
            assertThat(a.maybeNext(QUIET)).as("el reintento no manda una copia").isEmpty();
            assertThat(b.maybeNext(QUIET)).isEmpty();
            assertThat(c.maybeNext(QUIET)).isEmpty();

            StatusRuntimeException notAllClear = errorOf(() -> supervisor().closePossession(close(possession, false, "")));
            assertThat(notAllClear.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
            assertThat(GrpcErrors.reasonOf(notAllClear)).isEqualTo("POSSESSION_NOT_ALL_CLEAR");
            assertThat(GrpcErrors.metadataOf(notAllClear).get(FieldGrpcExceptionAdvice.PENDING_TEAMS).split(","))
                    .containsExactlyInAnyOrder(teamCode(shiftA), teamCode(shiftB), teamCode(shiftC));

            for (DeviceClient device : List.of(a, b, c)) {
                long clear = device.clearOfTrack(true);
                FieldCommand applied = device.nextEventResult();
                assertThat(applied.getEventResult().getSequence()).isEqualTo(clear);
                assertThat(applied.getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.APPLIED);
            }
            Possession closed = supervisor().closePossession(close(possession, false, ""));
            assertThat(closed.getStatus()).isEqualTo(Possession.Status.CLOSED);
            for (DeviceClient device : List.of(a, b, c)) {
                assertThat(device.outcome().getCode()).as("cerrar la posesion termina los streams con onCompleted").isEqualTo(Status.Code.OK);
            }

            // Lo que la noche conto hacia fuera, en orden, con el actor del token de cada llamada y la noche como correlacion.
            List<PublishedEvent> night = recorder.ofPossession(possession.getId());
            assertThat(night).extracting(PublishedEvent::name).containsExactly(
                    "possession.opened", "possession.evacuation-issued",
                    "possession.evacuation-acknowledged", "possession.evacuation-acknowledged",
                    "possession.clear-of-track", "possession.clear-of-track", "possession.clear-of-track",
                    "possession.closed");
            // Los dos acuses se mandan a la vez por dos streams: el orden entre ellos lo decide el bloqueo de la
            // posesion, y el segundo en tomarlo ya ve el primero. Las salidas de via van una a una.
            assertThat(night.subList(0, 2)).extracting(PublishedEvent::actor).containsExactly(SUPERVISOR, SUPERVISOR);
            assertThat(night.subList(2, 4)).extracting(PublishedEvent::actor).containsExactlyInAnyOrder("tecnico.a", "tecnico.b");
            assertThat(night.subList(4, 8)).extracting(PublishedEvent::actor).containsExactly("tecnico.a", "tecnico.b", "tecnico.c", SUPERVISOR);
            assertThat(night).extracting(PublishedEvent::correlationId).containsOnly(possession.getCode());
            assertThat(night.get(1).event().values()).containsEntry("commandId", evacuation.getCommandId()).containsEntry("sequence", evacuation.getSequence());
            PublishedEvent ackOfA = night.subList(2, 4).stream().filter(item -> "tecnico.a".equals(item.actor())).findFirst().orElseThrow();
            assertThat(ackOfA.event().values()).containsEntry("teamCode", teamCode(shiftA)).containsEntry("allAcknowledged", false);
            assertThat(ackOfA.event().values().get("pendingTeams")).asInstanceOf(InstanceOfAssertFactories.LIST)
                    .contains(teamCode(shiftC)).doesNotContain(teamCode(shiftA));
            assertThat(night.get(2).event().values().get("pendingTeams")).asInstanceOf(InstanceOfAssertFactories.LIST)
                    .as("el primero en tomar el bloqueo solo se ve a si mismo acusado").hasSize(2);
            assertThat(night.get(3).event().values().get("pendingTeams")).asInstanceOf(InstanceOfAssertFactories.LIST)
                    .as("el segundo ya ve el acuse del primero").containsExactly(teamCode(shiftC));
            assertThat(night.get(6).event().values()).containsEntry("allClear", true);
            assertThat(night.get(7).event().values()).containsEntry("forced", false).containsEntry("allClear", true);
        }
    }

    @Nested
    @DisplayName("Reanudacion")
    class Resumption {

        @Test
        void aDeviceThatLostCoverageGetsExactlyWhatItMissedInOrderAndThenLive() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            DeviceClient first = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), "dev-resume", shift, 0);
            first.awaitWelcome();
            say(possession.getId(), "uno");
            say(possession.getId(), "dos");
            say(possession.getId(), "tres");
            assertThat(sequences(first.nextN(3))).containsExactly(1L, 2L, 3L);

            first.cancel();
            say(possession.getId(), "cuatro");
            say(possession.getId(), "cinco");
            say(possession.getId(), "seis");

            DeviceClient resumed = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), "dev-resume", shift, 3);
            resumed.awaitWelcome();
            List<FieldCommand> missed = resumed.nextN(3);
            assertThat(sequences(missed)).containsExactly(4L, 5L, 6L);
            assertThat(missed).allMatch(FieldCommand::hasSupervisorMessage);
            say(possession.getId(), "siete");
            assertThat(resumed.next().getSequence()).isEqualTo(7L);
            assertThat(resumed.maybeNext(QUIET)).isEmpty();

            supervisor().closePossession(close(possession, true, "limpieza"));
            assertThat(resumed.outcome().getCode()).isEqualTo(Status.Code.OK);
        }
    }

    @Nested
    @DisplayName("Carrera entre la reproduccion y lo vivo")
    class CatchUpRace {

        @Test
        void commandsCommittedWhileTheReplayIsAboutToBeReadArriveOnceAndInOrder() throws Exception {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            say(possession.getId(), "antes");
            catchUpProbe.arm("dev-race-a", BlockingCatchUpProbe.Phase.AFTER_REGISTERED);

            DeviceClient device = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), "dev-race-a", shift, 0);
            catchUpProbe.awaitArrival();
            say(possession.getId(), "durante 1");
            say(possession.getId(), "durante 2");
            catchUpProbe.release();

            device.awaitWelcome();
            assertThat(sequences(device.nextN(3))).containsExactly(1L, 2L, 3L);
            assertThat(device.maybeNext(QUIET)).as("ni una copia de lo retenido").isEmpty();
            supervisor().closePossession(close(possession, true, "limpieza"));
        }

        @Test
        void aCommandCommittedAfterTheReplayAndBeforeTheFlushArrivesOnce() throws Exception {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            say(possession.getId(), "antes");
            catchUpProbe.arm("dev-race-b", BlockingCatchUpProbe.Phase.BEFORE_FLUSH);

            DeviceClient device = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), "dev-race-b", shift, 0);
            catchUpProbe.awaitArrival();
            say(possession.getId(), "entre la reproduccion y el volcado");
            catchUpProbe.release();

            device.awaitWelcome();
            assertThat(sequences(device.nextN(2))).containsExactly(1L, 2L);
            assertThat(device.maybeNext(QUIET)).isEmpty();
            supervisor().closePossession(close(possession, true, "limpieza"));
        }

        @Test
        void twoConcurrentEmittersReachALiveDeviceInStrictOrder() throws Exception {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            DeviceClient device = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), "dev-race-c", shift, 0);
            device.awaitWelcome();

            CountDownLatch go = new CountDownLatch(1);
            List<Thread> emitters = new ArrayList<>();
            for (int emitter = 0; emitter < 2; emitter++) {
                String name = "emisor-" + emitter;
                emitters.add(Thread.ofVirtual().start(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException ignored) {
                        return;
                    }
                    for (int index = 0; index < 20; index++) {
                        say(possession.getId(), name + " " + index);
                    }
                }));
            }
            go.countDown();
            for (Thread emitter : emitters) {
                emitter.join(TimeUnit.SECONDS.toMillis(30));
            }

            List<Long> expected = new ArrayList<>();
            for (long sequence = 1; sequence <= 40; sequence++) {
                expected.add(sequence);
            }
            assertThat(sequences(device.nextN(40))).containsExactlyElementsOf(expected);
            supervisor().closePossession(close(possession, true, "limpieza"));
        }
    }

    @Nested
    @DisplayName("Duplicados y atraso (SyncBufferedEvents)")
    class BufferedSync {

        /** Un reenvio por el canal vivo no se guarda dos veces, y un hueco solo se rellena por el canal del atraso. */
        @Test
        void aResentTaskEventIsStoredOnceAnsweredTwiceAndTheWatermarkIsContiguous() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            String deviceId = "dev-dup-" + UUID.randomUUID();
            DeviceClient device = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
            device.awaitWelcome();

            device.taskStarted(1, "o", "t1");
            device.taskStarted(2, "o", "t2");
            device.taskStarted(2, "o", "t2");
            List<FieldCommand> results = device.nextN(3);
            assertThat(results).allMatch(FieldCommand::hasEventResult);
            assertThat(results).extracting(command -> command.getEventResult().getOutcome()).containsOnly(EventResult.Outcome.PENDING_SYNC);
            assertThat(results).extracting(command -> command.getEventResult().getSequence()).containsExactly(1L, 2L, 2L);

            // #5 deja un hueco: por el canal vivo se rechaza; por el canal del atraso se guarda.
            device.taskStarted(5, "o", "t5");
            FieldCommand refused = device.nextEventResult();
            assertThat(refused.getEventResult().getSequence()).isEqualTo(5L);
            assertThat(refused.getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.REJECTED);
            assertThat(refused.getEventResult().getReason()).startsWith(TeamChannels.REASON_BACKLOG_PENDING);
            SyncClient sync = SyncClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift);
            sync.taskStarted(5, "o", "t5");
            SyncResult result = sync.finish();
            assertThat(result.getApplied()).isEqualTo(1);
            assertThat(result.getLastAppliedSequence()).as("con {1,2,5} la marca contigua es 2").isEqualTo(2L);
            assertThat(device.nextEventResult().getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.PENDING_SYNC);
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(eventRepository.findAll().stream().filter(event -> event.getDeviceId().equals(deviceId)).map(FieldEventRecord::getSequence))
                            .containsExactlyInAnyOrder(1L, 2L, 5L));

            device.cancel();
            DeviceClient reconnected = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
            assertThat(reconnected.awaitWelcome().getWelcome().getLastAppliedSequence()).isEqualTo(2L);
            supervisor().closePossession(close(possession, true, "limpieza"));
        }

        /**
         * El escenario de la regla de protocolo: un dispositivo pierde la cobertura con una orden de
         * desalojo en vuelo, trabaja a ciegas (empieza y acaba una tarea, acusa, sale de la via) y, al
         * volver, sube el atraso por SyncBufferedEvents en orden. Cada evento recibe su EventResult por
         * el canal vivo, el acuse y la salida cuentan para el tablero, y repetir la subida no guarda nada.
         */
        @Test
        void aDeviceBackFromACutUploadsItsBacklogInOrderAndGetsTheContiguousWatermark() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            String deviceId = "dev-backlog-" + UUID.randomUUID();
            DeviceClient device = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
            device.awaitWelcome();
            IssueCommandResponse evacuation = evacuate(possession.getId(), "key-backlog-" + deviceId, "tormenta");
            assertThat(device.nextCommand().getSequence()).isEqualTo(evacuation.getSequence());
            device.cancel();

            DeviceClient resumed = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, evacuation.getSequence());
            assertThat(resumed.awaitWelcome().getWelcome().getLastAppliedSequence()).isZero();
            SyncClient sync = SyncClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift);
            sync.taskStarted(1, "o", "t1");
            sync.taskCompleted(2, "o", "t1");
            sync.heartbeat("34.100");
            sync.ack(3, evacuation.getCommandId());
            sync.clearOfTrack(4, true);
            SyncResult result = sync.finish();

            assertThat(result.getApplied()).isEqualTo(4);
            assertThat(result.getDuplicates()).isZero();
            assertThat(result.getRejected()).isZero();
            assertThat(result.getLastAppliedSequence()).isEqualTo(4L);
            List<FieldCommand> results = resumed.nextN(4);
            assertThat(results).allMatch(FieldCommand::hasEventResult);
            // Los resultados salen de dos carriles: los eventos de trabajo los contesta la cola de
            // trabajo del dispositivo, y el acuse y la salida de via se contestan en linea para que
            // nunca esperen a una noche de tareas. Cada carril guarda su orden; entre si se cruzan.
            assertThat(results).extracting(command -> command.getEventResult().getSequence()).containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
            assertThat(results.stream().filter(command -> command.getEventResult().getOutcome() == EventResult.Outcome.PENDING_SYNC)
                    .map(command -> command.getEventResult().getSequence())).containsExactly(1L, 2L);
            assertThat(results.stream().filter(command -> command.getEventResult().getOutcome() == EventResult.Outcome.APPLIED)
                    .map(command -> command.getEventResult().getSequence())).containsExactly(3L, 4L);
            BoardClient board = BoardClient.watch(channel, TestTokens.supervisor(SUPERVISOR), possession.getId());
            PossessionBoard state = board.awaitBoard("the ack and the clear-of-track of the backlog",
                    candidate -> candidate.getAllClear() && candidate.getCommandsCount() == 1 && candidate.getCommands(0).getAckedByCount() == 1);
            assertThat(state.getCommands(0).getAckedBy(0)).isEqualTo(teamCode(shift));
            board.cancel();

            SyncClient again = SyncClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift);
            again.taskStarted(1, "o", "t1");
            again.taskCompleted(2, "o", "t1");
            again.ack(3, evacuation.getCommandId());
            again.clearOfTrack(4, true);
            SyncResult repeated = again.finish();
            assertThat(repeated.getApplied()).isZero();
            assertThat(repeated.getDuplicates()).isEqualTo(4);
            assertThat(repeated.getLastAppliedSequence()).isEqualTo(4L);
            assertThat(resumed.nextN(4)).extracting(command -> command.getEventResult().getOutcome())
                    .containsExactly(EventResult.Outcome.PENDING_SYNC, EventResult.Outcome.PENDING_SYNC, EventResult.Outcome.APPLIED,
                            EventResult.Outcome.APPLIED);
            supervisor().closePossession(close(possession, false, ""));
            assertThat(resumed.outcome().getCode()).isEqualTo(Status.Code.OK);
        }

        /** Por el canal vivo solo pasa el evento de trabajo que sigue a la marca; acuses y salidas de via pasan siempre. */
        @Test
        void aWorkEventWithAGapOnTheTeamChannelIsRefusedUntilTheBacklogIsUploaded() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            String deviceId = "dev-gap-" + UUID.randomUUID();
            DeviceClient device = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
            device.awaitWelcome();

            device.taskStarted(1, "o", "t1");
            assertThat(device.nextEventResult().getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.PENDING_SYNC);
            device.taskStarted(3, "o", "t3");
            FieldCommand refused = device.nextEventResult();
            assertThat(refused.getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.REJECTED);
            assertThat(refused.getEventResult().getReason()).startsWith(TeamChannels.REASON_BACKLOG_PENDING);
            device.send(TeamMessage.newBuilder().setDeviceId(deviceId).setSequence(4)
                    .setClearOfTrack(com.alejandro.mtofield.grpc.v1.ClearOfTrack.newBuilder().setEarthingRemoved(true)).build());
            assertThat(device.nextEventResult().getEventResult().getOutcome()).as("una salida de via pasa con hueco").isEqualTo(EventResult.Outcome.APPLIED);

            SyncClient sync = SyncClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift);
            sync.taskStarted(2, "o", "t2");
            sync.taskStarted(3, "o", "t3");
            SyncResult result = sync.finish();
            assertThat(result.getApplied()).isEqualTo(2);
            assertThat(result.getLastAppliedSequence()).as("1,2,3 subidos y 4 ya estaba").isEqualTo(4L);
            assertThat(device.nextN(2)).extracting(command -> command.getEventResult().getSequence()).containsExactly(2L, 3L);

            device.taskStarted(5, "o", "t5");
            assertThat(device.nextEventResult().getEventResult().getOutcome()).as("sin atraso, el canal vivo vuelve a admitir trabajo")
                    .isEqualTo(EventResult.Outcome.PENDING_SYNC);
            supervisor().closePossession(close(possession, true, "limpieza"));
        }

        @Test
        void theSyncStreamNeedsAJoinOfAnOpenShiftFirstAndTheBacklogInIncreasingOrder() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);

            SyncClient withoutJoin = SyncClient.open(channel, TestTokens.technician(TECHNICIAN), "dev-nojoin");
            withoutJoin.taskStarted(1, "o", "t1");
            assertThat(withoutJoin.finishExpectingAnError().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
            assertThat(GrpcErrors.reasonOf(withoutJoin.outcomeError())).isEqualTo(TeamChannels.REASON_JOIN_REQUIRED);

            SyncClient empty = SyncClient.open(channel, TestTokens.technician(TECHNICIAN), "dev-empty");
            assertThat(empty.finishExpectingAnError().getCode()).as("despedirse sin Join").isEqualTo(Status.Code.FAILED_PRECONDITION);

            SyncClient unknownShift = SyncClient.join(channel, TestTokens.technician(TECHNICIAN), "dev-unknown", UUID.randomUUID());
            assertThat(unknownShift.outcome(Duration.ofSeconds(10)).getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
            assertThat(GrpcErrors.reasonOf(unknownShift.outcomeError())).isEqualTo(TeamChannels.REASON_SHIFT_NOT_IN_OPEN_POSSESSION);

            SyncClient outOfOrder = SyncClient.join(channel, TestTokens.technician(TECHNICIAN), "dev-order-" + UUID.randomUUID(), shift);
            outOfOrder.taskStarted(5, "o", "t5");
            outOfOrder.taskStarted(3, "o", "t3");
            assertThat(outOfOrder.outcome(Duration.ofSeconds(10)).getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
            assertThat(GrpcErrors.reasonOf(outOfOrder.outcomeError())).isEqualTo(SyncSessions.REASON_OUT_OF_ORDER);

            SyncClient technicianWithoutRole = SyncClient.open(channel, TestTokens.supervisorWithoutTeam(SUPERVISOR), "dev-norole");
            assertThat(technicianWithoutRole.finishExpectingAnError().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
            supervisor().closePossession(close(possession, true, "limpieza"));
        }
    }

    @Nested
    @DisplayName("Sustitucion")
    class Supersession {

        @Test
        void aSecondStreamOfTheSameDeviceSupersedesTheFirst() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            DeviceClient first = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), "dev-twice", shift, 0);
            first.awaitWelcome();

            DeviceClient second = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), "dev-twice", shift, 0);
            second.awaitWelcome();

            assertThat(first.outcome().getCode()).isEqualTo(Status.Code.ABORTED);
            assertThat(GrpcErrors.reasonOf(first.outcomeError())).isEqualTo(DeviceStream.REASON_SUPERSEDED);
            say(possession.getId(), "solo al segundo");
            assertThat(second.next().getSupervisorMessage().getText()).isEqualTo("solo al segundo");
            assertThat(first.pending()).isZero();
            supervisor().closePossession(close(possession, true, "limpieza"));
        }
    }

    @Nested
    @DisplayName("Tablero")
    class Board {

        @Test
        void theBoardFollowsLivenessAcksSentAndQueuedAndEndsWithThePossession() {
            UUID shiftA = UUID.randomUUID();
            UUID shiftB = UUID.randomUUID();
            UUID shiftC = UUID.randomUUID();
            Possession possession = open(shiftA, shiftB, shiftC);
            BoardClient watcher = BoardClient.watch(channel, TestTokens.supervisor(SUPERVISOR), possession.getId());

            PossessionBoard initial = watcher.next();
            assertThat(initial.getTeamsList()).extracting(TeamState::getTeamCode)
                    .containsExactlyInAnyOrder(teamCode(shiftA), teamCode(shiftB), teamCode(shiftC));
            assertThat(initial.getTeamsList()).extracting(TeamState::getLiveness).containsOnly(TeamState.Liveness.DISCONNECTED);
            assertThat(initial.getCommandsList()).isEmpty();
            assertThat(initial.getAllClear()).isFalse();
            assertThat(initial.getEndsAt()).isEqualTo(possession.getEndsAt());

            DeviceClient a = DeviceClient.join(channel, TestTokens.technician("tecnico.a"), "dev-board-a", shiftA, 0);
            DeviceClient b = DeviceClient.join(channel, TestTokens.technician("tecnico.b"), "dev-board-b", shiftB, 0);
            DeviceClient c = DeviceClient.join(channel, TestTokens.technician("tecnico.c"), "dev-board-c", shiftC, 0);
            a.awaitWelcome();
            b.awaitWelcome();
            c.awaitWelcome();
            watcher.awaitBoard("los tres conectados", board -> board.getTeamsList().stream()
                    .allMatch(team -> team.getLiveness() == TeamState.Liveness.CONNECTED));

            a.heartbeat("12.345", 77, -90);
            PossessionBoard withKp = watcher.awaitBoard("el kp del equipo A", board -> team(board, shiftA).getKp().equals("12.345"));
            assertThat(team(withKp, shiftA).getBatteryPct()).isEqualTo(77);
            assertThat(team(withKp, shiftA).hasLastSeen()).isTrue();

            IssueCommandResponse evacuation = evacuate(possession.getId(), "evac-board", "Fin de la ventana");
            for (DeviceClient device : List.of(a, b, c)) {
                assertThat(device.nextCommand().getCommandId()).isEqualTo(evacuation.getCommandId());
            }
            PossessionBoard sent = watcher.awaitBoard("enviada a los tres", board -> board.getCommandsCount() == 1
                    && board.getCommands(0).getSentToCount() == 3);
            assertThat(sent.getCommands(0).getCommandId()).isEqualTo(evacuation.getCommandId());
            assertThat(sent.getCommands(0).getKind()).isEqualTo("EVACUATE_NOW");
            assertThat(sent.getCommands(0).getAckedByList()).isEmpty();
            assertThat(sent.getCommands(0).getPendingList()).containsExactlyInAnyOrder(teamCode(shiftA), teamCode(shiftB), teamCode(shiftC));
            assertThat(sent.getCommands(0).getQueuedForList()).isEmpty();

            a.ack(evacuation.getCommandId());
            b.ack(evacuation.getCommandId());
            PossessionBoard acked = watcher.awaitBoard("A y B han acusado", board -> board.getCommands(0).getAckedByCount() == 2);
            assertThat(acked.getCommands(0).getAckedByList()).containsExactlyInAnyOrder(teamCode(shiftA), teamCode(shiftB));
            assertThat(acked.getCommands(0).getPendingList()).containsExactly(teamCode(shiftC));
            assertThat(acked.getCommands(0).getSentToList()).containsExactly(teamCode(shiftC));

            c.cancel();
            PossessionBoard queued = watcher.awaitBoard("C sin stream: en cola", board -> board.getCommands(0).getQueuedForCount() == 1);
            assertThat(queued.getCommands(0).getQueuedForList()).containsExactly(teamCode(shiftC));
            assertThat(queued.getCommands(0).getSentToList()).isEmpty();
            assertThat(team(queued, shiftC).getLiveness()).isEqualTo(TeamState.Liveness.DISCONNECTED);
            assertThat(team(queued, shiftA).getLiveness()).isEqualTo(TeamState.Liveness.CONNECTED);

            DeviceClient cAgain = DeviceClient.join(channel, TestTokens.technician("tecnico.c"), "dev-board-c", shiftC, evacuation.getSequence());
            cAgain.awaitWelcome();
            PossessionBoard resumed = watcher.awaitBoard("C vuelve diciendo que ya tiene la orden: enviada, no en cola",
                    board -> board.getCommands(0).getSentToCount() == 1 && team(board, shiftC).getLiveness() == TeamState.Liveness.CONNECTED);
            assertThat(resumed.getCommands(0).getSentToList()).containsExactly(teamCode(shiftC));
            assertThat(resumed.getCommands(0).getQueuedForList()).isEmpty();
            assertThat(cAgain.maybeNext(QUIET)).as("y no la recibe otra vez").isEmpty();

            a.clearOfTrack(true);
            b.clearOfTrack(false);
            PossessionBoard twoClear = watcher.awaitBoard("A y B fuera de via", board -> team(board, shiftA).getClearOfTrack() && team(board, shiftB).getClearOfTrack());
            assertThat(twoClear.getAllClear()).isFalse();

            supervisor().closePossession(close(possession, true, "C no responde"));
            assertThat(watcher.outcome().getCode()).as("cerrar la posesion termina el tablero con onCompleted").isEqualTo(Status.Code.OK);
            assertThat(a.outcome().getCode()).isEqualTo(Status.Code.OK);
        }

        @Test
        void aSlowWatcherGetsTheLatestBoardNotABacklog() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            DeviceClient device = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), "dev-board-slow", shift, 0);
            device.awaitWelcome();
            BoardClient slow = BoardClient.watchSlowly(channel, TestTokens.supervisor(SUPERVISOR), possession.getId());
            PossessionBoard first = slow.next();
            assertThat(first.getCommandsList()).isEmpty();

            for (int index = 0; index < 20; index++) {
                evacuate(possession.getId(), "slow-" + index, "cambio " + index);
            }
            assertThat(device.nextN(20)).hasSize(20);
            assertThat(slow.maybeNext(QUIET)).as("sin pedir, no llega nada").isEmpty();

            slow.request(1);
            PossessionBoard latest = slow.awaitBoard("el ultimo estado de golpe", board -> board.getCommandsCount() == 20);
            assertThat(latest.getVersion()).isGreaterThan(first.getVersion());
            assertThat(slow.pending()).as("ni un tablero intermedio acumulado").isZero();
            slow.request(1);
            assertThat(slow.maybeNext(QUIET).map(PossessionBoard::getCommandsCount).orElse(20)).isEqualTo(20);

            slow.cancel();
            supervisor().closePossession(close(possession, true, "limpieza"));
        }

        @Test
        void watchingAClosedOrUnknownPossession() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            supervisor().closePossession(close(possession, true, "ya cerrada"));

            BoardClient watcher = BoardClient.watch(channel, TestTokens.supervisor(SUPERVISOR), possession.getId());
            PossessionBoard last = watcher.next();
            assertThat(last.getTeamsList()).extracting(TeamState::getTeamCode).containsExactly(teamCode(shift));
            assertThat(watcher.outcome().getCode()).isEqualTo(Status.Code.OK);

            BoardClient unknown = BoardClient.watch(channel, TestTokens.supervisor(SUPERVISOR), UUID.randomUUID().toString());
            assertThat(unknown.outcome().getCode()).isEqualTo(Status.Code.NOT_FOUND);
        }

        private TeamState team(PossessionBoard board, UUID shiftId) {
            return board.getTeamsList().stream().filter(team -> team.getShiftId().equals(shiftId.toString())).findFirst().orElseThrow();
        }
    }

    @Nested
    @DisplayName("Contrapresion de la cola de trabajo")
    class Backpressure {

        @Test
        void aDeviceThatFloodsTaskEventsIsClosedWithResourceExhaustedAndLosesNothing() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            String deviceId = "dev-flood-" + UUID.randomUUID();
            synchronizer.block(deviceId);
            try {
                DeviceClient device = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
                device.awaitWelcome();

                for (int index = 0; index < 80; index++) {
                    device.taskStarted("o", "t" + index);
                }

                assertThat(device.outcome(Duration.ofSeconds(30)).getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                assertThat(GrpcErrors.reasonOf(device.outcomeError())).isEqualTo(TeamChannels.REASON_WORK_QUEUE_FULL);
                long persisted = eventRepository.findAll().stream().filter(event -> event.getDeviceId().equals(deviceId)).count();
                assertThat(persisted).as("la cola es de 64 y uno se esta procesando").isBetween(65L, 66L);

                DeviceClient reconnected = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
                assertThat(reconnected.awaitWelcome().getWelcome().getLastAppliedSequence()).as("todo lo aceptado esta guardado").isEqualTo(persisted);
            } finally {
                synchronizer.release(deviceId);
            }
            supervisor().closePossession(close(possession, true, "limpieza"));
        }
    }

    /**
     * La persona y su equipo, ligados por el claim de grupos: un tercer contexto con
     * app.field.team-binding.enabled=true. Con el cliente de mantenimiento apagado el codigo del
     * equipo de un turno es el sintetico (T- y cuatro hex del id), que es lo que llevan los tokens.
     */
    @Nested
    @DisplayName("Equipo del token")
    @TestPropertySource(properties = "app.field.team-binding.enabled=true")
    class TeamOfTheToken {

        @Test
        void aTechnicianJoinsOnlyTheShiftOfTheirTeamAndASupervisorJoinsAnyTeam() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            String deviceOfTheTeam = "dev-team-" + UUID.randomUUID();

            DeviceClient withoutGroups = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), "dev-nogroup", shift, 0);
            assertThat(withoutGroups.outcome().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(GrpcErrors.reasonOf(withoutGroups.outcomeError())).isEqualTo(TeamBinding.REASON_TEAM_NOT_ALLOWED);
            assertThat(GrpcErrors.metadataOf(withoutGroups.outcomeError())).containsEntry(TeamBinding.TEAM_CODE, teamCode(shift));

            DeviceClient otherTeam = DeviceClient.join(channel, TestTokens.technicianOfTeams(TECHNICIAN, "EQ-OTRO"), "dev-other", shift, 0);
            assertThat(otherTeam.outcome().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);

            DeviceClient ofTheTeam = DeviceClient.join(channel, TestTokens.technicianOfTeams(TECHNICIAN, "EQ-OTRO", teamCode(shift)), deviceOfTheTeam, shift, 0);
            ofTheTeam.awaitWelcome();
            DeviceClient asSupervisor = DeviceClient.join(channel, TestTokens.supervisor(SUPERVISOR), "dev-super", shift, 0);
            asSupervisor.awaitWelcome();

            SyncClient syncOfAnother = SyncClient.join(channel, TestTokens.technicianOfTeams(TECHNICIAN, "EQ-OTRO"), "dev-other", shift);
            assertThat(syncOfAnother.outcome(Duration.ofSeconds(10)).getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(GrpcErrors.reasonOf(syncOfAnother.outcomeError())).isEqualTo(TeamBinding.REASON_TEAM_NOT_ALLOWED);
            SyncClient syncOfTheTeam = SyncClient.join(channel, TestTokens.technicianOfTeams(TECHNICIAN, teamCode(shift)), deviceOfTheTeam, shift);
            syncOfTheTeam.taskStarted(1, "o", "t1");
            assertThat(syncOfTheTeam.finish().getApplied()).isEqualTo(1);

            supervisor().closePossession(close(possession, true, "limpieza"));
            assertThat(ofTheTeam.outcome().getCode()).isEqualTo(Status.Code.OK);
        }
    }

    @Nested
    @DisplayName("Caducidad del token")
    class TokenExpiry {

        private String technicianExpiringIn(Duration ttl) {
            return TestTokens.mint(TECHNICIAN, List.of(TestTokens.AUDIENCE), List.of("field-team"), List.of("mto-field-technician"), Instant.now().plus(ttl));
        }

        /** El JWT se valida al abrir; el barrido cierra el stream cuando caduca, y la reanudacion con un token nuevo no pierde nada. */
        @Test
        void aTeamChannelWhoseTokenExpiresIsClosedWithUnauthenticatedAndResumesWithAFreshOne() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            String deviceId = "dev-expiring-" + UUID.randomUUID();
            DeviceClient device = DeviceClient.join(channel, technicianExpiringIn(Duration.ofSeconds(2)), deviceId, shift, 0);
            device.awaitWelcome();
            say(possession.getId(), "antes de caducar");
            assertThat(device.next().getSupervisorMessage().getText()).isEqualTo("antes de caducar");

            Status ended = device.outcome(Duration.ofSeconds(10));
            assertThat(ended.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
            assertThat(GrpcErrors.reasonOf(device.outcomeError())).isEqualTo(TokenExpirySweeper.REASON_TOKEN_EXPIRED);
            assertThat(GrpcErrors.metadataOf(device.outcomeError())).containsKey(TokenExpirySweeper.EXPIRED_AT);
            say(possession.getId(), "mientras renueva");

            DeviceClient resumed = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, 1);
            resumed.awaitWelcome();
            assertThat(resumed.next().getSupervisorMessage().getText()).isEqualTo("mientras renueva");
            supervisor().closePossession(close(possession, true, "limpieza"));
            assertThat(resumed.outcome().getCode()).isEqualTo(Status.Code.OK);
        }

        @Test
        void aBoardWatcherWhoseTokenExpiresIsClosedWithUnauthenticated() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            String supervisorExpiring = TestTokens.mint(SUPERVISOR, List.of(TestTokens.AUDIENCE), List.of("field-team", "field-supervise"),
                    List.of("mto-field-supervisor"), Instant.now().plusSeconds(2));
            BoardClient board = BoardClient.watch(channel, supervisorExpiring, possession.getId());
            assertThat(board.next().getTeamsCount()).isEqualTo(1);

            Status ended = board.outcome(Duration.ofSeconds(10));
            assertThat(ended.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
            assertThat(GrpcErrors.reasonOf(board.outcomeError())).isEqualTo(TokenExpirySweeper.REASON_TOKEN_EXPIRED);
            supervisor().closePossession(close(possession, true, "limpieza"));
        }
    }

    /**
     * La fase 2 de punta a punta: el mismo contexto con el cliente de mantenimiento encendido (el
     * sincronizador real, la cola de trabajo real) y mto-maintenance sustituido por un doble. Lo que
     * el doble contesta es lo que el dispositivo recibe como resultado, y lo que queda en la base.
     */
    @Nested
    @DisplayName("Sincronizacion con mto-maintenance")
    @TestPropertySource(properties = {"app.maintenance.enabled=true", "app.maintenance.sync-retry.enabled=false"})
    class MaintenanceSynchronization {

        @MockitoBean
        private MaintenanceClient maintenance;

        @Autowired
        private FieldEventSyncRetryService retryService;

        @Autowired
        private FieldEventService events;

        private final UUID orderId = UUID.randomUUID();
        private final UUID taskId = UUID.randomUUID();

        @BeforeEach
        void maintenanceKnowsEveryShift() {
            when(maintenance.isEnabled()).thenReturn(true);
            when(maintenance.findShift(any())).thenAnswer(invocation -> {
                UUID shiftId = invocation.getArgument(0);
                return Optional.of(new ShiftSnapshot(shiftId, "SH-" + shiftId.toString().substring(0, 4), LocalDate.now(ZoneOffset.UTC), "IN_PROGRESS",
                        "EQ-" + shiftId.toString().substring(0, 4).toUpperCase(), "Equipo", Instant.now().minusSeconds(3600),
                        Instant.now().plusSeconds(8 * 3600), List.of(1L)));
            });
            // Lo que no sea de este test (eventos vencidos de otros escenarios que el reintento recoja) tambien se contesta.
            when(maintenance.startTask(any(), any(), any(), any())).thenAnswer(invocation -> task(invocation.getArgument(1), "IN_PROGRESS"));
            when(maintenance.completeTask(any(), any(), any())).thenAnswer(invocation -> task(invocation.getArgument(1), "COMPLETED"));
        }

        private TaskSnapshot task(UUID id, String status) {
            return new TaskSnapshot(id, orderId, status, null, TECHNICIAN);
        }

        private FieldEventRecord stored(String deviceId, long sequence) {
            return eventRepository.findByDeviceIdAndSequence(deviceId, sequence).orElseThrow();
        }

        @Test
        void aStartAndACompletionArePassedOnWithTheShiftAndThePersonAndAnsweredApplied() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            assertThat(possession.getStatus()).isEqualTo(Possession.Status.OPEN);
            verify(maintenance).findShift(shift);
            String deviceId = "dev-sync-" + UUID.randomUUID();
            DeviceClient device = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
            device.awaitWelcome();

            long started = device.taskStarted(orderId.toString(), taskId.toString());
            FieldCommand startResult = device.nextEventResult();
            assertThat(startResult.getEventResult().getSequence()).isEqualTo(started);
            assertThat(startResult.getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.APPLIED);
            verify(maintenance).startTask(orderId, taskId, shift, TECHNICIAN);
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(stored(deviceId, started).getSyncStatus()).isEqualTo(FieldEventSyncStatus.SYNCED));

            long completed = device.taskCompleted(orderId.toString(), taskId.toString());
            FieldCommand completeResult = device.nextEventResult();
            assertThat(completeResult.getEventResult().getSequence()).isEqualTo(completed);
            assertThat(completeResult.getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.APPLIED);
            ArgumentCaptor<CompleteTaskCommand> command = ArgumentCaptor.forClass(CompleteTaskCommand.class);
            verify(maintenance).completeTask(eq(orderId), eq(taskId), command.capture());
            assertThat(command.getValue().shiftId()).isEqualTo(shift);
            assertThat(command.getValue().workComplete()).isTrue();
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(stored(deviceId, completed).getSyncStatus()).isEqualTo(FieldEventSyncStatus.SYNCED));

            supervisor().closePossession(close(possession, true, "limpieza"));
        }

        /** Un no de mantenimiento llega al dispositivo con su codigo, queda en la base y un reenvio no vuelve a preguntar. */
        @Test
        void aRejectionOfMaintenanceReachesTheDeviceWithItsCodeAndIsFinal() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            String deviceId = "dev-rej-" + UUID.randomUUID();
            when(maintenance.startTask(orderId, taskId, shift, TECHNICIAN))
                    .thenThrow(new MaintenanceRejectedException("start task", 409, "SHF-001", "Shift does not cover track 7"));
            DeviceClient device = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
            device.awaitWelcome();

            long sequence = device.taskStarted(orderId.toString(), taskId.toString());
            FieldCommand result = device.nextEventResult();

            assertThat(result.getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.REJECTED);
            assertThat(result.getEventResult().getReason()).isEqualTo("SHF-001: Shift does not cover track 7");
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                FieldEventRecord event = stored(deviceId, sequence);
                assertThat(event.getSyncStatus()).isEqualTo(FieldEventSyncStatus.REJECTED);
                assertThat(event.getLastError()).isEqualTo("SHF-001: Shift does not cover track 7");
            });

            device.cancel();
            DeviceClient reconnected = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, sequence);
            reconnected.awaitWelcome();
            reconnected.taskStarted(sequence, orderId.toString(), taskId.toString());
            FieldCommand resent = reconnected.nextEventResult();
            assertThat(resent.getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.REJECTED);
            assertThat(resent.getEventResult().getReason()).isEqualTo("SHF-001: Shift does not cover track 7");
            verify(maintenance, times(1)).startTask(orderId, taskId, shift, TECHNICIAN);
            supervisor().closePossession(close(possession, true, "limpieza"));
        }

        /** Mantenimiento caido: el dispositivo oye PENDING_SYNC, el evento queda FAILED, y el reintento le lleva el resultado cuando vuelve. */
        @Test
        void whenMaintenanceIsDownTheDeviceHearsPendingSyncAndTheRetryDeliversTheOutcomeLater() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            String deviceId = "dev-down-" + UUID.randomUUID();
            when(maintenance.startTask(orderId, taskId, shift, TECHNICIAN))
                    .thenThrow(new MaintenanceUnavailableException("mto-maintenance is unavailable for 'start task'"))
                    .thenReturn(task(taskId, "IN_PROGRESS"));
            DeviceClient device = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
            device.awaitWelcome();

            long sequence = device.taskStarted(orderId.toString(), taskId.toString());
            assertThat(device.nextEventResult().getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.PENDING_SYNC);
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                FieldEventRecord event = stored(deviceId, sequence);
                assertThat(event.getSyncStatus()).isEqualTo(FieldEventSyncStatus.FAILED);
                assertThat(event.getSyncAttempts()).isEqualTo(1);
                assertThat(event.getLastError()).contains("unavailable");
                assertThat(event.getNextAttemptAt()).isAfter(Instant.now());
            });

            // Vence el siguiente intento y mantenimiento ha vuelto.
            events.markFailed(stored(deviceId, sequence).getId(), "due now", Instant.now().minusSeconds(1));
            retryService.retryDue();

            FieldCommand result = device.nextEventResult();
            assertThat(result.getEventResult().getSequence()).isEqualTo(sequence);
            assertThat(result.getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.APPLIED);
            assertThat(stored(deviceId, sequence).getSyncStatus()).isEqualTo(FieldEventSyncStatus.SYNCED);
            verify(maintenance, times(2)).startTask(orderId, taskId, shift, TECHNICIAN);
            supervisor().closePossession(close(possession, true, "limpieza"));
        }

        /** Un turno que mantenimiento no conoce no abre nada, y con mantenimiento caido la llamada es UNAVAILABLE. */
        @Test
        void openingAPossessionAsksMaintenanceForEveryShift() {
            UUID unknown = UUID.randomUUID();
            UUID unreachable = UUID.randomUUID();
            when(maintenance.findShift(unknown)).thenReturn(Optional.empty());
            when(maintenance.findShift(unreachable)).thenThrow(new MaintenanceUnavailableException("mto-maintenance is unavailable for 'find shift'"));

            StatusRuntimeException notFound = errorOf(() -> open(unknown));
            assertThat(notFound.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);

            StatusRuntimeException down = errorOf(() -> open(unreachable));
            assertThat(down.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
            assertThat(GrpcErrors.reasonOf(down)).isEqualTo(MaintenanceUnavailableException.REASON);
        }
    }

    private FieldServiceGrpc.FieldServiceBlockingStub supervisor() {
        return withToken(TestTokens.supervisor(SUPERVISOR));
    }

    private Possession open(UUID... shiftIds) {
        OpenPossessionRequest.Builder request = OpenPossessionRequest.newBuilder();
        for (UUID shiftId : shiftIds) {
            request.addShiftIds(shiftId.toString());
        }
        return supervisor().openPossession(request.build());
    }

    private static ClosePossessionRequest close(Possession possession, boolean force, String reason) {
        return close(possession.getId(), force, reason);
    }

    private static ClosePossessionRequest close(String possessionId, boolean force, String reason) {
        return ClosePossessionRequest.newBuilder().setPossessionId(possessionId).setForce(force).setReason(reason).build();
    }

    private IssueCommandResponse evacuate(String possessionId, String key, String reason) {
        return supervisor().issueCommand(IssueCommandRequest.newBuilder().setPossessionId(possessionId).setIdempotencyKey(key)
                .setEvacuateNow(EvacuateNow.newBuilder().setReason(reason)).build());
    }

    private IssueCommandResponse say(String possessionId, String text) {
        return supervisor().issueCommand(IssueCommandRequest.newBuilder().setPossessionId(possessionId)
                .setSupervisorMessage(SupervisorMessage.newBuilder().setAuthor(SUPERVISOR).setText(text)).build());
    }

    /** El equipo sintetico del cliente de mantenimiento apagado. */
    private static String teamCode(UUID shiftId) {
        return "T-" + shiftId.toString().substring(0, 4).toUpperCase();
    }

    private static List<Long> sequences(List<FieldCommand> commands) {
        return commands.stream().map(FieldCommand::getSequence).toList();
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
        return errorOf(call).getStatus().getCode();
    }

    private static StatusRuntimeException errorOf(ThrowingCall call) {
        Throwable thrown = catchThrowable(call::run);
        assertThat(thrown).as("la llamada tenia que fallar").isInstanceOf(StatusRuntimeException.class);
        return (StatusRuntimeException) thrown;
    }

    @FunctionalInterface
    private interface ThrowingCall {
        void run() throws Exception;
    }

    /** Un evento publicado tal cual, con quien lo publico y bajo que correlacion, segun el contexto del hilo que lo publico. */
    record PublishedEvent(DomainEvent event, String actor, String correlationId) {
        String name() {
            return event.entityName() + "." + event.eventName();
        }
    }

    static final class RecordingDomainEventPublisher implements DomainEventPublisher {

        final List<PublishedEvent> published = new CopyOnWriteArrayList<>();
        private final CurrentUserService currentUser;

        RecordingDomainEventPublisher(CurrentUserService currentUser) {
            this.currentUser = currentUser;
        }

        @Override
        public void publish(DomainEvent event) {
            publish(UUID.randomUUID(), event);
        }

        @Override
        public void publish(UUID operationId, DomainEvent event) {
            published.add(new PublishedEvent(event, currentUser.getUsername().orElse("system"), MessagingCorrelation.current()));
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        List<PublishedEvent> ofPossession(String possessionId) {
            return published.stream().filter(item -> item.event().entityId().equals(possessionId)).toList();
        }
    }
}
