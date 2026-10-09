package com.alejandro.mtofield.infrastructure.grpc;

import com.alejandro.mtofield.application.dto.SyncJob;
import com.alejandro.mtofield.application.service.FieldEventSynchronizer;
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
import com.alejandro.mtofield.grpc.v1.WatchPossessionBoardRequest;
import com.alejandro.mtofield.infrastructure.grpc.advice.FieldGrpcExceptionAdvice;
import com.alejandro.mtofield.infrastructure.grpc.stream.CatchUpProbe;
import com.alejandro.mtofield.infrastructure.grpc.stream.DeviceStream;
import com.alejandro.mtofield.infrastructure.grpc.stream.TeamChannels;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldEventRecord;
import com.alejandro.mtofield.infrastructure.persistence.repository.FieldEventRepository;
import com.alejandro.mtofield.support.BoardClient;
import com.alejandro.mtofield.support.DeviceClient;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.grpc.test.autoconfigure.AutoConfigureTestGrpcTransport;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.grpc.client.GrpcChannelFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

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

        @Bean
        @Primary
        GatedSynchronizer gatedSynchronizer(@Qualifier("pendingSyncEventSynchronizer") FieldEventSynchronizer delegate) {
            return new GatedSynchronizer(delegate);
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

        private final FieldEventSynchronizer delegate;
        private final Map<String, CountDownLatch> gates = new ConcurrentHashMap<>();

        GatedSynchronizer(FieldEventSynchronizer delegate) {
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
        public void process(SyncJob job) {
            CountDownLatch gate = gates.get(job.context().deviceId());
            if (gate != null) {
                try {
                    gate.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            delegate.process(job);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registerPostgreSQLProperties(registry);
    }

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
    @DisplayName("Duplicados y marca de agua")
    class Duplicates {

        @Test
        void aResentTaskEventIsStoredOnceAnsweredTwiceAndTheWatermarkIsContiguous() {
            UUID shift = UUID.randomUUID();
            Possession possession = open(shift);
            String deviceId = "dev-dup-" + UUID.randomUUID();
            DeviceClient device = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
            device.awaitWelcome();

            device.taskStarted(1, "o", "t1");
            device.taskStarted(2, "o", "t2");
            device.taskStarted(5, "o", "t5");
            device.taskStarted(5, "o", "t5");

            List<FieldCommand> results = device.nextN(4);
            assertThat(results).allMatch(FieldCommand::hasEventResult);
            assertThat(results).extracting(command -> command.getEventResult().getOutcome()).containsOnly(EventResult.Outcome.PENDING_SYNC);
            assertThat(results).extracting(command -> command.getEventResult().getSequence()).containsExactlyInAnyOrder(1L, 2L, 5L, 5L);
            assertThat(sequences(results)).isSorted();
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(eventRepository.findAll().stream().filter(event -> event.getDeviceId().equals(deviceId)).map(FieldEventRecord::getSequence))
                            .containsExactlyInAnyOrder(1L, 2L, 5L));

            device.cancel();
            DeviceClient reconnected = DeviceClient.join(channel, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
            assertThat(reconnected.awaitWelcome().getWelcome().getLastAppliedSequence()).as("con {1,2,5} la marca contigua es 2").isEqualTo(2L);
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
}
