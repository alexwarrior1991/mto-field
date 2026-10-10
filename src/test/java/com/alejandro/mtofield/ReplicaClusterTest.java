package com.alejandro.mtofield;

import com.alejandro.mtofield.grpc.v1.ClosePossessionRequest;
import com.alejandro.mtofield.grpc.v1.CommandState;
import com.alejandro.mtofield.grpc.v1.EvacuateNow;
import com.alejandro.mtofield.grpc.v1.EventResult;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.grpc.v1.FieldServiceGrpc;
import com.alejandro.mtofield.grpc.v1.IssueCommandRequest;
import com.alejandro.mtofield.grpc.v1.IssueCommandResponse;
import com.alejandro.mtofield.grpc.v1.OpenPossessionRequest;
import com.alejandro.mtofield.grpc.v1.Possession;
import com.alejandro.mtofield.grpc.v1.PossessionBoard;
import com.alejandro.mtofield.grpc.v1.SupervisorMessage;
import com.alejandro.mtofield.grpc.v1.TeamState;
import com.alejandro.mtofield.infrastructure.grpc.GrpcErrors;
import com.alejandro.mtofield.infrastructure.grpc.stream.DeviceStream;
import com.alejandro.mtofield.infrastructure.grpc.stream.DeviceStreamRegistry;
import com.alejandro.mtofield.application.service.RemoteDeviceStates;
import com.alejandro.mtofield.support.BoardClient;
import com.alejandro.mtofield.support.DeviceClient;
import com.alejandro.mtofield.support.LocalReplicaBusConfiguration;
import com.alejandro.mtofield.support.LocalReplicaHub;
import com.alejandro.mtofield.support.PostgreSQLTestContainer;
import com.alejandro.mtofield.support.TestJwtDecoderConfiguration;
import com.alejandro.mtofield.support.TestTokens;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Dos replicas (dos contextos de Spring en esta JVM, cada una con su Netty en un puerto libre)
 * sobre la misma base, unidas por el bus en memoria de {@link LocalReplicaHub}: el responsable y
 * su tablero en A, el dispositivo en B. Lo que la fase 4 promete, de punta a punta: las ordenes
 * de A llegan a B en orden y una sola vez (por el bus y, con el bus cortado, por el tic de puesta
 * al dia), el tablero de A ve al equipo de B, un stream mas nuevo en A sustituye al de B, el
 * cierre en A cierra en B, y una replica parada deja de contar.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplicaClusterTest {

    private static final String TECHNICIAN = "campo.tecnico1";
    private static final String SUPERVISOR = "campo.responsable";
    private static final Duration QUIET = Duration.ofMillis(400);
    private static final Duration CATCH_UP = Duration.ofSeconds(1);

    private static ConfigurableApplicationContext replicaA;
    private static ConfigurableApplicationContext replicaB;
    private static ManagedChannel toA;
    private static ManagedChannel toB;

    @BeforeAll
    static void startTwoReplicas() {
        replicaA = replica("A");
        replicaB = replica("B");
        toA = channelTo(replicaA);
        toB = channelTo(replicaB);
        assertThat(LocalReplicaHub.members()).isEqualTo(2);
    }

    @AfterAll
    static void stopEverything() {
        for (ManagedChannel channel : List.of(toA, toB)) {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
        for (ConfigurableApplicationContext context : List.of(replicaB, replicaA)) {
            if (context != null && context.isActive()) {
                context.close();
            }
        }
        LocalReplicaHub.cut(false);
    }

    private static ConfigurableApplicationContext replica(String id) {
        Map<String, Object> properties = new LinkedHashMap<>(PostgreSQLTestContainer.datasourceProperties());
        properties.put("server.port", "0");
        properties.put("spring.grpc.server.port", "0");
        properties.put("spring.main.banner-mode", "off");
        properties.put("app.maintenance.enabled", "false");
        properties.put("app.maintenance.sync-retry.enabled", "false");
        properties.put("app.rabbitmq.enabled", "false");
        properties.put("app.security.audience-validation-enabled", "true");
        properties.put("app.security.required-audience", TestTokens.AUDIENCE);
        properties.put("spring.security.oauth2.resourceserver.jwt.issuer-uri", TestTokens.ISSUER);
        properties.put("app.field.team-binding.enabled", "false");
        properties.put("app.field.board.tick", "500ms");
        properties.put("app.field.liveness.stale-after", "2s");
        properties.put("app.field.liveness.disconnected-after", "4s");
        properties.put("app.field.replicas.id", id);
        properties.put("app.field.replicas.catch-up", CATCH_UP.toMillis() + "ms");
        properties.put("app.field.replicas.remote-ttl", "6s");
        properties.put("management.tracing.enabled", "false");
        // Como argumentos y no como properties(): estas son valores por defecto, por debajo del YAML
        // del perfil dev, y aqui tienen que ganar (la base del test, los puertos, la replica).
        String[] arguments = properties.entrySet().stream().map(entry -> "--" + entry.getKey() + "=" + entry.getValue()).toArray(String[]::new);
        return new SpringApplicationBuilder(MtoFieldApplication.class, TestJwtDecoderConfiguration.class, LocalReplicaBusConfiguration.class)
                .run(arguments);
    }

    private static ManagedChannel channelTo(ConfigurableApplicationContext context) {
        Integer port = context.getEnvironment().getProperty("local.grpc.server.port", Integer.class);
        assertThat(port).as("the test auto-configuration publishes the bound gRPC port").isNotNull();
        return ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    }

    /** Las ordenes emitidas en A llegan al dispositivo de B por el bus, en orden y una sola vez; el tablero de A ve al equipo de B. */
    @Test
    @Order(1)
    void commandsIssuedOnOneReplicaReachADeviceOnTheOtherAndItsBoardSeesThatDevice() {
        UUID shift = UUID.randomUUID();
        Possession possession = open(shift);
        String deviceId = "dev-cluster-" + UUID.randomUUID();
        BoardClient board = BoardClient.watch(toA, TestTokens.supervisor(SUPERVISOR), possession.getId());

        DeviceClient device = DeviceClient.join(toB, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
        device.awaitWelcome();
        device.heartbeat("34.271", 80, -70);
        board.awaitBoard("the team on B is CONNECTED with its kp on the board of A", current ->
                team(current, shift).getLiveness() == TeamState.Liveness.CONNECTED && "34.271".equals(team(current, shift).getKp()));

        List<Long> issued = new ArrayList<>();
        for (int index = 1; index <= 5; index++) {
            issued.add(say(possession.getId(), "mensaje " + index).getSequence());
        }
        IssueCommandResponse evacuation = evacuate(possession.getId(), "evac-" + deviceId, "desde A");
        issued.add(evacuation.getSequence());
        List<FieldCommand> received = new ArrayList<>();
        while (received.size() < issued.size()) {
            received.add(device.nextCommand());
        }
        assertThat(received.stream().map(FieldCommand::getSequence).toList()).isEqualTo(issued);
        assertThat(received.getLast().hasEvacuateNow()).isTrue();
        board.awaitBoard("sent to the team on B", current -> command(current, evacuation.getCommandId()).getSentToList().contains(teamCode(shift)));

        device.ack(evacuation.getCommandId());
        assertThat(device.nextEventResult().getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.APPLIED);
        board.awaitBoard("acked by the team on B", current -> command(current, evacuation.getCommandId()).getAckedByList().contains(teamCode(shift)));
        assertThat(device.maybeNext(QUIET)).as("nothing arrives twice").isEmpty();

        supervisor(toA).closePossession(close(possession, true, "limpieza"));
        assertThat(device.outcome().getCode()).as("the close on A completes the stream on B").isEqualTo(Status.Code.OK);
        assertThat(board.outcome().getCode()).isEqualTo(Status.Code.OK);
    }

    /** Con el bus cortado, el tic de puesta al dia trae de la base lo que el bus no trajo: la orden y, despues, el cierre. */
    @Test
    @Order(2)
    void withTheBusCutTheCatchUpTickDeliversTheCommandsAndTheCloseFromTheDatabase() {
        UUID shift = UUID.randomUUID();
        Possession possession = open(shift);
        String deviceId = "dev-cut-" + UUID.randomUUID();
        DeviceClient device = DeviceClient.join(toB, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
        device.awaitWelcome();

        long droppedBefore = LocalReplicaHub.dropped();
        LocalReplicaHub.cut(true);
        try {
            IssueCommandResponse first = say(possession.getId(), "sin bus 1");
            IssueCommandResponse second = say(possession.getId(), "sin bus 2");
            FieldCommand one = device.nextCommand();
            FieldCommand two = device.nextCommand();
            assertThat(one.getSequence()).isEqualTo(first.getSequence());
            assertThat(two.getSequence()).isEqualTo(second.getSequence());
            assertThat(LocalReplicaHub.dropped()).as("the bus really dropped the relays").isGreaterThan(droppedBefore);
            assertThat(device.maybeNext(QUIET)).isEmpty();

            supervisor(toA).closePossession(close(possession, true, "limpieza"));
            assertThat(device.outcome(CATCH_UP.multipliedBy(5)).getCode()).as("the tick finds the possession closed in the database")
                    .isEqualTo(Status.Code.OK);
        } finally {
            LocalReplicaHub.cut(false);
        }
    }

    /** El dispositivo que vuelve por A deja su stream de B ABORTED SUPERSEDED, como un segundo stream local. */
    @Test
    @Order(3)
    void aNewerStreamOnAnotherReplicaSupersedesTheOneHeldHere() {
        UUID shift = UUID.randomUUID();
        Possession possession = open(shift);
        String deviceId = "dev-moved-" + UUID.randomUUID();
        DeviceClient onB = DeviceClient.join(toB, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
        onB.awaitWelcome();

        DeviceClient onA = DeviceClient.join(toA, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
        onA.awaitWelcome();
        assertThat(onB.outcome().getCode()).isEqualTo(Status.Code.ABORTED);
        assertThat(GrpcErrors.reasonOf(onB.outcomeError())).isEqualTo(DeviceStream.REASON_SUPERSEDED);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(replicaB.getBean(DeviceStreamRegistry.class).ofDevice(deviceId)).isNull());
        assertThat(replicaA.getBean(DeviceStreamRegistry.class).ofDevice(deviceId)).isNotNull();

        long sequence = say(possession.getId(), "al nuevo stream").getSequence();
        assertThat(onA.nextCommand().getSequence()).isEqualTo(sequence);
        supervisor(toA).closePossession(close(possession, true, "limpieza"));
        assertThat(onA.outcome().getCode()).isEqualTo(Status.Code.OK);
    }

    /** B se para: lo que contaba de sus dispositivos deja de valer en A y el equipo pasa a DISCONNECTED. */
    @Test
    @Order(4)
    void aReplicaThatStopsNoLongerCountsOnTheBoardsOfTheOthers() {
        UUID shift = UUID.randomUUID();
        Possession possession = open(shift);
        String deviceId = "dev-stopping-" + UUID.randomUUID();
        BoardClient board = BoardClient.watch(toA, TestTokens.supervisor(SUPERVISOR), possession.getId());
        DeviceClient device = DeviceClient.join(toB, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
        device.awaitWelcome();
        board.awaitBoard("the team on B is CONNECTED on A", current -> team(current, shift).getLiveness() == TeamState.Liveness.CONNECTED);
        assertThat(replicaA.getBean(RemoteDeviceStates.class).ofDevice(deviceId)).isPresent();

        toB.shutdownNow();
        replicaB.close();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(replicaA.getBean(RemoteDeviceStates.class).ofDevice(deviceId)).isEmpty());
        board.awaitBoard("the team is DISCONNECTED on A", current -> team(current, shift).getLiveness() == TeamState.Liveness.DISCONNECTED);
        supervisor(toA).closePossession(close(possession, true, "limpieza"));
        assertThat(board.outcome().getCode()).isEqualTo(Status.Code.OK);
    }

    private FieldServiceGrpc.FieldServiceBlockingStub supervisor(ManagedChannel channel) {
        return TestTokens.withToken(FieldServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS), TestTokens.supervisor(SUPERVISOR));
    }

    private Possession open(UUID shiftId) {
        return supervisor(toA).openPossession(OpenPossessionRequest.newBuilder().addShiftIds(shiftId.toString()).build());
    }

    private static ClosePossessionRequest close(Possession possession, boolean force, String reason) {
        return ClosePossessionRequest.newBuilder().setPossessionId(possession.getId()).setForce(force).setReason(reason).build();
    }

    private IssueCommandResponse evacuate(String possessionId, String key, String reason) {
        return supervisor(toA).issueCommand(IssueCommandRequest.newBuilder().setPossessionId(possessionId).setIdempotencyKey(key)
                .setEvacuateNow(EvacuateNow.newBuilder().setReason(reason)).build());
    }

    private IssueCommandResponse say(String possessionId, String text) {
        return supervisor(toA).issueCommand(IssueCommandRequest.newBuilder().setPossessionId(possessionId)
                .setSupervisorMessage(SupervisorMessage.newBuilder().setAuthor(SUPERVISOR).setText(text)).build());
    }

    private static String teamCode(UUID shiftId) {
        return "T-" + shiftId.toString().substring(0, 4).toUpperCase();
    }

    private static TeamState team(PossessionBoard board, UUID shiftId) {
        return board.getTeamsList().stream().filter(team -> team.getShiftId().equals(shiftId.toString())).findFirst().orElseThrow();
    }

    private static CommandState command(PossessionBoard board, String commandId) {
        return board.getCommandsList().stream().filter(command -> command.getCommandId().equals(commandId)).findFirst()
                .orElse(CommandState.getDefaultInstance());
    }
}
