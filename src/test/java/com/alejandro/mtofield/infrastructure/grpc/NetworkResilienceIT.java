package com.alejandro.mtofield.infrastructure.grpc;

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
import com.alejandro.mtofield.infrastructure.grpc.stream.DeviceStreamRegistry;
import com.alejandro.mtofield.support.BoardClient;
import com.alejandro.mtofield.support.DeviceClient;
import com.alejandro.mtofield.support.PostgreSQLTestContainer;
import com.alejandro.mtofield.support.TestJwtDecoderConfiguration;
import com.alejandro.mtofield.support.TestTokens;
import com.alejandro.mtofield.support.ToxiproxyGateway;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.grpc.test.autoconfigure.LocalGrpcServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * El canal de campo con una red de verdad en medio: el Netty del servidor en un puerto libre, el
 * dispositivo detras de un Toxiproxy ({@link ToxiproxyGateway}) y el responsable y el tablero en
 * directo. Lo que el transporte in-process de {@code GrpcServiceLayerTest} no puede ensenar: el
 * corte de una conexion TCP a mitad del desalojo, una red lenta y estrecha, un par que enmudece sin
 * cerrar (lo que detecta el keepalive del servidor) y el ping del cliente dentro de lo permitido.
 *
 * <p>grpc-java no admite un keepalive por debajo de 10 s en ningun lado ({@code KeepAliveManager}
 * sube cualquier valor menor), asi que el servidor corre aqui con el minimo real, 10 s + 1 s, y
 * los dos escenarios que lo esperan tardan eso.</p>
 */
@SpringBootTest(properties = {
        "server.port=0",
        "spring.grpc.server.port=0",
        "app.maintenance.enabled=false",
        "app.security.audience-validation-enabled=true",
        "app.security.required-audience=" + TestTokens.AUDIENCE,
        "spring.security.oauth2.resourceserver.jwt.issuer-uri=" + TestTokens.ISSUER,
        "app.field.team-binding.enabled=false",
        "app.field.board.tick=500ms",
        "app.field.liveness.stale-after=2s",
        "app.field.liveness.disconnected-after=4s",
        "spring.grpc.server.keepalive.time=10s",
        "spring.grpc.server.keepalive.timeout=1s",
        "spring.grpc.server.keepalive.permit.time=1s",
        "spring.grpc.server.keepalive.permit.without-calls=true",
        "app.rabbitmq.enabled=false"
})
@Import(TestJwtDecoderConfiguration.class)
class NetworkResilienceIT extends PostgreSQLTestContainer {

    private static final String TECHNICIAN = "campo.tecnico1";
    private static final String SUPERVISOR = "campo.responsable";
    private static final Duration QUIET = Duration.ofMillis(400);
    private static final Duration KEEPALIVE_DETECTION = Duration.ofSeconds(11);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registerPostgreSQLProperties(registry);
    }

    @LocalGrpcServerPort
    private int grpcPort;

    @Autowired
    private DeviceStreamRegistry registry;

    private ToxiproxyGateway.Link link;
    private ManagedChannel direct;
    private ManagedChannel viaProxy;
    private final List<ManagedChannel> extraChannels = new ArrayList<>();

    @BeforeEach
    void openChannels() {
        link = ToxiproxyGateway.towards(grpcPort).proxyTo("field-" + UUID.randomUUID(), grpcPort);
        direct = ManagedChannelBuilder.forAddress("localhost", grpcPort).usePlaintext().build();
        viaProxy = link.channelBuilder().build();
    }

    /** Tambien tras un {@code @BeforeEach} abortado por la asuncion, cuando no hay nada abierto. */
    @AfterEach
    void closeChannels() {
        extraChannels.forEach(ManagedChannel::shutdownNow);
        if (viaProxy != null) {
            viaProxy.shutdownNow();
        }
        if (direct != null) {
            direct.shutdownNow();
        }
        if (link != null) {
            link.close();
        }
    }

    /**
     * El corte a mitad del desalojo: Toxiproxy cierra las dos conexiones, el servidor da el stream
     * por cerrado y la orden que se emite mientras tanto queda en cola para el equipo. Al reanudar
     * con la ultima secuencia vista, la orden llega una sola vez, en orden, y el tablero pasa de
     * queued_for a sent_to y a acked_by.
     */
    @Test
    void aDeviceCutDuringTheEvacuationReceivesTheOrderOnceAndInOrderWhenItResumes() throws Exception {
        UUID shift = UUID.randomUUID();
        Possession possession = open(shift);
        String deviceId = "dev-cut-" + UUID.randomUUID();
        BoardClient board = BoardClient.watch(direct, TestTokens.supervisor(SUPERVISOR), possession.getId());

        DeviceClient before = DeviceClient.join(viaProxy, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
        before.awaitWelcome();
        long lastSeen = say(possession.getId(), "antes del corte").getSequence();
        assertThat(before.nextCommand().getSequence()).isEqualTo(lastSeen);
        board.awaitBoard("the team is CONNECTED", current -> team(current, shift).getLiveness() == TeamState.Liveness.CONNECTED);

        link.proxy().disable();
        assertThat(before.outcome(Duration.ofSeconds(10)).getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(registry.ofDevice(deviceId)).isNull());

        IssueCommandResponse evacuation = evacuate(possession.getId(), "evac-" + deviceId, "corte a mitad del desalojo");
        board.awaitBoard("the order is queued for the team", current ->
                command(current, evacuation.getCommandId()).getQueuedForList().contains(teamCode(shift)));
        assertThat(team(board.awaitBoard("the team is DISCONNECTED", current ->
                team(current, shift).getLiveness() == TeamState.Liveness.DISCONNECTED), shift).getLiveness())
                .isEqualTo(TeamState.Liveness.DISCONNECTED);

        link.proxy().enable();
        DeviceClient resumed = DeviceClient.join(freshChannel(), TestTokens.technician(TECHNICIAN), deviceId, shift, lastSeen);
        resumed.awaitWelcome();
        FieldCommand order = resumed.nextCommand();
        assertThat(order.hasEvacuateNow()).as("the first thing after the Welcome is the evacuation").isTrue();
        assertThat(order.getSequence()).isEqualTo(evacuation.getSequence());
        board.awaitBoard("the order is sent to the team", current ->
                command(current, evacuation.getCommandId()).getSentToList().contains(teamCode(shift)));

        resumed.ack(evacuation.getCommandId());
        assertThat(resumed.nextEventResult().getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.APPLIED);
        board.awaitBoard("the order is acked by the team", current ->
                command(current, evacuation.getCommandId()).getAckedByList().contains(teamCode(shift)));
        assertThat(resumed.maybeNext(QUIET)).as("the order arrives once").isEmpty();

        supervisor().closePossession(close(possession, true, "limpieza"));
        assertThat(resumed.outcome().getCode()).isEqualTo(Status.Code.OK);
    }

    /**
     * Una red lenta y estrecha (300 ms de latencia con 100 de jitter y 16 KB/s en los dos
     * sentidos): veinte mensajes y el desalojo llegan en el orden en que se emitieron, sin
     * duplicados, y el acuse vuelve.
     */
    @Test
    void ordersAndAcksKeepTheirOrderOnASlowAndNarrowNetwork() throws Exception {
        link.proxy().toxics().latency("latency-down", ToxicDirection.DOWNSTREAM, 300).setJitter(100);
        link.proxy().toxics().latency("latency-up", ToxicDirection.UPSTREAM, 300).setJitter(100);
        link.proxy().toxics().bandwidth("bandwidth-down", ToxicDirection.DOWNSTREAM, 16);
        link.proxy().toxics().bandwidth("bandwidth-up", ToxicDirection.UPSTREAM, 16);
        UUID shift = UUID.randomUUID();
        Possession possession = open(shift);
        String deviceId = "dev-slow-" + UUID.randomUUID();
        BoardClient board = BoardClient.watch(direct, TestTokens.supervisor(SUPERVISOR), possession.getId());

        DeviceClient device = DeviceClient.join(viaProxy, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
        device.awaitWelcome();
        List<Long> issued = new ArrayList<>();
        for (int index = 1; index <= 20; index++) {
            issued.add(say(possession.getId(), "mensaje " + index).getSequence());
        }
        IssueCommandResponse evacuation = evacuate(possession.getId(), "evac-" + deviceId, "red lenta");
        issued.add(evacuation.getSequence());

        List<FieldCommand> received = new ArrayList<>();
        while (received.size() < issued.size()) {
            received.add(device.nextCommand());
        }
        assertThat(received.stream().map(FieldCommand::getSequence).toList()).isEqualTo(issued);
        assertThat(received.subList(0, 20)).extracting(command -> command.getSupervisorMessage().getText())
                .containsExactlyElementsOf(issuedTexts(20));
        assertThat(received.get(20).hasEvacuateNow()).isTrue();

        device.ack(evacuation.getCommandId());
        assertThat(device.nextEventResult().getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.APPLIED);
        board.awaitBoard("the order is acked by the team", current ->
                command(current, evacuation.getCommandId()).getAckedByList().contains(teamCode(shift)));
        assertThat(device.maybeNext(QUIET)).as("nothing arrives twice").isEmpty();

        supervisor().closePossession(close(possession, true, "limpieza"));
        assertThat(device.outcome(Duration.ofSeconds(15)).getCode()).isEqualTo(Status.Code.OK);
    }

    /**
     * Un par que enmudece sin cerrar: Toxiproxy se traga los bytes en los dos sentidos (ni FIN ni
     * RST, el enlace de radio muerto). El ping del keepalive del servidor no vuelve y el
     * servidor cierra el transporte en time + timeout; el stream desaparece del registro y el
     * tablero marca al equipo DISCONNECTED sin que nadie haya dicho nada.
     */
    @Test
    void aPeerThatGoesSilentWithoutClosingIsDetectedByTheServerKeepalive() throws Exception {
        UUID shift = UUID.randomUUID();
        Possession possession = open(shift);
        String deviceId = "dev-mute-" + UUID.randomUUID();
        BoardClient board = BoardClient.watch(direct, TestTokens.supervisor(SUPERVISOR), possession.getId());
        DeviceClient device = DeviceClient.join(viaProxy, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
        device.awaitWelcome();
        board.awaitBoard("the team is CONNECTED", current -> team(current, shift).getLiveness() == TeamState.Liveness.CONNECTED);

        link.proxy().toxics().timeout("mute-down", ToxicDirection.DOWNSTREAM, 0);
        link.proxy().toxics().timeout("mute-up", ToxicDirection.UPSTREAM, 0);
        Instant muteSince = Instant.now();

        await().atMost(KEEPALIVE_DETECTION.plusSeconds(5)).untilAsserted(() -> assertThat(registry.ofDevice(deviceId)).isNull());
        assertThat(Duration.between(muteSince, Instant.now())).as("detected in keepalive time + timeout").isLessThan(KEEPALIVE_DETECTION.plusSeconds(5));
        board.awaitBoard("the team is DISCONNECTED", current -> team(current, shift).getLiveness() == TeamState.Liveness.DISCONNECTED);

        device.cancel();
        supervisor().closePossession(close(possession, true, "limpieza"));
    }

    /**
     * El ping del cliente dentro de lo permitido: un dispositivo que hace ping cada 10 s (el minimo
     * que admite grpc-java; el simulador cada 20 s) sin ninguna llamada de aplicacion, con
     * permit.time 1 s, no recibe GOAWAY ENHANCE_YOUR_CALM y sigue recibiendo ordenes despues.
     */
    @Test
    void aClientPingingWithinThePermittedRateIsNotToldToCalmDown() throws Exception {
        ManagedChannel pinging = link.channelBuilder()
                .keepAliveTime(10, TimeUnit.SECONDS)
                .keepAliveTimeout(1, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .build();
        extraChannels.add(pinging);
        UUID shift = UUID.randomUUID();
        Possession possession = open(shift);
        String deviceId = "dev-ping-" + UUID.randomUUID();
        DeviceClient device = DeviceClient.join(pinging, TestTokens.technician(TECHNICIAN), deviceId, shift, 0);
        device.awaitWelcome();

        assertThat(device.maybeNext(KEEPALIVE_DETECTION.plusSeconds(1))).as("silence on the application level while the client pings").isEmpty();
        assertThat(device.isFinished()).isFalse();
        assertThat(registry.ofDevice(deviceId)).isNotNull();
        long sequence = say(possession.getId(), "despues del ping").getSequence();
        assertThat(device.nextCommand().getSequence()).isEqualTo(sequence);

        supervisor().closePossession(close(possession, true, "limpieza"));
        assertThat(device.outcome().getCode()).isEqualTo(Status.Code.OK);
    }

    private ManagedChannel freshChannel() {
        ManagedChannel channel = link.channelBuilder().build();
        extraChannels.add(channel);
        return channel;
    }

    private FieldServiceGrpc.FieldServiceBlockingStub supervisor() {
        return TestTokens.withToken(FieldServiceGrpc.newBlockingStub(direct).withDeadlineAfter(10, TimeUnit.SECONDS), TestTokens.supervisor(SUPERVISOR));
    }

    private Possession open(UUID shiftId) {
        return supervisor().openPossession(OpenPossessionRequest.newBuilder().addShiftIds(shiftId.toString()).build());
    }

    private static ClosePossessionRequest close(Possession possession, boolean force, String reason) {
        return ClosePossessionRequest.newBuilder().setPossessionId(possession.getId()).setForce(force).setReason(reason).build();
    }

    private IssueCommandResponse evacuate(String possessionId, String key, String reason) {
        return supervisor().issueCommand(IssueCommandRequest.newBuilder().setPossessionId(possessionId).setIdempotencyKey(key)
                .setEvacuateNow(EvacuateNow.newBuilder().setReason(reason)).build());
    }

    private IssueCommandResponse say(String possessionId, String text) {
        return supervisor().issueCommand(IssueCommandRequest.newBuilder().setPossessionId(possessionId)
                .setSupervisorMessage(SupervisorMessage.newBuilder().setAuthor(SUPERVISOR).setText(text)).build());
    }

    private static List<String> issuedTexts(int count) {
        List<String> texts = new ArrayList<>(count);
        for (int index = 1; index <= count; index++) {
            texts.add("mensaje " + index);
        }
        return texts;
    }

    /** El equipo sintetico del cliente de mantenimiento apagado. */
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
