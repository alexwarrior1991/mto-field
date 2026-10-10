package com.alejandro.mtofield.simulator;

import com.alejandro.mtofield.grpc.v1.CommandAck;
import com.alejandro.mtofield.grpc.v1.EventResult;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.grpc.v1.SyncResult;
import com.alejandro.mtofield.grpc.v1.TaskStarted;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.alejandro.mtofield.grpc.v1.Welcome;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El fichero de estado de un dispositivo del simulador: ida y vuelta de lo que guarda, escritura
 * atomica sin temporal que se quede, y un fichero ausente o corrupto que cuenta como ninguno.
 */
class DeviceStateStoreTest {

    @TempDir
    Path directory;

    @Test
    void whatIsSavedComesBackWholeAndAFileIsNeverLeftHalfWritten() {
        DeviceStateStore store = new DeviceStateStore(directory, "sim-t1-d1");
        TeamMessage ack = TeamMessage.newBuilder().setDeviceId("sim-t1-d1").setSequence(7)
                .setOccurredAt(Timestamp.newBuilder().setSeconds(1_760_000_000L).setNanos(123))
                .setCommandAck(CommandAck.newBuilder().setCommandId("c-1").setAccepted(true)).build();
        TeamMessage work = TeamMessage.newBuilder().setDeviceId("sim-t1-d1").setSequence(8)
                .setTaskStarted(TaskStarted.newBuilder().setOrderId("MO-SIM").setTaskId("task-1")).build();

        assertThat(store.load()).as("sin fichero no hay estado").isEmpty();
        store.save(new DeviceStateStore.State(8, 12, List.of(ack, work)));

        assertThat(store.file()).exists();
        assertThat(store.file().resolveSibling("sim-t1-d1.json.tmp")).as("el temporal se mueve, no se queda").doesNotExist();
        assertThat(store.load()).hasValueSatisfying(state -> {
            assertThat(state.sequence()).isEqualTo(8);
            assertThat(state.lastCommandSequence()).isEqualTo(12);
            assertThat(state.unconfirmed()).containsExactly(ack, work);
        });

        store.save(new DeviceStateStore.State(9, 12, List.of()));
        assertThat(store.load()).hasValueSatisfying(state -> {
            assertThat(state.sequence()).isEqualTo(9);
            assertThat(state.unconfirmed()).isEmpty();
        });
    }

    /** Un dispositivo que vuelve a arrancar sigue por su fichero: se une tras su ultima orden, reenvia lo no confirmado y no retrocede. */
    @Test
    void aRestartedDeviceResumesFromItsFileResendsWhatWasUnconfirmedAndKeepsItsCounter() {
        DeviceStateStore store = new DeviceStateStore(directory, "sim-t1-d1");
        TeamMessage ack = TeamMessage.newBuilder().setDeviceId("sim-t1-d1").setSequence(7)
                .setCommandAck(CommandAck.newBuilder().setCommandId("c-1").setAccepted(true)).build();
        TeamMessage work = TeamMessage.newBuilder().setDeviceId("sim-t1-d1").setSequence(8)
                .setTaskStarted(TaskStarted.newBuilder().setOrderId("MO-SIM").setTaskId("task-1")).build();
        store.save(new DeviceStateStore.State(8, 12, List.of(ack, work)));
        FakeTransport transport = new FakeTransport();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            DeviceScript script = new DeviceScript("sim-t1-d1", UUID.randomUUID(), "team 1", false, Duration.ofSeconds(10), new BigDecimal("31.000"),
                    scheduler, store);

            assertThat(script.lastCommandSequence()).isEqualTo(12);
            script.onConnected(transport);
            assertThat(transport.sent).singleElement().satisfies(join -> assertThat(join.getJoin().getLastCommandSequence()).isEqualTo(12));

            // El servidor guarda hasta #6: el acuse #7 vuelve por el canal, el trabajo #8 por el atraso, y el contador sigue en 8.
            script.onCommand(FieldCommand.newBuilder().setSequence(0).setWelcome(Welcome.newBuilder().setPossessionId("p").setLastAppliedSequence(6)).build());
            assertThat(transport.sent).hasSize(2).last().isEqualTo(ack);
            assertThat(transport.backlogs).singleElement().isEqualTo(List.of(work));
            assertThat(store.load()).hasValueSatisfying(state -> assertThat(state.sequence()).isEqualTo(8));

            // Una orden aplicada y el resultado del acuse se guardan en cuanto llegan.
            script.onCommand(FieldCommand.newBuilder().setSequence(13).setCommandId("c-2")
                    .setEventResult(EventResult.newBuilder().setSequence(7).setOutcome(EventResult.Outcome.APPLIED)).build());
            assertThat(store.load()).hasValueSatisfying(state -> {
                assertThat(state.lastCommandSequence()).isEqualTo(13);
                assertThat(state.unconfirmed()).containsExactly(work);
            });
        } finally {
            scheduler.shutdownNow();
        }

        // Sin fichero (o perdido) y con el servidor por delante, el contador adopta la marca y lo guarda.
        DeviceStateStore fresh = new DeviceStateStore(directory, "sim-t9-d1");
        ScheduledExecutorService another = Executors.newSingleThreadScheduledExecutor();
        try {
            DeviceScript script = new DeviceScript("sim-t9-d1", UUID.randomUUID(), "team 9", false, Duration.ofSeconds(10), new BigDecimal("31.000"),
                    another, fresh);
            script.onConnected(new FakeTransport());
            script.onCommand(FieldCommand.newBuilder().setWelcome(Welcome.newBuilder().setPossessionId("p").setLastAppliedSequence(9)).build());
            assertThat(fresh.load()).hasValueSatisfying(state -> assertThat(state.sequence()).isEqualTo(9));
        } finally {
            another.shutdownNow();
        }
    }

    static final class FakeTransport implements DeviceScript.Transport {
        final List<TeamMessage> sent = new ArrayList<>();
        final List<List<TeamMessage>> backlogs = new ArrayList<>();

        @Override
        public void send(TeamMessage message) {
            sent.add(message);
        }

        @Override
        public void cancel() {
        }

        @Override
        public void halfClose() {
        }

        @Override
        public void uploadBacklog(List<TeamMessage> backlog, Consumer<SyncResult> onResult, Consumer<Status> onFailure) {
            backlogs.add(backlog);
        }
    }

    @Test
    void aCorruptFileCountsAsNoneAndDoesNotStopTheDevice() throws Exception {
        DeviceStateStore store = new DeviceStateStore(directory, "sim-t2-d1");
        Files.writeString(store.file(), "{\"sequence\": 3, \"unconfirmed\": [{\"not\": \"a message\"}]");

        assertThat(store.load()).isEmpty();

        store.save(new DeviceStateStore.State(3, 0, List.of()));
        assertThat(store.load()).hasValueSatisfying(state -> assertThat(state.sequence()).isEqualTo(3));
    }
}
