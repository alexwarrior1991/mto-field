package com.alejandro.mtofield.simulator;

import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.grpc.v1.FieldServiceGrpc;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.alejandro.mtofield.support.TestTokens;
import com.alejandro.mtofield.grpc.v1.SyncResult;
import java.util.List;
import java.util.function.Consumer;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.BlockingClientCall;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * El segundo estilo de cliente: el stub bloqueante v2 ({@code newBlockingV2Stub().teamChannel()})
 * sobre dos hilos virtuales, uno que lee y otro que escribe. {@code write()} bloquea hasta que
 * el transporte esta listo, asi que la contrapresion es gratis; {@code halfClose()} al salir.
 */
final class BlockingDevice implements DeviceScript.Transport {

    private static final TeamMessage POISON = TeamMessage.getDefaultInstance();

    private final DeviceScript script;
    private final BlockingClientCall<TeamMessage, FieldCommand> call;
    private final LinkedBlockingQueue<TeamMessage> outbound = new LinkedBlockingQueue<>();
    private final CompletableFuture<Status> ended = new CompletableFuture<>();
    private volatile boolean cancelled;

    private final ManagedChannel channel;
    private final String token;

    private BlockingDevice(DeviceScript script, BlockingClientCall<TeamMessage, FieldCommand> call, ManagedChannel channel, String token) {
        this.script = script;
        this.call = call;
        this.channel = channel;
        this.token = token;
    }

    static DeviceScript.Session open(ManagedChannel channel, String token, DeviceScript script) {
        BlockingClientCall<TeamMessage, FieldCommand> call = TestTokens.withToken(FieldServiceGrpc.newBlockingV2Stub(channel), token).teamChannel();
        BlockingDevice device = new BlockingDevice(script, call, channel, token);
        Thread.ofVirtual().name("sim-writer-" + script.deviceId()).start(device::writeLoop);
        Thread.ofVirtual().name("sim-reader-" + script.deviceId()).start(device::readLoop);
        script.onConnected(device);
        return new DeviceScript.Session(device, device.ended);
    }

    @Override
    public void send(TeamMessage message) {
        outbound.add(message);
    }

    @Override
    public void cancel() {
        cancelled = true;
        call.cancel("simulated loss of coverage", null);
        outbound.add(POISON);
    }

    @Override
    public void halfClose() {
        outbound.add(POISON);
    }

    /** El atraso sube con el estilo de observador: el stub bloqueante no aporta nada a una subida de una sola respuesta. */
    @Override
    public void uploadBacklog(List<TeamMessage> backlog, Consumer<SyncResult> onResult, Consumer<Status> onFailure) {
        BufferedSync.upload(channel, token, script.deviceId(), script.shiftId(), backlog, onResult, onFailure);
    }

    private void writeLoop() {
        try {
            while (true) {
                TeamMessage next = outbound.take();
                if (next == POISON) {
                    if (!cancelled) {
                        call.halfClose();
                    }
                    return;
                }
                if (!call.write(next)) {
                    return;
                }
            }
        } catch (StatusException | IllegalStateException failed) {
            // El lector es quien cuenta como termino el stream; una escritura tras cancelar no es noticia.
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void readLoop() {
        Status status;
        try {
            FieldCommand command;
            while ((command = call.read()) != null) {
                script.onCommand(command);
            }
            status = Status.OK;
        } catch (StatusException failed) {
            status = failed.getStatus();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            status = Status.CANCELLED;
        }
        outbound.add(POISON);
        script.onDisconnected(status);
        ended.complete(status);
    }
}
