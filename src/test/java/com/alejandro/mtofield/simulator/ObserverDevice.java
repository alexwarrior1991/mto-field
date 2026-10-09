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
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * El primer estilo de cliente: el asincrono, con {@code ClientResponseObserver} para coger el
 * {@code ClientCallStreamObserver} y {@code setOnReadyHandler}. Lo que sube espera en una cola
 * propia y se escribe solo cuando el transporte esta listo, con el mismo CAS de drenado que usa
 * el servidor: nunca se bloquea el hilo del callback.
 */
final class ObserverDevice implements ClientResponseObserver<TeamMessage, FieldCommand>, DeviceScript.Transport {

    private final DeviceScript script;
    private final ManagedChannel channel;
    private final String token;
    private final ConcurrentLinkedQueue<TeamMessage> outbound = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean draining = new AtomicBoolean();
    private final CompletableFuture<Status> ended = new CompletableFuture<>();
    private volatile ClientCallStreamObserver<TeamMessage> requests;

    private ObserverDevice(DeviceScript script, ManagedChannel channel, String token) {
        this.script = script;
        this.channel = channel;
        this.token = token;
    }

    static DeviceScript.Session open(ManagedChannel channel, String token, DeviceScript script) {
        ObserverDevice device = new ObserverDevice(script, channel, token);
        TestTokens.withToken(FieldServiceGrpc.newStub(channel), token).teamChannel(device);
        script.onConnected(device);
        return new DeviceScript.Session(device, device.ended);
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<TeamMessage> requestStream) {
        this.requests = requestStream;
        requestStream.setOnReadyHandler(this::drain);
    }

    @Override
    public void send(TeamMessage message) {
        outbound.add(message);
        drain();
    }

    private void drain() {
        do {
            if (!draining.compareAndSet(false, true)) {
                return;
            }
            try {
                TeamMessage next;
                while (requests.isReady() && (next = outbound.poll()) != null) {
                    requests.onNext(next);
                }
            } finally {
                draining.set(false);
            }
        } while (!outbound.isEmpty() && requests.isReady());
    }

    @Override
    public void cancel() {
        requests.cancel("simulated loss of coverage", null);
    }

    @Override
    public void uploadBacklog(List<TeamMessage> backlog, Consumer<SyncResult> onResult, Consumer<Status> onFailure) {
        BufferedSync.upload(channel, token, script.deviceId(), script.shiftId(), backlog, onResult, onFailure);
    }

    @Override
    public void halfClose() {
        requests.onCompleted();
    }

    @Override
    public void onNext(FieldCommand command) {
        script.onCommand(command);
    }

    @Override
    public void onError(Throwable throwable) {
        Status status = Status.fromThrowable(throwable);
        script.onDisconnected(status);
        ended.complete(status);
    }

    @Override
    public void onCompleted() {
        script.onDisconnected(Status.OK);
        ended.complete(Status.OK);
    }
}
