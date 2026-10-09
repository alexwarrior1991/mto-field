package com.alejandro.mtofield.simulator;

import com.alejandro.mtofield.grpc.v1.FieldServiceGrpc;
import com.alejandro.mtofield.grpc.v1.Join;
import com.alejandro.mtofield.grpc.v1.SyncResult;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.alejandro.mtofield.support.TestTokens;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * La subida del atraso por {@code SyncBufferedEvents}: una llamada aparte del canal vivo, que
 * manda el {@code Join}, el atraso en orden (solo cuando el transporte esta listo, como el
 * {@link ObserverDevice}), se despide y entrega el {@link SyncResult}. Los resultados de cada
 * evento llegan por el {@code TeamChannel}, no por aqui.
 */
final class BufferedSync implements ClientResponseObserver<TeamMessage, SyncResult> {

    private final Queue<TeamMessage> outbound;
    private final Consumer<SyncResult> onResult;
    private final Consumer<Status> onFailure;
    private final AtomicBoolean draining = new AtomicBoolean();
    private volatile ClientCallStreamObserver<TeamMessage> requests;
    private volatile boolean halfClosed;

    private BufferedSync(Queue<TeamMessage> outbound, Consumer<SyncResult> onResult, Consumer<Status> onFailure) {
        this.outbound = outbound;
        this.onResult = onResult;
        this.onFailure = onFailure;
    }

    static void upload(ManagedChannel channel, String token, String deviceId, UUID shiftId, List<TeamMessage> backlog,
                       Consumer<SyncResult> onResult, Consumer<Status> onFailure) {
        Queue<TeamMessage> outbound = new ArrayDeque<>(backlog.size() + 1);
        outbound.add(TeamMessage.newBuilder().setDeviceId(deviceId).setSequence(0)
                .setJoin(Join.newBuilder().setShiftId(shiftId.toString())).build());
        outbound.addAll(backlog);
        BufferedSync sync = new BufferedSync(outbound, onResult, onFailure);
        TestTokens.withToken(FieldServiceGrpc.newStub(channel), token).syncBufferedEvents(sync);
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<TeamMessage> requestStream) {
        this.requests = requestStream;
        requestStream.setOnReadyHandler(this::drain);
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
                if (outbound.isEmpty() && !halfClosed) {
                    halfClosed = true;
                    requests.onCompleted();
                }
            } finally {
                draining.set(false);
            }
        } while (!outbound.isEmpty() && requests.isReady());
    }

    @Override
    public void onNext(SyncResult result) {
        onResult.accept(result);
    }

    @Override
    public void onError(Throwable throwable) {
        onFailure.accept(Status.fromThrowable(throwable));
    }

    @Override
    public void onCompleted() {
        // El resultado ya llego por onNext.
    }
}
