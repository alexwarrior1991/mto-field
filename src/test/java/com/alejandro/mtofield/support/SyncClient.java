package com.alejandro.mtofield.support;

import com.alejandro.mtofield.grpc.v1.ClearOfTrack;
import com.alejandro.mtofield.grpc.v1.CommandAck;
import com.alejandro.mtofield.grpc.v1.FieldServiceGrpc;
import com.alejandro.mtofield.grpc.v1.Heartbeat;
import com.alejandro.mtofield.grpc.v1.Join;
import com.alejandro.mtofield.grpc.v1.SyncResult;
import com.alejandro.mtofield.grpc.v1.TaskCompleted;
import com.alejandro.mtofield.grpc.v1.TaskStarted;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.fail;

/**
 * Un {@code SyncBufferedEvents} de prueba: se abre, manda el {@code Join} y el atraso en el orden
 * que el test quiera, y {@link #finish()} cierra la subida y espera el {@link SyncResult}.
 */
public final class SyncClient implements StreamObserver<SyncResult> {

    private final String deviceId;
    private final CompletableFuture<SyncResult> result = new CompletableFuture<>();
    private final CompletableFuture<Status> outcome = new CompletableFuture<>();
    private volatile Metadata trailers = new Metadata();
    private volatile StreamObserver<TeamMessage> requests;

    private SyncClient(String deviceId) {
        this.deviceId = deviceId;
    }

    public static SyncClient open(ManagedChannel channel, String token, String deviceId) {
        SyncClient client = new SyncClient(deviceId);
        client.requests = TestTokens.withToken(FieldServiceGrpc.newStub(channel), token).syncBufferedEvents(client);
        return client;
    }

    /** Abre y manda el Join. */
    public static SyncClient join(ManagedChannel channel, String token, String deviceId, UUID shiftId) {
        SyncClient client = open(channel, token, deviceId);
        client.join(shiftId);
        return client;
    }

    public void join(UUID shiftId) {
        send(message(0).setJoin(Join.newBuilder().setShiftId(shiftId.toString())).build());
    }

    public void heartbeat(String kp) {
        send(message(0).setHeartbeat(Heartbeat.newBuilder().setKp(kp)).build());
    }

    public void taskStarted(long sequence, String orderId, String taskId) {
        send(message(sequence).setTaskStarted(TaskStarted.newBuilder().setOrderId(orderId).setTaskId(taskId)).build());
    }

    public void taskCompleted(long sequence, String orderId, String taskId) {
        send(message(sequence).setTaskCompleted(TaskCompleted.newBuilder().setOrderId(orderId).setTaskId(taskId).setWorkComplete(true)).build());
    }

    public void ack(long sequence, String commandId) {
        send(message(sequence).setCommandAck(CommandAck.newBuilder().setCommandId(commandId).setAccepted(true)).build());
    }

    public void clearOfTrack(long sequence, boolean earthingRemoved) {
        send(message(sequence).setClearOfTrack(ClearOfTrack.newBuilder().setEarthingRemoved(earthingRemoved)).build());
    }

    public void send(TeamMessage message) {
        requests.onNext(message);
    }

    /** Cierra la subida y espera el resultado; si el servidor cerro con error, falla el test con su estado. */
    public SyncResult finish() {
        requests.onCompleted();
        try {
            return result.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException timedOut) {
            return fail("Sync of device " + deviceId + " did not answer within 10 s");
        } catch (ExecutionException failed) {
            return fail("Sync of device " + deviceId + " failed: " + Status.fromThrowable(failed.getCause()) + " " + trailers);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return fail("interrupted");
        }
    }

    /** Cierra la subida y espera a que el servidor termine, como sea. */
    public Status finishExpectingAnError() {
        try {
            requests.onCompleted();
        } catch (RuntimeException alreadyClosed) {
            // El servidor cerro antes; el estado llega igual.
        }
        return outcome(Duration.ofSeconds(10));
    }

    public Status outcome(Duration timeout) {
        try {
            return outcome.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException timedOut) {
            return fail("Sync of device " + deviceId + " did not end within " + timeout);
        } catch (ExecutionException | InterruptedException unexpected) {
            return fail("Sync of device " + deviceId + " failed strangely", unexpected);
        }
    }

    /** El error con el que termino, con sus trailers (el ErrorInfo), para leer su {@code reason}. */
    public StatusRuntimeException outcomeError() {
        return outcome(Duration.ofSeconds(10)).asRuntimeException(trailers);
    }

    @Override
    public void onNext(SyncResult value) {
        result.complete(value);
    }

    @Override
    public void onError(Throwable throwable) {
        Metadata received = Status.trailersFromThrowable(throwable);
        if (received != null) {
            trailers = received;
        }
        result.completeExceptionally(throwable);
        outcome.complete(Status.fromThrowable(throwable));
    }

    @Override
    public void onCompleted() {
        outcome.complete(Status.OK);
    }

    private TeamMessage.Builder message(long withSequence) {
        Instant now = Instant.now();
        return TeamMessage.newBuilder().setDeviceId(deviceId).setSequence(withSequence)
                .setOccurredAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()));
    }
}
