package com.alejandro.mtofield.support;

import com.alejandro.mtofield.grpc.v1.ClearOfTrack;
import com.alejandro.mtofield.grpc.v1.CommandAck;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.grpc.v1.FieldServiceGrpc;
import com.alejandro.mtofield.grpc.v1.Heartbeat;
import com.alejandro.mtofield.grpc.v1.Join;
import com.alejandro.mtofield.grpc.v1.TaskCompleted;
import com.alejandro.mtofield.grpc.v1.TaskStarted;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Un dispositivo de prueba sobre {@code TeamChannel}: lo que recibe se guarda en una cola que el
 * test lee con plazo, y lo que sube lleva su secuencia monotona. Cancelar es lo que hace un
 * dispositivo sin cobertura; despedirse, {@link #halfClose()}.
 */
public final class DeviceClient implements ClientResponseObserver<TeamMessage, FieldCommand> {

    private final String deviceId;
    private final BlockingQueue<FieldCommand> received = new LinkedBlockingQueue<>();
    private final CompletableFuture<Status> outcome = new CompletableFuture<>();
    private volatile Metadata trailers = new Metadata();
    private final AtomicLong sequence = new AtomicLong();
    private volatile ClientCallStreamObserver<TeamMessage> requests;

    private DeviceClient(String deviceId) {
        this.deviceId = deviceId;
    }

    public static DeviceClient open(ManagedChannel channel, String token, String deviceId) {
        DeviceClient client = new DeviceClient(deviceId);
        TestTokens.withToken(FieldServiceGrpc.newStub(channel), token).teamChannel(client);
        return client;
    }

    /** Abre, se une y espera el Welcome. */
    public static DeviceClient join(ManagedChannel channel, String token, String deviceId, UUID shiftId, long lastCommandSequence) {
        DeviceClient client = open(channel, token, deviceId);
        client.join(shiftId, lastCommandSequence);
        return client;
    }

    public String deviceId() {
        return deviceId;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<TeamMessage> requestStream) {
        this.requests = requestStream;
    }

    @Override
    public void onNext(FieldCommand command) {
        received.add(command);
    }

    @Override
    public void onError(Throwable throwable) {
        Metadata received = Status.trailersFromThrowable(throwable);
        if (received != null) {
            trailers = received;
        }
        outcome.complete(Status.fromThrowable(throwable));
    }

    @Override
    public void onCompleted() {
        outcome.complete(Status.OK);
    }

    public void join(UUID shiftId, long lastCommandSequence) {
        send(message(0).setJoin(Join.newBuilder().setShiftId(shiftId.toString()).setLastCommandSequence(lastCommandSequence)).build());
    }

    public void heartbeat(String kp, int batteryPct, int signalDbm) {
        send(message(0).setHeartbeat(Heartbeat.newBuilder().setKp(kp).setBatteryPct(batteryPct).setSignalDbm(signalDbm)).build());
    }

    public long ack(String commandId) {
        return sendNext(builder -> builder.setCommandAck(CommandAck.newBuilder().setCommandId(commandId).setAccepted(true)));
    }

    public long clearOfTrack(boolean earthingRemoved) {
        return sendNext(builder -> builder.setClearOfTrack(ClearOfTrack.newBuilder().setEarthingRemoved(earthingRemoved)));
    }

    public long taskStarted(String orderId, String taskId) {
        return sendNext(builder -> builder.setTaskStarted(TaskStarted.newBuilder().setOrderId(orderId).setTaskId(taskId)));
    }

    public void taskStarted(long withSequence, String orderId, String taskId) {
        send(message(withSequence).setTaskStarted(TaskStarted.newBuilder().setOrderId(orderId).setTaskId(taskId)).build());
    }

    public long taskCompleted(String orderId, String taskId) {
        return sendNext(builder -> builder.setTaskCompleted(TaskCompleted.newBuilder().setOrderId(orderId).setTaskId(taskId).setWorkComplete(true)));
    }

    public void send(TeamMessage message) {
        requests.onNext(message);
    }

    private long sendNext(java.util.function.UnaryOperator<TeamMessage.Builder> body) {
        long next = sequence.incrementAndGet();
        send(body.apply(message(next)).build());
        return next;
    }

    private TeamMessage.Builder message(long withSequence) {
        Instant now = Instant.now();
        return TeamMessage.newBuilder().setDeviceId(deviceId).setSequence(withSequence)
                .setOccurredAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()));
    }

    /** Lo siguiente que llega, o un fallo del test si no llega a tiempo. */
    public FieldCommand next(Duration timeout) {
        try {
            FieldCommand command = received.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (command == null) {
                return fail("Device " + deviceId + " received nothing within " + timeout + " (outcome: " + outcome.getNow(null) + ")");
            }
            return command;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return fail("interrupted");
        }
    }

    public FieldCommand next() {
        return next(Duration.ofSeconds(10));
    }

    public Optional<FieldCommand> maybeNext(Duration timeout) {
        try {
            return Optional.ofNullable(received.poll(timeout.toMillis(), TimeUnit.MILLISECONDS));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        }
    }

    public FieldCommand awaitWelcome() {
        FieldCommand first = next();
        assertThat(first.hasWelcome()).as("lo primero tras el Join es el Welcome, no %s", first.getCommandCase()).isTrue();
        assertThat(first.getSequence()).isZero();
        return first;
    }

    public List<FieldCommand> nextN(int count) {
        List<FieldCommand> commands = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            commands.add(next());
        }
        return commands;
    }

    /** El siguiente que no sea un EventResult (lo que el servidor contesta a lo que este dispositivo sube). */
    public FieldCommand nextCommand() {
        while (true) {
            FieldCommand command = next();
            if (!command.hasEventResult()) {
                return command;
            }
        }
    }

    /** El siguiente EventResult, saltando lo demas. */
    public FieldCommand nextEventResult() {
        while (true) {
            FieldCommand command = next();
            if (command.hasEventResult()) {
                return command;
            }
        }
    }

    public int pending() {
        return received.size();
    }

    public void cancel() {
        requests.cancel("device lost coverage", null);
    }

    public void halfClose() {
        requests.onCompleted();
    }

    public boolean isFinished() {
        return outcome.isDone();
    }

    public Status outcome(Duration timeout) {
        try {
            return outcome.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException timedOut) {
            return fail("Stream of device " + deviceId + " did not end within " + timeout);
        } catch (ExecutionException | InterruptedException unexpected) {
            return fail("Stream of device " + deviceId + " failed strangely", unexpected);
        }
    }

    public Status outcome() {
        return outcome(Duration.ofSeconds(10));
    }

    /** El error con el que termino, con sus trailers (el ErrorInfo), para leer su {@code reason}. */
    public StatusRuntimeException outcomeError() {
        return outcome().asRuntimeException(trailers);
    }
}
