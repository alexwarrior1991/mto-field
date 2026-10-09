package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.application.dto.DevicePrincipal;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.infrastructure.grpc.GrpcErrors;
import com.alejandro.mtofield.infrastructure.grpc.metrics.FieldMetrics;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * La bajada de un {@code TeamChannel}: un stream por dispositivo, abierto toda la noche.
 *
 * <p>Invariantes:</p>
 * <ul>
 *   <li><b>Un solo escritor.</b> En {@code out} solo escribe el hilo que gana el CAS de
 *       {@code draining}; quien ofrece una orden la deja en la cola y, como mucho, drena. El
 *       terminal ({@code onCompleted} o {@code onError}) lo emite el drenador, exactamente una vez.</li>
 *   <li><b>Catch-up y vivo no se mezclan.</b> Mientras se reproduce el atraso, lo que llega en vivo
 *       se retiene; al volcar se descarta lo que la reproduccion ya cubrio.</li>
 *   <li><b>Nunca una secuencia menor o igual que la ultima encolada.</b> El despachador puede
 *       reenviar; el stream no duplica.</li>
 * </ul>
 *
 * <p>El drenado vuelve a comprobar la cola despues de soltar el CAS: una oferta que llego mientras
 * se drenaba, y cuyo propio drenado perdio el CAS, no se queda esperando a nadie.</p>
 */
public final class DeviceStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceStream.class);

    public static final String REASON_SUPERSEDED = "SUPERSEDED";
    public static final String REASON_OUTBOUND_FULL = "OUTBOUND_QUEUE_FULL";
    public static final String REASON_NOT_READING = "DEVICE_NOT_READING";

    public enum Mode { CATCHING_UP, LIVE, CLOSED }

    /** {@code error == null} es un fin normal ({@code onCompleted}). */
    private record Terminal(StatusRuntimeException error) {
    }

    private final String deviceId;
    private final UUID shiftId;
    private final UUID possessionId;
    private final String teamCode;
    private final DevicePrincipal principal;
    private final Instant tokenExpiresAt;
    private final ServerCallStreamObserver<FieldCommand> out;
    private final ArrayBlockingQueue<FieldCommand> outbound;
    private final int heldCapacity;
    private final int catchUpPageSize;
    private final Duration putTimeout;
    private final FieldMetrics metrics;
    private final Consumer<DeviceStream> onClosed;

    private final Object stateLock = new Object();
    private final List<FieldCommand> held = new ArrayList<>();
    private Mode mode = Mode.CATCHING_UP;
    private long lastEnqueued;

    private final AtomicBoolean draining = new AtomicBoolean();
    private final AtomicReference<Terminal> terminal = new AtomicReference<>();
    private volatile boolean terminated;
    private volatile long lastSentSequence;
    private volatile long notReadySinceNanos;

    public DeviceStream(String deviceId, UUID shiftId, UUID possessionId, String teamCode, DevicePrincipal principal, Instant tokenExpiresAt,
                        ServerCallStreamObserver<FieldCommand> out, int outboundCapacity, int heldCapacity, int catchUpPageSize,
                        Duration putTimeout, FieldMetrics metrics, Consumer<DeviceStream> onClosed) {
        this.deviceId = deviceId;
        this.shiftId = shiftId;
        this.possessionId = possessionId;
        this.teamCode = teamCode;
        this.principal = principal;
        this.tokenExpiresAt = tokenExpiresAt;
        this.out = out;
        this.outbound = new ArrayBlockingQueue<>(outboundCapacity);
        this.heldCapacity = heldCapacity;
        this.catchUpPageSize = catchUpPageSize;
        this.putTimeout = putTimeout;
        this.metrics = metrics;
        this.onClosed = onClosed;
    }

    public String deviceId() {
        return deviceId;
    }

    public UUID shiftId() {
        return shiftId;
    }

    public UUID possessionId() {
        return possessionId;
    }

    public String teamCode() {
        return teamCode;
    }

    public DevicePrincipal principal() {
        return principal;
    }

    public Instant tokenExpiresAt() {
        return tokenExpiresAt;
    }

    /** La mayor secuencia ya escrita en el transporte: lo que el tablero llama "enviada". */
    public long lastSentSequence() {
        return lastSentSequence;
    }

    public int outboundDepth() {
        return outbound.size();
    }

    public Mode mode() {
        synchronized (stateLock) {
            return mode;
        }
    }

    public boolean isClosed() {
        return mode() == Mode.CLOSED;
    }

    /**
     * Una orden en vivo. En catch-up se retiene; en vivo se encola; cerrada o ya encolada
     * (secuencia menor o igual que la ultima), se ignora. Nunca escribe en {@code out} desde aqui
     * salvo que gane el drenado.
     */
    public void offer(FieldCommand command) {
        StatusRuntimeException failure = null;
        synchronized (stateLock) {
            if (mode == Mode.CLOSED || command.getSequence() <= lastEnqueued) {
                return;
            }
            if (mode == Mode.CATCHING_UP) {
                if (held.size() >= heldCapacity) {
                    failure = GrpcErrors.of(Status.Code.RESOURCE_EXHAUSTED, REASON_OUTBOUND_FULL,
                            "device " + deviceId + " fell too far behind during catch-up");
                } else {
                    held.add(command);
                    lastEnqueued = command.getSequence();
                    return;
                }
            } else if (outbound.offer(command)) {
                lastEnqueued = command.getSequence();
            } else {
                failure = GrpcErrors.of(Status.Code.RESOURCE_EXHAUSTED, REASON_OUTBOUND_FULL,
                        "device " + deviceId + " is not reading: outbound queue full");
            }
        }
        if (failure != null) {
            fail(failure);
            return;
        }
        drain();
    }

    /**
     * Reproduce el atraso y pasa a vivo. Corre en su propio hilo, nunca en el del callback: la
     * reproduccion espera a que el dispositivo lea.
     *
     * @param after   la ultima orden que el dispositivo aplico ({@code Join.last_command_sequence})
     * @param welcome el Welcome, que va el primero y con secuencia 0
     */
    public void catchUp(long after, FieldCommand welcome, ReplaySource replay, CatchUpProbe probe) {
        try {
            put(welcome);
            probe.afterRegistered(this);
            long cursor = after;
            while (true) {
                List<FieldCommand> page = replay.after(cursor, catchUpPageSize);
                for (FieldCommand command : page) {
                    put(command);
                    cursor = command.getSequence();
                }
                if (page.size() < catchUpPageSize) {
                    break;
                }
            }
            probe.beforeFlush(this);
            flush(cursor);
            drain();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            fail(GrpcErrors.of(Status.Code.UNAVAILABLE, "SHUTTING_DOWN", "server is shutting down"));
        } catch (StreamClosedException closed) {
            LOGGER.debug("Catch-up of device {} abandoned: stream closed", deviceId);
        } catch (RuntimeException unexpected) {
            LOGGER.error("Catch-up of device {} failed", deviceId, unexpected);
            fail(GrpcErrors.of(Status.Code.INTERNAL, "CATCH_UP_FAILED", "catch-up failed: " + unexpected.getMessage()));
        }
    }

    /** Encola esperando sitio (acotado) y drena; si el dispositivo no lee, cierra el stream. */
    private void put(FieldCommand command) throws InterruptedException {
        long deadline = System.nanoTime() + putTimeout.toNanos();
        while (true) {
            if (isClosed()) {
                throw new StreamClosedException();
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                fail(GrpcErrors.of(Status.Code.RESOURCE_EXHAUSTED, REASON_NOT_READING, "device " + deviceId + " did not read during catch-up"));
                throw new StreamClosedException();
            }
            if (outbound.offer(command, Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(200)), TimeUnit.NANOSECONDS)) {
                drain();
                return;
            }
            drain();
        }
    }

    /** Vuelca lo retenido que la reproduccion no cubrio y pasa a vivo. */
    private void flush(long replayedUpTo) throws InterruptedException {
        awaitRoomForHeld();
        boolean overflow = false;
        synchronized (stateLock) {
            if (mode == Mode.CLOSED) {
                throw new StreamClosedException();
            }
            for (FieldCommand command : held) {
                if (command.getSequence() > replayedUpTo && !outbound.offer(command)) {
                    overflow = true;
                    break;
                }
            }
            held.clear();
            if (!overflow) {
                lastEnqueued = Math.max(lastEnqueued, replayedUpTo);
                mode = Mode.LIVE;
            }
        }
        if (overflow) {
            fail(GrpcErrors.of(Status.Code.RESOURCE_EXHAUSTED, REASON_OUTBOUND_FULL, "device " + deviceId + " is not reading: outbound queue full"));
            throw new StreamClosedException();
        }
    }

    private void awaitRoomForHeld() throws InterruptedException {
        long deadline = System.nanoTime() + putTimeout.toNanos();
        while (true) {
            int needed;
            synchronized (stateLock) {
                if (mode == Mode.CLOSED) {
                    throw new StreamClosedException();
                }
                needed = held.size();
            }
            if (outbound.remainingCapacity() >= needed) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                fail(GrpcErrors.of(Status.Code.RESOURCE_EXHAUSTED, REASON_NOT_READING, "device " + deviceId + " did not read during catch-up"));
                throw new StreamClosedException();
            }
            drain();
            Thread.sleep(20);
        }
    }

    /** Cierra con error. Idempotente: el primero gana y el drenador lo emite. */
    public void fail(StatusRuntimeException error) {
        terminate(new Terminal(error));
    }

    /** Fin normal: la posesion se ha cerrado. */
    public void complete() {
        terminate(new Terminal(null));
    }

    /** Otro stream del mismo dispositivo ha tomado el relevo. */
    public void supersede() {
        terminate(new Terminal(GrpcErrors.of(Status.Code.ABORTED, REASON_SUPERSEDED,
                "another stream of device " + deviceId + " superseded this one")));
    }

    /**
     * La llamada ya no esta (el cliente cancelo o la red cayo): no se escribe nada mas, ni un
     * terminal, y solo queda limpiar. Idempotente.
     */
    public void abandon() {
        synchronized (stateLock) {
            mode = Mode.CLOSED;
            held.clear();
        }
        if (terminal.compareAndSet(null, new Terminal(GrpcErrors.of(Status.Code.CANCELLED, "CLIENT_GONE", "client cancelled the call")))) {
            terminated = true;
            outbound.clear();
            try {
                onClosed.accept(this);
            } catch (RuntimeException exception) {
                LOGGER.warn("Close callback of device {} failed", deviceId, exception);
            }
        }
    }

    private void terminate(Terminal candidate) {
        synchronized (stateLock) {
            mode = Mode.CLOSED;
            held.clear();
        }
        if (terminal.compareAndSet(null, candidate)) {
            try {
                onClosed.accept(this);
            } catch (RuntimeException exception) {
                LOGGER.warn("Close callback of device {} failed", deviceId, exception);
            }
        }
        drain();
    }

    /**
     * Escribe en {@code out} mientras este listo y haya algo, y emite el terminal si lo hay. Lo
     * llama quien encola, el {@code onReadyHandler} del transporte y el propio catch-up; solo uno
     * escribe a la vez.
     */
    public void drain() {
        do {
            if (!draining.compareAndSet(false, true)) {
                return;
            }
            try {
                Terminal current = terminal.get();
                while (current == null && out.isReady()) {
                    FieldCommand next = outbound.poll();
                    if (next == null) {
                        break;
                    }
                    readyAgain();
                    try {
                        out.onNext(next);
                    } catch (RuntimeException gone) {
                        // La llamada ya esta cancelada por el cliente: el stream termina aqui, sin
                        // escribir nada mas, y el cierre llega por el onCancelHandler.
                        LOGGER.debug("Write to device {} failed, closing the stream: {}", deviceId, gone.toString());
                        abandon();
                        current = terminal.get();
                        break;
                    }
                    if (next.getSequence() > 0) {
                        lastSentSequence = next.getSequence();
                    }
                    current = terminal.get();
                }
                if (current == null && !outbound.isEmpty() && !out.isReady()) {
                    notReadyNow();
                }
                if (current != null && !terminated) {
                    emit(current);
                }
            } finally {
                draining.set(false);
            }
        } while (shouldDrainAgain());
    }

    private boolean shouldDrainAgain() {
        if (terminated) {
            return false;
        }
        if (terminal.get() != null) {
            return true;
        }
        return !outbound.isEmpty() && out.isReady();
    }

    private void emit(Terminal current) {
        terminated = true;
        outbound.clear();
        try {
            if (current.error() == null) {
                out.onCompleted();
            } else {
                out.onError(current.error());
            }
        } catch (RuntimeException alreadyGone) {
            // La llamada ya estaba cancelada por el cliente: no hay a quien contarselo.
            LOGGER.debug("Terminal of device {} could not be written: {}", deviceId, alreadyGone.toString());
        }
    }

    private void notReadyNow() {
        if (notReadySinceNanos == 0) {
            notReadySinceNanos = System.nanoTime();
        }
    }

    private void readyAgain() {
        long since = notReadySinceNanos;
        if (since != 0) {
            notReadySinceNanos = 0;
            metrics.recordNotReady(Duration.ofNanos(System.nanoTime() - since));
        }
    }

    @Override
    public String toString() {
        return "DeviceStream{" + deviceId + ", shift " + shiftId + ", possession " + possessionId + ", " + mode() + "}";
    }

    static final class StreamClosedException extends RuntimeException {
        StreamClosedException() {
            super("stream closed", null, false, false);
        }
    }
}
