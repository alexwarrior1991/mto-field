package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.grpc.v1.PossessionBoard;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Un observador de {@code WatchPossessionBoard}. Guarda solo el ULTIMO tablero: una publicacion
 * sobrescribe la anterior, asi que un observador lento se salta versiones en vez de acumular cola.
 * Escribe con el mismo CAS y la misma re-comprobacion que {@link DeviceStream}.
 */
public final class BoardWatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(BoardWatcher.class);

    private record Terminal(StatusRuntimeException error) {
    }

    private final ServerCallStreamObserver<PossessionBoard> out;
    private final Consumer<BoardWatcher> onClosed;
    private final Instant tokenExpiresAt;
    private final AtomicReference<PossessionBoard> latest = new AtomicReference<>();
    private final AtomicBoolean draining = new AtomicBoolean();
    private final AtomicReference<Terminal> terminal = new AtomicReference<>();
    private volatile boolean terminated;
    private volatile long lastSentVersion;

    public BoardWatcher(ServerCallStreamObserver<PossessionBoard> out, Instant tokenExpiresAt, Consumer<BoardWatcher> onClosed) {
        this.out = out;
        this.tokenExpiresAt = tokenExpiresAt;
        this.onClosed = onClosed;
    }

    /** Cuando caduca el token con el que se abrio, o {@code null} si no se sabe; lo lee el barrido de {@code TokenExpirySweeper}. */
    public Instant tokenExpiresAt() {
        return tokenExpiresAt;
    }

    public long lastSentVersion() {
        return lastSentVersion;
    }

    public boolean isClosed() {
        return terminal.get() != null;
    }

    public void publish(PossessionBoard board) {
        if (terminal.get() != null) {
            return;
        }
        latest.set(board);
        drain();
    }

    /** Fin normal: la posesion se ha cerrado; el ultimo tablero publicado sale antes. */
    public void complete() {
        terminate(new Terminal(null));
    }

    public void fail(StatusRuntimeException error) {
        terminate(new Terminal(error));
    }

    /** El cliente cancelo: nada mas se escribe. */
    public void abandon() {
        if (terminal.compareAndSet(null, new Terminal(null))) {
            terminated = true;
            latest.set(null);
            onClosed.accept(this);
        }
    }

    private void terminate(Terminal candidate) {
        if (terminal.compareAndSet(null, candidate)) {
            onClosed.accept(this);
        }
        drain();
    }

    public void drain() {
        do {
            if (!draining.compareAndSet(false, true)) {
                return;
            }
            try {
                Terminal current = terminal.get();
                PossessionBoard board;
                while (out.isReady() && (board = latest.getAndSet(null)) != null) {
                    try {
                        out.onNext(board);
                        lastSentVersion = board.getVersion();
                    } catch (RuntimeException gone) {
                        LOGGER.debug("Board could not be written, watcher closed: {}", gone.toString());
                        abandon();
                        break;
                    }
                    current = terminal.get();
                    if (current != null) {
                        break;
                    }
                }
                if (current != null && !terminated && latest.get() == null) {
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
        return latest.get() != null && out.isReady();
    }

    private void emit(Terminal current) {
        terminated = true;
        try {
            if (current.error() == null) {
                out.onCompleted();
            } else {
                out.onError(current.error());
            }
        } catch (RuntimeException alreadyGone) {
            LOGGER.debug("Terminal of a board watcher could not be written: {}", alreadyGone.toString());
        }
    }
}
