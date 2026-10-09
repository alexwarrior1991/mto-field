package com.alejandro.mtofield.support;

import com.alejandro.mtofield.grpc.v1.FieldServiceGrpc;
import com.alejandro.mtofield.grpc.v1.PossessionBoard;
import com.alejandro.mtofield.grpc.v1.WatchPossessionBoardRequest;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.Metadata;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.fail;

/**
 * Un observador de prueba de {@code WatchPossessionBoard}. En modo lento pide los tableros de uno
 * en uno ({@code disableAutoRequestWithInitial(1)} + {@link #request}), que sobre el transporte
 * in-process deja al servidor sin {@code isReady()} entre uno y otro: lo que hace falta para ver
 * que el servidor conflaciona en vez de encolar.
 */
public final class BoardClient implements ClientResponseObserver<WatchPossessionBoardRequest, PossessionBoard> {

    private final boolean slow;
    private final BlockingQueue<PossessionBoard> received = new LinkedBlockingQueue<>();
    private final CompletableFuture<Status> outcome = new CompletableFuture<>();
    private volatile Metadata trailers = new Metadata();
    private volatile ClientCallStreamObserver<WatchPossessionBoardRequest> requests;

    private BoardClient(boolean slow) {
        this.slow = slow;
    }

    public static BoardClient watch(ManagedChannel channel, String token, String possessionId) {
        return start(channel, token, possessionId, false);
    }

    public static BoardClient watchSlowly(ManagedChannel channel, String token, String possessionId) {
        return start(channel, token, possessionId, true);
    }

    private static BoardClient start(ManagedChannel channel, String token, String possessionId, boolean slow) {
        BoardClient client = new BoardClient(slow);
        TestTokens.withToken(FieldServiceGrpc.newStub(channel), token)
                .watchPossessionBoard(WatchPossessionBoardRequest.newBuilder().setPossessionId(possessionId).build(), client);
        return client;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<WatchPossessionBoardRequest> requestStream) {
        this.requests = requestStream;
        if (slow) {
            requestStream.disableAutoRequestWithInitial(1);
        }
    }

    @Override
    public void onNext(PossessionBoard board) {
        received.add(board);
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

    public void request(int count) {
        requests.request(count);
    }

    public void cancel() {
        requests.cancel("supervisor closed the board", null);
    }

    public PossessionBoard next(Duration timeout) {
        try {
            PossessionBoard board = received.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (board == null) {
                return fail("No board within " + timeout + " (outcome: " + outcome.getNow(null) + ")");
            }
            return board;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return fail("interrupted");
        }
    }

    public PossessionBoard next() {
        return next(Duration.ofSeconds(10));
    }

    public Optional<PossessionBoard> maybeNext(Duration timeout) {
        try {
            return Optional.ofNullable(received.poll(timeout.toMillis(), TimeUnit.MILLISECONDS));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        }
    }

    /** Lee tableros hasta que uno cumpla la condicion; los tableros son estados enteros, asi que los intermedios no importan. */
    public PossessionBoard awaitBoard(String description, Predicate<PossessionBoard> condition) {
        Instant deadline = Instant.now().plusSeconds(15);
        PossessionBoard last = null;
        while (Instant.now().isBefore(deadline)) {
            Optional<PossessionBoard> board = maybeNext(Duration.ofMillis(250));
            if (board.isPresent()) {
                last = board.get();
                if (condition.test(last)) {
                    return last;
                }
            }
        }
        return fail("No board satisfied '" + description + "' in time; last one: " + last);
    }

    public int pending() {
        return received.size();
    }

    public Status outcome(Duration timeout) {
        try {
            return outcome.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException timedOut) {
            return fail("The board stream did not end within " + timeout);
        } catch (ExecutionException | InterruptedException unexpected) {
            return fail("The board stream failed strangely", unexpected);
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
