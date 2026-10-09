package com.alejandro.mtofield.simulator;

import com.alejandro.mtofield.grpc.v1.ClosePossessionRequest;
import com.alejandro.mtofield.grpc.v1.CommandState;
import com.alejandro.mtofield.grpc.v1.EvacuateNow;
import com.alejandro.mtofield.grpc.v1.FieldServiceGrpc;
import com.alejandro.mtofield.grpc.v1.IssueCommandRequest;
import com.alejandro.mtofield.grpc.v1.IssueCommandResponse;
import com.alejandro.mtofield.grpc.v1.OpenPossessionRequest;
import com.alejandro.mtofield.grpc.v1.Possession;
import com.alejandro.mtofield.grpc.v1.PossessionBoard;
import com.alejandro.mtofield.grpc.v1.SupervisorMessage;
import com.alejandro.mtofield.grpc.v1.TeamState;
import com.alejandro.mtofield.grpc.v1.WatchPossessionBoardRequest;
import com.alejandro.mtofield.infrastructure.grpc.GrpcErrors;
import com.alejandro.mtofield.support.TestTokens;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/** El responsable: abre la posesion, mira el tablero, ordena el desalojo y cierra. */
final class SupervisorConsole {

    private static final String WHO = "supervisor";

    private final ManagedChannel channel;
    private final TokenClient tokens;
    private final AtomicReference<PossessionBoard> latest = new AtomicReference<>();
    private volatile Thread watcher;

    SupervisorConsole(ManagedChannel channel, TokenClient tokens) {
        this.channel = channel;
        this.tokens = tokens;
    }

    /** Un stub con el token de ahora: tras un UNAUTHENTICATED el siguiente sale con uno nuevo. */
    private FieldServiceGrpc.FieldServiceBlockingStub stub() {
        try {
            return TestTokens.withToken(FieldServiceGrpc.newBlockingStub(channel), tokens.supervisorToken());
        } catch (IOException noToken) {
            throw new IllegalStateException("no supervisor token: " + noToken.getMessage(), noToken);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while getting a token", interrupted);
        }
    }

    private <T> T withRenewal(java.util.function.Function<FieldServiceGrpc.FieldServiceBlockingStub, T> call) {
        try {
            return call.apply(stub());
        } catch (StatusRuntimeException failed) {
            if (failed.getStatus().getCode() != io.grpc.Status.Code.UNAUTHENTICATED) {
                throw failed;
            }
            Log.info(WHO, "token expired or rejected: renewing it and retrying");
            tokens.invalidate();
            return call.apply(stub());
        }
    }

    Possession open(List<UUID> shiftIds) {
        OpenPossessionRequest.Builder request = OpenPossessionRequest.newBuilder();
        shiftIds.forEach(shiftId -> request.addShiftIds(shiftId.toString()));
        Possession possession = withRenewal(stub -> stub.openPossession(request.build()));
        Log.info(WHO, "possession " + possession.getCode() + " (" + possession.getId() + ") opened with " + possession.getShiftIdsCount()
                + " shift(s), ends at " + time(possession.getEndsAt().getSeconds()));
        Log.info(WHO, "shift ids: " + String.join(",", possession.getShiftIdsList()));
        return possession;
    }

    /** Mira el tablero en un hilo propio y lo pinta cada vez que llega una version; si el token caduca, vuelve con uno nuevo. */
    void watch(String possessionId) {
        watcher = Thread.ofVirtual().name("sim-board").start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Iterator<PossessionBoard> boards = stub()
                            .watchPossessionBoard(WatchPossessionBoardRequest.newBuilder().setPossessionId(possessionId).build());
                    while (boards.hasNext()) {
                        PossessionBoard board = boards.next();
                        latest.set(board);
                        print(board);
                    }
                    Log.info(WHO, "board stream completed: the possession is closed");
                    return;
                } catch (StatusRuntimeException failed) {
                    Log.info(WHO, "board stream ended: " + failed.getStatus().getCode() + " " + GrpcErrors.reasonOf(failed));
                    if (failed.getStatus().getCode() != io.grpc.Status.Code.UNAUTHENTICATED) {
                        return;
                    }
                    tokens.invalidate();
                    Log.info(WHO, "token expired: watching the board again with a new one");
                }
            }
        });
    }

    PossessionBoard latestBoard() {
        return latest.get();
    }

    IssueCommandResponse evacuate(String possessionId, String reason) {
        IssueCommandResponse response = withRenewal(stub -> stub.issueCommand(IssueCommandRequest.newBuilder()
                .setPossessionId(possessionId)
                .setIdempotencyKey("evacuate-" + UUID.randomUUID())
                .setEvacuateNow(EvacuateNow.newBuilder().setReason(reason))
                .build()));
        Log.info(WHO, "EVACUATE_NOW issued: command " + response.getCommandId() + " #" + response.getSequence());
        return response;
    }

    IssueCommandResponse say(String possessionId, String text) {
        IssueCommandResponse response = withRenewal(stub -> stub.issueCommand(IssueCommandRequest.newBuilder()
                .setPossessionId(possessionId)
                .setSupervisorMessage(SupervisorMessage.newBuilder().setAuthor("supervisor").setText(text))
                .build()));
        Log.info(WHO, "message issued #" + response.getSequence() + ": " + text);
        return response;
    }

    Possession close(String possessionId, boolean force, String reason) {
        try {
            Possession closed = withRenewal(stub -> stub.closePossession(ClosePossessionRequest.newBuilder().setPossessionId(possessionId).setForce(force).setReason(reason).build()));
            Log.info(WHO, "possession " + closed.getCode() + " closed" + (force ? " (forced: " + reason + ")" : ""));
            return closed;
        } catch (StatusRuntimeException refused) {
            Log.info(WHO, "close refused: " + refused.getStatus().getCode() + " " + GrpcErrors.reasonOf(refused) + " " + GrpcErrors.metadataOf(refused));
            throw refused;
        }
    }

    void stopWatching() {
        Thread thread = watcher;
        if (thread != null) {
            thread.interrupt();
        }
    }

    private static void print(PossessionBoard board) {
        StringBuilder text = new StringBuilder();
        text.append("board v").append(board.getVersion()).append(" all_clear=").append(board.getAllClear())
                .append(" ends=").append(time(board.getEndsAt().getSeconds())).append('\n');
        for (TeamState team : board.getTeamsList()) {
            String seen = team.hasLastSeen() ? (Instant.now().getEpochSecond() - team.getLastSeen().getSeconds()) + "s ago" : "never";
            text.append(String.format("    %-10s %-12s kp %-9s bat %3d%%  seen %-9s clear=%s%n", team.getTeamCode(), team.getLiveness(),
                    team.getKp().isBlank() ? "-" : team.getKp(), team.getBatteryPct(), seen, team.getClearOfTrack() ? "yes" : "no"));
        }
        for (CommandState command : board.getCommandsList()) {
            text.append(String.format("    %s issued %s acked=%s pending=%s sent=%s queued=%s%n", command.getKind(),
                    time(command.getIssuedAt().getSeconds()), command.getAckedByList(), command.getPendingList(), command.getSentToList(),
                    command.getQueuedForList()));
        }
        Log.info(WHO, text.toString().stripTrailing());
    }

    private static String time(long epochSeconds) {
        return LocalTime.ofInstant(Instant.ofEpochSecond(epochSeconds), ZoneId.systemDefault()).withNano(0).toString();
    }
}
