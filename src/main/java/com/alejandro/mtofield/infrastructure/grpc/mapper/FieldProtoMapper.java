package com.alejandro.mtofield.infrastructure.grpc.mapper;

import com.alejandro.mtofield.application.dto.BoardSnapshot;
import com.alejandro.mtofield.application.dto.PossessionView;
import com.alejandro.mtofield.application.mapper.ProtoTimestamps;
import com.alejandro.mtofield.domain.model.TeamLiveness;
import com.alejandro.mtofield.grpc.v1.CommandState;
import com.alejandro.mtofield.grpc.v1.Possession;
import com.alejandro.mtofield.grpc.v1.PossessionBoard;
import com.alejandro.mtofield.grpc.v1.TeamState;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionStatus;

import java.util.UUID;

/**
 * De los DTO de la aplicacion a los mensajes del contrato, a mano: los builders de protobuf no son
 * beans y MapStruct no los conoce.
 */
public final class FieldProtoMapper {

    private FieldProtoMapper() {
    }

    public static Possession toProto(PossessionView view) {
        Possession.Builder builder = Possession.newBuilder()
                .setId(view.id().toString())
                .setCode(view.code())
                .setEndsAt(ProtoTimestamps.toProto(view.endsAt()))
                .setStatus(toProto(view.status()));
        view.shiftIds().forEach(shiftId -> builder.addShiftIds(shiftId.toString()));
        return builder.build();
    }

    public static Possession.Status toProto(PossessionStatus status) {
        return switch (status) {
            case OPEN -> Possession.Status.OPEN;
            case CLOSED -> Possession.Status.CLOSED;
        };
    }

    public static PossessionBoard toProto(BoardSnapshot board) {
        PossessionBoard.Builder builder = PossessionBoard.newBuilder()
                .setVersion(board.version())
                .setEndsAt(ProtoTimestamps.toProto(board.endsAt()))
                .setAllClear(board.allClear());
        for (BoardSnapshot.TeamState team : board.teams()) {
            TeamState.Builder state = TeamState.newBuilder()
                    .setShiftId(team.shiftId().toString())
                    .setTeamCode(team.teamCode())
                    .setLiveness(toProto(team.liveness()))
                    .setKp(team.kp() == null ? "" : team.kp())
                    .setBatteryPct(team.batteryPct())
                    .setClearOfTrack(team.clearOfTrack());
            if (team.lastSeen() != null) {
                state.setLastSeen(ProtoTimestamps.toProto(team.lastSeen()));
            }
            builder.addTeams(state);
        }
        for (BoardSnapshot.CommandState command : board.commands()) {
            builder.addCommands(CommandState.newBuilder()
                    .setCommandId(command.commandId().toString())
                    .setKind(command.kind().name())
                    .setIssuedAt(ProtoTimestamps.toProto(command.issuedAt()))
                    .addAllAckedBy(command.acks().ackedBy())
                    .addAllPending(command.acks().pending())
                    .addAllSentTo(command.acks().sentTo())
                    .addAllQueuedFor(command.acks().queuedFor()));
        }
        return builder.build();
    }

    public static TeamState.Liveness toProto(TeamLiveness.Liveness liveness) {
        return switch (liveness) {
            case CONNECTED -> TeamState.Liveness.CONNECTED;
            case STALE -> TeamState.Liveness.STALE;
            case DISCONNECTED -> TeamState.Liveness.DISCONNECTED;
        };
    }

    /** Un id del contrato como UUID; uno mal formado es INVALID_ARGUMENT, con el nombre del campo. */
    public static UUID uuid(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException(field + " is not a UUID: " + value);
        }
    }
}
