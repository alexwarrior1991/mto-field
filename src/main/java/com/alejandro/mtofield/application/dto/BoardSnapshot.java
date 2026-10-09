package com.alejandro.mtofield.application.dto;

import com.alejandro.mtofield.domain.model.CommandAckSummary;
import com.alejandro.mtofield.domain.model.TeamLiveness;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldCommandKind;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * El tablero de una posesion en un instante: lo que {@code WatchPossessionBoard} manda entero
 * cada vez (conflado: solo importa el ultimo). La version crece con cada publicacion de la
 * posesion en esta replica y se siembra con el reloj, para que un reinicio no la haga retroceder.
 */
public record BoardSnapshot(
        UUID possessionId,
        long version,
        PossessionStatus status,
        Instant endsAt,
        boolean allClear,
        List<TeamState> teams,
        List<CommandState> commands
) {

    public BoardSnapshot {
        teams = List.copyOf(teams);
        commands = List.copyOf(commands);
    }

    /**
     * @param lastSeen {@code null} si ningun dispositivo del equipo ha hablado con esta replica
     * @param kp       el ultimo kp latido, o vacio
     */
    public record TeamState(
            UUID shiftId,
            String teamCode,
            TeamLiveness.Liveness liveness,
            Instant lastSeen,
            String kp,
            int batteryPct,
            boolean clearOfTrack
    ) {
    }

    public record CommandState(
            UUID commandId,
            long sequence,
            FieldCommandKind kind,
            Instant issuedAt,
            CommandAckSummary acks
    ) {
    }

    public long connectedTeams() {
        return teams.stream().filter(team -> team.liveness() == TeamLiveness.Liveness.CONNECTED).count();
    }

    public long commandsPendingAck() {
        return commands.stream().filter(command -> !command.acks().allAcked()).count();
    }
}
