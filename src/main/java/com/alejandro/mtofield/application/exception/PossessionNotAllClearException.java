package com.alejandro.mtofield.application.exception;

import java.util.List;
import java.util.UUID;

/** Cerrar sin {@code force} con equipos todavia en la via. Lleva quienes faltan. */
public class PossessionNotAllClearException extends BusinessException {

    private final List<String> pendingTeams;

    public PossessionNotAllClearException(UUID possessionId, List<String> pendingTeams) {
        super("POSSESSION_NOT_ALL_CLEAR", "Possession " + possessionId + " still has teams on the track: " + String.join(", ", pendingTeams));
        this.pendingTeams = List.copyOf(pendingTeams);
    }

    public List<String> getPendingTeams() {
        return pendingTeams;
    }
}
