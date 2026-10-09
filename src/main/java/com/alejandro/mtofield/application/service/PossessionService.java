package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.application.dto.PossessionView;
import com.alejandro.mtofield.application.dto.ShiftMembership;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Abrir y cerrar posesiones, y saber en cual esta un turno. */
public interface PossessionService {

    /** @param endsAt {@code null} toma el menor fin previsto de los turnos */
    PossessionView open(List<UUID> shiftIds, Instant endsAt, String openedBy);

    PossessionView close(UUID possessionId, boolean force, String reason, String closedBy);

    PossessionView get(UUID possessionId);

    Optional<ShiftMembership> membershipOfOpenPossession(UUID shiftId);
}
