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

    /** La nueva hora de fin de una posesion abierta (lo que un WindowChanged cuenta a los dispositivos). */
    PossessionView changeEndsAt(UUID possessionId, Instant endsAt, String changedBy);

    PossessionView get(UUID possessionId);

    Optional<ShiftMembership> membershipOfOpenPossession(UUID shiftId);

    /**
     * Relee el turno en mto-maintenance y guarda su dotacion y sus vehiculos de ahora: lo que hace un
     * Join que no encuentra a la persona en la dotacion copiada al abrir (un suplente anadido despues).
     * Sin el cliente, o si mantenimiento no contesta, devuelve lo que ya habia.
     */
    Optional<ShiftMembership> refreshCrew(UUID shiftId);
}
