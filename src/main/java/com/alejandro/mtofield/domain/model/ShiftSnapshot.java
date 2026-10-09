package com.alejandro.mtofield.domain.model;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * Lo que mto-field necesita saber de un turno de mto-maintenance para agruparlo en una posesion.
 * Es un subconjunto de su {@code MaintenanceShiftResponse}: solo las claves que se usan, para que
 * un campo nuevo alla no rompa nada aqui.
 */
public record ShiftSnapshot(
        UUID id,
        String code,
        LocalDate shiftDate,
        String status,
        String teamCode,
        String teamName,
        Instant plannedStart,
        Instant plannedEnd,
        List<Long> trackIds
) {

    public ShiftSnapshot {
        trackIds = trackIds == null ? List.of() : List.copyOf(trackIds);
    }
}
