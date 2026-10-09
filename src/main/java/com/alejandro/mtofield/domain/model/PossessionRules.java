package com.alejandro.mtofield.domain.model;

import java.time.Instant;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Las reglas de abrir y cerrar una posesion, sin framework. Un {@link IllegalArgumentException}
 * es una peticion mal formada ({@code INVALID_ARGUMENT}); un {@link ShiftNotWorkableException},
 * un turno que no puede entrar ({@code FAILED_PRECONDITION}).
 */
public final class PossessionRules {

    /** Los estados de un turno de mto-maintenance en los que ya no se trabaja. */
    public static final Set<String> TERMINAL_SHIFT_STATUSES = Set.of("CLOSED", "CANCELLED");

    private PossessionRules() {
    }

    /**
     * Los turnos que se agrupan: al menos uno, ninguno repetido, todos del mismo dia y ninguno
     * terminado.
     *
     * @return la fecha comun de los turnos
     */
    public static LocalDate validateShifts(List<ShiftSnapshot> shifts) {
        if (shifts == null || shifts.isEmpty()) {
            throw new IllegalArgumentException("A possession needs at least one shift");
        }
        Set<UUID> seen = new HashSet<>();
        LocalDate date = null;
        for (ShiftSnapshot shift : shifts) {
            Objects.requireNonNull(shift, "shift");
            if (!seen.add(shift.id())) {
                throw new IllegalArgumentException("Shift " + shift.id() + " is repeated");
            }
            if (shift.shiftDate() == null) {
                throw new IllegalArgumentException("Shift " + shift.id() + " has no date");
            }
            if (date == null) {
                date = shift.shiftDate();
            } else if (!date.equals(shift.shiftDate())) {
                throw new IllegalArgumentException("The shifts are not on the same date: " + date + " and " + shift.shiftDate());
            }
            if (shift.status() != null && TERMINAL_SHIFT_STATUSES.contains(shift.status())) {
                throw new ShiftNotWorkableException(shift.id(), shift.status());
            }
        }
        return date;
    }

    /** Sin {@code ends_at}, la posesion acaba con el primer turno que acaba. */
    public static Instant defaultEndsAt(List<ShiftSnapshot> shifts) {
        return shifts.stream()
                .map(ShiftSnapshot::plannedEnd)
                .filter(Objects::nonNull)
                .min(Instant::compareTo)
                .orElseThrow(() -> new IllegalArgumentException("ends_at is required: none of the shifts has a planned end"));
    }

    /** Un cierre forzado (sin que todos hayan salido de la via) deja constancia del motivo. */
    public static void validateCloseRequest(boolean force, String reason) {
        if (force && (reason == null || reason.isBlank())) {
            throw new IllegalArgumentException("A forced close needs a reason");
        }
    }
}
