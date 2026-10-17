package com.alejandro.mtofield.domain.model;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * Lo que mto-field necesita saber de un turno de mto-maintenance para agruparlo en una posesion.
 * Es un subconjunto de su {@code MaintenanceShiftResponse}: solo las claves que se usan, para que
 * un campo nuevo alla no rompa nada aqui.
 *
 * <p>{@code crewUsernames} es el usuario de Keycloak de cada persona de su dotacion de mto-resources
 * (jefe, supervisor y linemen; las personas sin usuario no cuentan) y {@code vehicleIds}, sus
 * vehiculos: vacios en un turno sin dotacion, que se liga por el grupo de su equipo.</p>
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
        List<Long> trackIds,
        Long executionPackageId,
        List<String> crewUsernames,
        List<UUID> vehicleIds
) {

    public ShiftSnapshot {
        trackIds = trackIds == null ? List.of() : List.copyOf(trackIds);
        crewUsernames = crewUsernames == null ? List.of() : crewUsernames.stream()
                .filter(username -> username != null && !username.isBlank())
                .distinct()
                .toList();
        vehicleIds = vehicleIds == null ? List.of() : List.copyOf(vehicleIds);
    }

    /** Un turno sin dotacion ni vehiculos: el de un mto-maintenance anterior, o el sintetico del cliente apagado. */
    public ShiftSnapshot(UUID id, String code, LocalDate shiftDate, String status, String teamCode, String teamName, Instant plannedStart,
                         Instant plannedEnd, List<Long> trackIds) {
        this(id, code, shiftDate, status, teamCode, teamName, plannedStart, plannedEnd, trackIds, null, List.of(), List.of());
    }
}
