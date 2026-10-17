package com.alejandro.mtofield.application.dto;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * La posesion abierta en la que esta un turno: lo que un Join necesita para aceptarse. Con la dotacion
 * del turno ({@code crewUsernames}, en minusculas) un Join se liga por la persona; sin ella, por el
 * grupo del equipo. Sus vehiculos y sus vias son los de la posicion que manda el dispositivo.
 */
public record ShiftMembership(UUID possessionId, UUID shiftId, String teamCode, Instant endsAt, String possessionCode,
                              List<String> crewUsernames, List<UUID> vehicleIds, List<Long> trackIds, Long executionPackageId) {

    public ShiftMembership {
        crewUsernames = crewUsernames == null ? List.of() : List.copyOf(crewUsernames);
        vehicleIds = vehicleIds == null ? List.of() : List.copyOf(vehicleIds);
        trackIds = trackIds == null ? List.of() : List.copyOf(trackIds);
    }

    /** Un turno sin dotacion ni vehiculos. */
    public ShiftMembership(UUID possessionId, UUID shiftId, String teamCode, Instant endsAt) {
        this(possessionId, shiftId, teamCode, endsAt, null, List.of(), List.of(), List.of(), null);
    }

    public boolean hasCrew() {
        return !crewUsernames.isEmpty();
    }

    public boolean isInCrew(String username) {
        return username != null && crewUsernames.contains(username.trim().toLowerCase(Locale.ROOT));
    }
}
