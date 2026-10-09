package com.alejandro.mtofield.application.dto;

import java.time.Instant;
import java.util.UUID;

/** La posesion abierta en la que esta un turno: lo que un Join necesita para aceptarse. */
public record ShiftMembership(UUID possessionId, UUID shiftId, String teamCode, Instant endsAt) {
}
