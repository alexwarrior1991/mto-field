package com.alejandro.mtofield.application.dto;

import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** La posesion tal como la devuelven OpenPossession y ClosePossession. */
public record PossessionView(UUID id, String code, List<UUID> shiftIds, Instant endsAt, PossessionStatus status) {

    public PossessionView {
        shiftIds = List.copyOf(shiftIds);
    }
}
