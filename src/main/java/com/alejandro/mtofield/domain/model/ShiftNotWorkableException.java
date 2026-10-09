package com.alejandro.mtofield.domain.model;

import java.util.UUID;

/** Un turno que mto-maintenance ya dio por terminado (CLOSED o CANCELLED) no entra en una posesion. */
public class ShiftNotWorkableException extends RuntimeException {

    private final UUID shiftId;
    private final String status;

    public ShiftNotWorkableException(UUID shiftId, String status) {
        super("Shift " + shiftId + " is " + status + " and cannot join a possession");
        this.shiftId = shiftId;
        this.status = status;
    }

    public UUID getShiftId() {
        return shiftId;
    }

    public String getStatus() {
        return status;
    }
}
