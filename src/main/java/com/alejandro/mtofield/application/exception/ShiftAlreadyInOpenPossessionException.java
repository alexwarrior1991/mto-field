package com.alejandro.mtofield.application.exception;

import java.util.UUID;

public class ShiftAlreadyInOpenPossessionException extends BusinessException {

    public ShiftAlreadyInOpenPossessionException(UUID shiftId) {
        super("SHIFT_ALREADY_IN_OPEN_POSSESSION", "Shift " + shiftId + " is already in an open possession");
    }
}
