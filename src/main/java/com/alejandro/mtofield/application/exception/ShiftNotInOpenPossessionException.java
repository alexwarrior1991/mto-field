package com.alejandro.mtofield.application.exception;

import java.util.UUID;

public class ShiftNotInOpenPossessionException extends BusinessException {

    public ShiftNotInOpenPossessionException(UUID shiftId) {
        super("SHIFT_NOT_IN_OPEN_POSSESSION", "Shift " + shiftId + " is not in an open possession");
    }
}
