package com.alejandro.mtofield.application.exception;

import java.util.UUID;

public class PossessionNotOpenException extends BusinessException {

    public PossessionNotOpenException(UUID possessionId) {
        super("POSSESSION_NOT_OPEN", "Possession " + possessionId + " is not open");
    }
}
