package com.alejandro.mtofield.application.exception;

import java.util.UUID;

public class PossessionNotFoundException extends BusinessException {

    public PossessionNotFoundException(UUID possessionId) {
        super("POSSESSION_NOT_FOUND", "Possession " + possessionId + " does not exist");
    }
}
