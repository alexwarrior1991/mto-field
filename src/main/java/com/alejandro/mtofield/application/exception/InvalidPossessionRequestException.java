package com.alejandro.mtofield.application.exception;

/** Una peticion mal formada sobre una posesion: sin turnos, un turno que no existe, fechas distintas. */
public class InvalidPossessionRequestException extends BusinessException {

    public InvalidPossessionRequestException(String message) {
        super("INVALID_POSSESSION_REQUEST", message);
    }
}
