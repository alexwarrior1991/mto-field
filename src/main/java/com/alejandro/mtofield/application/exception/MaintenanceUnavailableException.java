package com.alejandro.mtofield.application.exception;

/**
 * mto-maintenance no ha contestado: red, tiempo agotado, un 5xx, el circuito abierto o la cuenta
 * de servicio rechazada. Lo que se le iba a contar queda FAILED y se reintenta.
 */
public class MaintenanceUnavailableException extends BusinessException {

    public static final String REASON = "MAINTENANCE_UNAVAILABLE";

    public MaintenanceUnavailableException(String message) {
        super(REASON, message);
    }

    public MaintenanceUnavailableException(String message, Throwable cause) {
        super(REASON, message);
        initCause(cause);
    }
}
