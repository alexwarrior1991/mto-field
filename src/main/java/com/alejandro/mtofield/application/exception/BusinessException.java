package com.alejandro.mtofield.application.exception;

/**
 * Raiz de los errores de negocio que cierran una llamada. Cada uno lleva un {@code reason} estable,
 * que viaja en el {@code google.rpc.ErrorInfo} del estado gRPC.
 */
public abstract class BusinessException extends RuntimeException {

    private final String reason;

    protected BusinessException(String reason, String message) {
        super(message);
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }
}
