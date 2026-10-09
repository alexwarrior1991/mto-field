package com.alejandro.mtofield.application.exception;

/**
 * mto-maintenance ha dicho que no: un 4xx de negocio con su codigo estable ({@code TRN-001} la
 * tarea o la orden no admiten la transicion, {@code SHF-001} el turno no esta trabajando en esa
 * via, {@code MO-404} la orden no existe, {@code VAL-001} el cuerpo...). No es un fallo de
 * disponibilidad: mto-maintenance esta contestando, y no cuenta para el circuito.
 */
public class MaintenanceRejectedException extends BusinessException {

    public static final String REASON = "MAINTENANCE_REJECTED";

    /** El codigo con el que mto-maintenance dice que una tarea o su orden no estan en el estado que la peticion exige. */
    public static final String INVALID_TRANSITION = "TRN-001";

    private final int status;
    private final String errorCode;
    private final String detail;

    public MaintenanceRejectedException(String operation, int status, String errorCode, String detail) {
        super(REASON, "mto-maintenance rejected '" + operation + "' with " + status + (errorCode == null ? "" : " " + errorCode)
                + (detail == null || detail.isBlank() ? "" : ": " + detail));
        this.status = status;
        this.errorCode = errorCode;
        this.detail = detail;
    }

    public int getStatus() {
        return status;
    }

    /** El {@code errorCode} de la respuesta de mto-maintenance, o {@code null} si no se pudo leer. */
    public String getErrorCode() {
        return errorCode;
    }

    /** El mensaje de mto-maintenance, o el cuerpo tal cual si no era su JSON de error. */
    public String getDetail() {
        return detail;
    }

    /** Lo que se le cuenta al dispositivo y queda en {@code last_error}: el codigo y el motivo, sin el envoltorio. */
    public String reason() {
        String code = errorCode == null ? "HTTP " + status : errorCode;
        return detail == null || detail.isBlank() ? code : code + ": " + detail;
    }

    /** Un 409 {@code TRN-001}: la tarea no esta en el estado que la peticion exige. Puede ser una respuesta perdida. */
    public boolean isTransitionConflict() {
        return status == 409 && INVALID_TRANSITION.equals(errorCode);
    }
}
