package com.alejandro.mtofield.infrastructure.grpc.advice;

import com.alejandro.mtofield.application.exception.BusinessException;
import com.alejandro.mtofield.application.exception.InvalidPossessionRequestException;
import com.alejandro.mtofield.application.exception.PossessionNotAllClearException;
import com.alejandro.mtofield.application.exception.PossessionNotFoundException;
import com.alejandro.mtofield.application.exception.PossessionNotOpenException;
import com.alejandro.mtofield.application.exception.ShiftAlreadyInOpenPossessionException;
import com.alejandro.mtofield.application.exception.ShiftNotInOpenPossessionException;
import com.alejandro.mtofield.domain.model.ShiftNotWorkableException;
import com.alejandro.mtofield.infrastructure.grpc.GrpcErrors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.validation.ConstraintViolationException;
import org.springframework.grpc.server.advice.GrpcAdvice;
import org.springframework.grpc.server.advice.GrpcExceptionHandler;

import java.util.Map;

/**
 * Lo que cierra una llamada, traducido a un estado gRPC con {@code google.rpc.ErrorInfo}.
 *
 * <p>Solo para las RPC unarias y para abrir un stream. Dentro de un {@code TeamChannel}, un error
 * de negocio de un mensaje se contesta en banda con {@code EventResult{REJECTED}}: cerrar el stream
 * por un acuse mal formado dejaria al equipo sin canal.</p>
 */
@GrpcAdvice
public class FieldGrpcExceptionAdvice {

    public static final String PENDING_TEAMS = "pending_teams";

    @GrpcExceptionHandler(PossessionNotFoundException.class)
    public StatusRuntimeException notFound(PossessionNotFoundException exception) {
        return GrpcErrors.of(Status.Code.NOT_FOUND, exception.getReason(), exception.getMessage());
    }

    @GrpcExceptionHandler(PossessionNotAllClearException.class)
    public StatusRuntimeException notAllClear(PossessionNotAllClearException exception) {
        return GrpcErrors.of(Status.Code.FAILED_PRECONDITION, exception.getReason(), exception.getMessage(),
                Map.of(PENDING_TEAMS, String.join(",", exception.getPendingTeams())));
    }

    @GrpcExceptionHandler({PossessionNotOpenException.class, ShiftAlreadyInOpenPossessionException.class, ShiftNotInOpenPossessionException.class})
    public StatusRuntimeException failedPrecondition(BusinessException exception) {
        return GrpcErrors.of(Status.Code.FAILED_PRECONDITION, exception.getReason(), exception.getMessage());
    }

    @GrpcExceptionHandler(ShiftNotWorkableException.class)
    public StatusRuntimeException shiftNotWorkable(ShiftNotWorkableException exception) {
        return GrpcErrors.of(Status.Code.FAILED_PRECONDITION, "SHIFT_NOT_WORKABLE", exception.getMessage(),
                Map.of("shift_id", exception.getShiftId().toString(), "status", exception.getStatus()));
    }

    @GrpcExceptionHandler(InvalidPossessionRequestException.class)
    public StatusRuntimeException invalidRequest(InvalidPossessionRequestException exception) {
        return GrpcErrors.of(Status.Code.INVALID_ARGUMENT, exception.getReason(), exception.getMessage());
    }

    @GrpcExceptionHandler(IllegalArgumentException.class)
    public StatusRuntimeException illegalArgument(IllegalArgumentException exception) {
        return GrpcErrors.of(Status.Code.INVALID_ARGUMENT, "INVALID_ARGUMENT", exception.getMessage());
    }

    @GrpcExceptionHandler(ConstraintViolationException.class)
    public StatusRuntimeException constraintViolation(ConstraintViolationException exception) {
        return GrpcErrors.of(Status.Code.INVALID_ARGUMENT, "INVALID_ARGUMENT", exception.getMessage());
    }

    /** Cualquier otro error de negocio que no tenga su codigo arriba: la llamada no procede, con su motivo. */
    @GrpcExceptionHandler(BusinessException.class)
    public StatusRuntimeException business(BusinessException exception) {
        return GrpcErrors.of(Status.Code.FAILED_PRECONDITION, exception.getReason(), exception.getMessage());
    }
}
