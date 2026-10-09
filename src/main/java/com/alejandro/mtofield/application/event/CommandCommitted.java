package com.alejandro.mtofield.application.event;

import com.alejandro.mtofield.grpc.v1.FieldCommand;

import java.util.UUID;

/**
 * Una orden ya confirmada en la base, lista para enviarse. Se publica dentro de la transaccion y el
 * despachador la recibe AFTER_COMMIT: nunca se envia nada que no este escrito.
 *
 * @param targetShiftId {@code null} es difusion
 */
public record CommandCommitted(UUID possessionId, UUID targetShiftId, FieldCommand command) {
}
