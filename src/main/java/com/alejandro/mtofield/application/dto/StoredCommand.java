package com.alejandro.mtofield.application.dto;

import com.alejandro.mtofield.grpc.v1.FieldCommand;

import java.util.UUID;

/** Una orden leida de la base: el mensaje tal como se envio y a quien iba. */
public record StoredCommand(UUID id, long sequence, UUID targetShiftId, boolean requiresAck, FieldCommand command) {

    public boolean addressedTo(UUID shiftId) {
        return targetShiftId == null || targetShiftId.equals(shiftId);
    }
}
