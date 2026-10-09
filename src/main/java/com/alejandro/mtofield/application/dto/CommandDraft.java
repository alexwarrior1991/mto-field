package com.alejandro.mtofield.application.dto;

import com.alejandro.mtofield.grpc.v1.EventResult;
import com.alejandro.mtofield.grpc.v1.EvacuateNow;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.grpc.v1.SupervisorMessage;
import com.alejandro.mtofield.grpc.v1.WindowChanged;
import com.alejandro.mtofield.application.mapper.ProtoTimestamps;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldCommandKind;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.UUID;
import java.util.function.UnaryOperator;

/**
 * Una orden antes de tomar su numero: que lleva, a quien va, si pide acuse y, si viene de
 * IssueCommand, su clave de idempotencia. El id, la secuencia y la hora los pone
 * {@code FieldCommandService.issue} dentro de la transaccion.
 *
 * @param targetShiftId {@code null} es difusion a todos los turnos de la posesion
 */
public record CommandDraft(
        FieldCommandKind kind,
        UUID targetShiftId,
        boolean requiresAck,
        String idempotencyKey,
        UnaryOperator<FieldCommand.Builder> body
) {

    public FieldCommand build(UUID commandId, long sequence, Instant issuedAt) {
        FieldCommand.Builder builder = FieldCommand.newBuilder()
                .setCommandId(commandId.toString())
                .setSequence(sequence)
                .setIssuedAt(timestamp(issuedAt))
                .setRequiresAck(requiresAck);
        return body.apply(builder).build();
    }

    public static CommandDraft evacuateNow(String idempotencyKey, EvacuateNow evacuateNow) {
        return new CommandDraft(FieldCommandKind.EVACUATE_NOW, null, true, idempotencyKey, builder -> builder.setEvacuateNow(evacuateNow));
    }

    public static CommandDraft supervisorMessage(String idempotencyKey, SupervisorMessage message) {
        return new CommandDraft(FieldCommandKind.SUPERVISOR_MESSAGE, null, false, idempotencyKey, builder -> builder.setSupervisorMessage(message));
    }

    public static CommandDraft windowChanged(String idempotencyKey, WindowChanged windowChanged) {
        return new CommandDraft(FieldCommandKind.WINDOW_CHANGED, null, false, idempotencyKey, builder -> builder.setWindowChanged(windowChanged));
    }

    /** El resultado de un evento subido, dirigido al turno del dispositivo que lo subio. */
    public static CommandDraft eventResult(UUID shiftId, long sequence, EventResult.Outcome outcome, String reason) {
        EventResult.Builder result = EventResult.newBuilder().setSequence(sequence).setOutcome(outcome);
        if (reason != null) {
            result.setReason(reason);
        }
        EventResult built = result.build();
        return new CommandDraft(FieldCommandKind.EVENT_RESULT, shiftId, false, null, builder -> builder.setEventResult(built));
    }

    public static Timestamp timestamp(Instant instant) {
        return ProtoTimestamps.toProto(instant);
    }
}
