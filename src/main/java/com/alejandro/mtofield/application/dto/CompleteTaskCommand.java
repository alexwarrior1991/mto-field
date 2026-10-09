package com.alejandro.mtofield.application.dto;

import java.util.List;
import java.util.UUID;

/**
 * Lo que un {@code TaskCompleted} cuenta a mto-maintenance: el espejo de su {@code CompleteTaskRequest}
 * con las claves que el contrato de campo lleva. El material no viaja: el contrato no lo trae.
 */
public record CompleteTaskCommand(
        UUID shiftId,
        List<String> taskTypeCodes,
        String notes,
        boolean workComplete,
        List<InlineDefect> defects,
        List<String> photoRefs
) {

    public CompleteTaskCommand {
        taskTypeCodes = taskTypeCodes == null ? List.of() : List.copyOf(taskTypeCodes);
        defects = defects == null ? List.of() : List.copyOf(defects);
        photoRefs = photoRefs == null ? List.of() : List.copyOf(photoRefs);
    }

    /** @param severity {@code LOW}, {@code MEDIUM}, {@code HIGH} o {@code CRITICAL}, como lo nombra mto-maintenance */
    public record InlineDefect(String severity, String description, String technicalNotes, String correctionType, String partsReplaced) {
    }
}
