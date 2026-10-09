package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.CommandDraft;
import com.alejandro.mtofield.application.dto.SyncJob;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.application.service.FieldEventSynchronizer;
import com.alejandro.mtofield.configuration.AuditActorResolver;
import com.alejandro.mtofield.grpc.v1.EventResult;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * El sincronizador de la fase 1: sin mto-maintenance delante, un evento de tarea se queda PENDING
 * y el dispositivo recibe {@code PENDING_SYNC}, que es la verdad. La fase 2 lo sustituye por el que
 * cuenta el inicio y el fin de la tarea a mto-maintenance y contesta APPLIED o REJECTED.
 */
@Service
@RequiredArgsConstructor
class PendingSyncEventSynchronizer implements FieldEventSynchronizer {

    private final FieldCommandService commands;

    @Override
    public void process(SyncJob job) {
        commands.issue(job.context().possessionId(),
                CommandDraft.eventResult(job.context().shiftId(), job.context().sequence(), EventResult.Outcome.PENDING_SYNC, null),
                AuditActorResolver.SYSTEM_ACTOR);
    }
}
