package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.CommandDraft;
import com.alejandro.mtofield.application.dto.SyncJob;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.application.service.FieldEventSynchronizer;
import com.alejandro.mtofield.configuration.AuditActorResolver;
import com.alejandro.mtofield.grpc.v1.EventResult;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

/**
 * El sincronizador con el cliente de mantenimiento apagado ({@code app.maintenance.enabled=false}):
 * sin mto-maintenance delante, un evento de tarea se queda PENDING y el dispositivo recibe
 * {@code PENDING_SYNC}, que es la verdad. Con el cliente encendido lo sustituye
 * {@link MaintenanceEventSynchronizer}.
 */
@Service
@ConditionalOnProperty(prefix = "app.maintenance", name = "enabled", havingValue = "false", matchIfMissing = false)
@RequiredArgsConstructor
class PendingSyncEventSynchronizer implements FieldEventSynchronizer {

    private final FieldCommandService commands;

    @Override
    public Outcome process(SyncJob job) {
        if (!job.retry()) {
            commands.issue(job.context().possessionId(),
                    CommandDraft.eventResult(job.context().shiftId(), job.context().sequence(), EventResult.Outcome.PENDING_SYNC, null),
                    AuditActorResolver.SYSTEM_ACTOR);
        }
        return Outcome.PENDING;
    }
}
