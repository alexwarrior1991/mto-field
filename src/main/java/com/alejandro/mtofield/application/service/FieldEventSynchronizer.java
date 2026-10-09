package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.application.dto.SyncJob;

/**
 * Lo que se hace con un evento de tarea ya persistido, fuera del hilo del stream: contarselo a
 * mto-maintenance ({@code MaintenanceEventSynchronizer}) o, con el cliente apagado, contestar que
 * queda pendiente ({@code PendingSyncEventSynchronizer}). Nunca lanza: el resultado dice como quedo.
 */
public interface FieldEventSynchronizer {

    enum Outcome {
        /** mto-maintenance lo tiene (o ya lo tenia): el evento queda SYNCED y el dispositivo recibe APPLIED. */
        SYNCED,
        /** mto-maintenance dijo que no: REJECTED, con el motivo, y no se reintenta. */
        REJECTED,
        /** mto-maintenance no contesto: FAILED hasta el siguiente reintento. */
        FAILED,
        /** Sin cliente: queda PENDING y el dispositivo recibe PENDING_SYNC. */
        PENDING
    }

    Outcome process(SyncJob job);
}
