package com.alejandro.mtofield.application.dto;

import java.util.UUID;

/**
 * Un evento de tarea persistido que hay que contarle a mto-maintenance, fuera del hilo del stream.
 *
 * @param retry {@code false} en el primer intento (desde la cola de trabajo del dispositivo), que
 *              contesta PENDING_SYNC si mto-maintenance no responde; {@code true} en los reintentos
 *              programados, que solo contestan cuando se resuelve
 */
public record SyncJob(UUID eventId, EventContext context, boolean retry) {

    public static SyncJob first(UUID eventId, EventContext context) {
        return new SyncJob(eventId, context, false);
    }
}
