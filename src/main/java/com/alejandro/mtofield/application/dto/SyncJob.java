package com.alejandro.mtofield.application.dto;

import java.util.UUID;

/** Un evento de tarea persistido que hay que contarle a mto-maintenance, fuera del hilo del stream. */
public record SyncJob(UUID eventId, EventContext context) {
}
