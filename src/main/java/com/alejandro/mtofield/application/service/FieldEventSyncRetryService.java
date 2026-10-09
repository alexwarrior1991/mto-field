package com.alejandro.mtofield.application.service;

/**
 * El reintento de los eventos de tarea que mto-maintenance no contesto ({@code FAILED}) o que una
 * cola de trabajo dejo a medias ({@code PENDING} con su siguiente intento vencido). Lo programa
 * {@code FieldEventSyncRetryConfiguration} cada {@code app.maintenance.sync-retry.interval}.
 */
public interface FieldEventSyncRetryService {

    /** @return cuantos eventos quedaron resueltos (SYNCED o REJECTED) en esta pasada */
    int retryDue();
}
