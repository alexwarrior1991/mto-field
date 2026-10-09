package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.application.dto.SyncJob;

/**
 * Lo que se hace con un evento de tarea ya persistido, fuera del hilo del stream: en la fase 1,
 * contestar que queda pendiente de sincronizar; en la fase 2, contarselo a mto-maintenance.
 */
public interface FieldEventSynchronizer {

    void process(SyncJob job);
}
