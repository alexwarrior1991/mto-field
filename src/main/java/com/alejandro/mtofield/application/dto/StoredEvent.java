package com.alejandro.mtofield.application.dto;

import com.alejandro.mtofield.infrastructure.persistence.entity.FieldEventSyncStatus;

import java.util.UUID;

/**
 * El resultado de guardar un evento: si se inserto ahora o ya estaba (un reenvio), y su estado de
 * sincronizacion. Un evento rechazado antes de guardarse (secuencia 0) no tiene id.
 */
public record StoredEvent(UUID id, boolean inserted, FieldEventSyncStatus status) {

    public static StoredEvent rejected() {
        return new StoredEvent(null, false, null);
    }
}
