package com.alejandro.mtofield.infrastructure.persistence.entity;

/**
 * Si un evento hay que contarselo a mto-maintenance y como va. NOT_REQUIRED es lo que se aplica
 * aqui (acuses, salidas de via); el resto es el ciclo de una tarea: PENDING hasta que se intente,
 * SYNCED si mto-maintenance lo aplico, FAILED si no respondio (se reintenta), REJECTED si dijo que no.
 */
public enum FieldEventSyncStatus {
    NOT_REQUIRED,
    PENDING,
    SYNCED,
    FAILED,
    REJECTED
}
