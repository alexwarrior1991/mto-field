package com.alejandro.mtofield.infrastructure.persistence.entity;

/** Que lleva una orden descendente. Los tres primeros los emite el responsable; EVENT_RESULT lo emite el servicio. */
public enum FieldCommandKind {
    WINDOW_CHANGED,
    EVACUATE_NOW,
    SUPERVISOR_MESSAGE,
    EVENT_RESULT
}
