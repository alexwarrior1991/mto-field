package com.alejandro.mtofield.infrastructure.persistence.entity;

/** Que subio un dispositivo. El latido no se guarda, asi que no esta aqui. */
public enum FieldEventKind {
    TASK_STARTED,
    TASK_COMPLETED,
    COMMAND_ACK,
    CLEAR_OF_TRACK
}
