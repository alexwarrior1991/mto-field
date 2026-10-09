package com.alejandro.mtofield.infrastructure.persistence.entity;

/** Ciclo de vida de una posesion: se abre agrupando turnos y se cierra una vez, con o sin todos fuera de via. */
public enum PossessionStatus {
    OPEN,
    CLOSED
}
