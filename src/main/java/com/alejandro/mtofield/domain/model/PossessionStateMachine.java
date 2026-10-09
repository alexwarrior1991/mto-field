package com.alejandro.mtofield.domain.model;

import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionStatus;

/**
 * El ciclo de una posesion: {@code OPEN -> CLOSED}, una sola vez. Se cierra cuando todos los
 * equipos han salido de la via, o de forma forzada por el responsable, que queda registrado.
 */
public final class PossessionStateMachine {

    private PossessionStateMachine() {
    }

    public static boolean canClose(PossessionStatus status) {
        return status == PossessionStatus.OPEN;
    }

    /** Sin {@code force}, cerrar exige que todos los turnos hayan mandado su salida de via. */
    public static boolean closeAllowed(boolean allClear, boolean force) {
        return allClear || force;
    }

    /** Un dispositivo solo puede unirse, y una orden solo puede emitirse, mientras la posesion esta abierta. */
    public static boolean acceptsTraffic(PossessionStatus status) {
        return status == PossessionStatus.OPEN;
    }
}
