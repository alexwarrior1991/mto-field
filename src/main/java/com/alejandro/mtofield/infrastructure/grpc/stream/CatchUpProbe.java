package com.alejandro.mtofield.infrastructure.grpc.stream;

/**
 * Costura de test para la carrera entre la reproduccion del atraso y las ordenes en vivo: en
 * produccion no hace nada; un test pone un bean {@code @Primary} que para el hilo de catch-up en
 * estos dos puntos y emite ordenes mientras tanto.
 */
public interface CatchUpProbe {

    CatchUpProbe NONE = new CatchUpProbe() {
    };

    /** El stream ya esta registrado y aun no ha leido nada de la base. */
    default void afterRegistered(DeviceStream stream) {
    }

    /** La reproduccion ha terminado y las ordenes retenidas aun no se han volcado. */
    default void beforeFlush(DeviceStream stream) {
    }
}
