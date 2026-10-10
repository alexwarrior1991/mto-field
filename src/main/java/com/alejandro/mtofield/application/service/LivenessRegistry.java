package com.alejandro.mtofield.application.service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Lo que se sabe en memoria de cada dispositivo conectado: cuando hablo por ultima vez y lo que dijo
 * su ultimo latido. Es estado de esta replica (la fase 4 lo comparte); el tablero lo lee para
 * decidir si un equipo esta CONNECTED, STALE o DISCONNECTED.
 */
public interface LivenessRegistry {

    record DeviceLiveness(
            String deviceId,
            UUID shiftId,
            UUID possessionId,
            Instant lastSeen,
            String kp,
            int batteryPct,
            int signalDbm,
            boolean streamOpen
    ) {
    }

    void streamOpened(String deviceId, UUID shiftId, UUID possessionId);

    /** Cualquier mensaje del dispositivo cuenta como senal de vida. */
    void touch(String deviceId);

    void heartbeat(String deviceId, String kp, int batteryPct, int signalDbm);

    /** El stream se cerro; lo ultimo que dijo se conserva hasta que la posesion se olvide. */
    void streamClosed(String deviceId);

    Optional<DeviceLiveness> ofDevice(String deviceId);

    List<DeviceLiveness> ofPossession(UUID possessionId);

    void forget(UUID possessionId);
}
