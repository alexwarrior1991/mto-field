package com.alejandro.mtofield.application.service;

import java.util.List;
import java.util.UUID;

/**
 * Lo que el tablero necesita saber de los streams abiertos en esta replica: que equipos tienen
 * uno y hasta que secuencia se les ha escrito. Lo implementa el registro de streams.
 */
public interface DeviceStreamPresence {

    /** @param lastSentSequence la mayor secuencia ya escrita en el transporte de ese stream */
    record StreamPresence(String deviceId, UUID shiftId, String teamCode, long lastSentSequence) {
    }

    List<StreamPresence> streamsOf(UUID possessionId);
}
