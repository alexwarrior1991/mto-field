package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.application.replicas.ReplicaMessage;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Lo ultimo que cada otra replica conto de cada dispositivo. El tablero lo funde con lo local (lo
 * local gana para un dispositivo con stream abierto aqui); caduca si nadie lo vuelve a contar y
 * se olvida con la posesion o con la replica que lo contaba.
 */
public interface RemoteDeviceStates {

    record RemoteDevice(String replicaId, ReplicaMessage.Device state, Instant reportedAt) {
    }

    /**
     * Guarda lo contado, salvo que sea de un stream mas viejo que el que ya se conoce: la replica
     * que cerro el stream antiguo no debe borrar lo que cuenta la que abrio el nuevo.
     */
    void upsert(String replicaId, ReplicaMessage.Device state, Instant reportedAt);

    Optional<RemoteDevice> ofDevice(String deviceId);

    List<RemoteDevice> ofPossession(UUID possessionId);

    void forget(UUID possessionId);

    /** La replica se fue: fuera lo que contaba. Devuelve las posesiones afectadas. */
    Set<UUID> forgetReplica(String replicaId);

    /** Lo que nadie volvio a contar desde {@code before} caduca. Devuelve las posesiones afectadas. */
    Set<UUID> expire(Instant before);
}
