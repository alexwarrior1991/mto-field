package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.replicas.ReplicaMessage;
import com.alejandro.mtofield.application.service.RemoteDeviceStates;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Estado por JVM, un record inmutable por dispositivo sustituido entero con {@code compute}. */
@Service
public class InMemoryRemoteDeviceStates implements RemoteDeviceStates {

    private final Map<String, RemoteDevice> devices = new ConcurrentHashMap<>();
    /** Las replicas que se despidieron: lo que cuenten despues de un stream cerrado llega tarde y no vale. */
    private final Set<String> stopped = ConcurrentHashMap.newKeySet();

    @Override
    public void upsert(String replicaId, ReplicaMessage.Device state, Instant reportedAt) {
        if (stopped.contains(replicaId)) {
            if (!state.streamOpen()) {
                return;
            }
            // Un stream abierto despues de la despedida es una replica que volvio con el mismo nombre.
            stopped.remove(replicaId);
        }
        devices.compute(state.deviceId(), (id, previous) -> previous == null || accepts(previous, replicaId, state)
                ? new RemoteDevice(replicaId, state, reportedAt) : previous);
    }

    private static boolean accepts(RemoteDevice previous, String replicaId, ReplicaMessage.Device state) {
        if (replicaId.equals(previous.replicaId())) {
            return true;
        }
        Instant known = previous.state().openedAt();
        return state.openedAt() == null || known == null || !state.openedAt().isBefore(known);
    }

    @Override
    public Optional<RemoteDevice> ofDevice(String deviceId) {
        return Optional.ofNullable(devices.get(deviceId));
    }

    @Override
    public List<RemoteDevice> ofPossession(UUID possessionId) {
        return devices.values().stream().filter(device -> possessionId.equals(device.state().possessionId())).toList();
    }

    @Override
    public void forget(UUID possessionId) {
        devices.values().removeIf(device -> possessionId.equals(device.state().possessionId()));
    }

    @Override
    public Set<UUID> forgetReplica(String replicaId) {
        stopped.add(replicaId);
        return removeIf(device -> replicaId.equals(device.replicaId()));
    }

    @Override
    public Set<UUID> expire(Instant before) {
        return removeIf(device -> device.reportedAt().isBefore(before));
    }

    private Set<UUID> removeIf(java.util.function.Predicate<RemoteDevice> condition) {
        Set<UUID> affected = new HashSet<>();
        devices.entrySet().removeIf(entry -> {
            if (condition.test(entry.getValue())) {
                affected.add(entry.getValue().state().possessionId());
                return true;
            }
            return false;
        });
        return affected;
    }
}
