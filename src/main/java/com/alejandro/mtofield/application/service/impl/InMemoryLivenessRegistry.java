package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.service.LivenessRegistry;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Estado por JVM: lo ultimo que se supo de cada dispositivo. Los records son inmutables y se
 * sustituyen enteros con {@code compute}, asi que dos callbacks del mismo dispositivo (el latido y
 * el cierre del stream) no se pisan a medias.
 */
@Service
class InMemoryLivenessRegistry implements LivenessRegistry {

    private final Clock clock;
    private final Map<String, DeviceLiveness> devices = new ConcurrentHashMap<>();
    private final Map<UUID, Set<String>> devicesByPossession = new ConcurrentHashMap<>();

    InMemoryLivenessRegistry(Clock clock) {
        this.clock = clock;
    }

    @Override
    public void streamOpened(String deviceId, UUID shiftId, UUID possessionId) {
        devicesByPossession.computeIfAbsent(possessionId, ignored -> ConcurrentHashMap.newKeySet()).add(deviceId);
        devices.compute(deviceId, (id, previous) -> {
            DeviceLiveness base = previous != null && possessionId.equals(previous.possessionId()) ? previous : null;
            return new DeviceLiveness(deviceId, shiftId, possessionId, clock.instant(),
                    base == null ? "" : base.kp(), base == null ? 0 : base.batteryPct(), base == null ? 0 : base.signalDbm(), true);
        });
    }

    @Override
    public void touch(String deviceId) {
        devices.computeIfPresent(deviceId, (id, previous) -> new DeviceLiveness(previous.deviceId(), previous.shiftId(),
                previous.possessionId(), clock.instant(), previous.kp(), previous.batteryPct(), previous.signalDbm(), previous.streamOpen()));
    }

    @Override
    public void heartbeat(String deviceId, String kp, int batteryPct, int signalDbm) {
        devices.computeIfPresent(deviceId, (id, previous) -> new DeviceLiveness(previous.deviceId(), previous.shiftId(),
                previous.possessionId(), clock.instant(), kp == null ? "" : kp, batteryPct, signalDbm, previous.streamOpen()));
    }

    @Override
    public void streamClosed(String deviceId) {
        devices.computeIfPresent(deviceId, (id, previous) -> new DeviceLiveness(previous.deviceId(), previous.shiftId(),
                previous.possessionId(), previous.lastSeen(), previous.kp(), previous.batteryPct(), previous.signalDbm(), false));
    }

    @Override
    public Optional<DeviceLiveness> ofDevice(String deviceId) {
        return Optional.ofNullable(devices.get(deviceId));
    }

    @Override
    public List<DeviceLiveness> ofPossession(UUID possessionId) {
        Set<String> ids = devicesByPossession.get(possessionId);
        if (ids == null) {
            return List.of();
        }
        return ids.stream()
                .map(devices::get)
                .filter(liveness -> liveness != null && possessionId.equals(liveness.possessionId()))
                .toList();
    }

    @Override
    public void forget(UUID possessionId) {
        Set<String> ids = devicesByPossession.remove(possessionId);
        if (ids == null) {
            return;
        }
        for (String deviceId : ids) {
            devices.computeIfPresent(deviceId, (id, previous) -> possessionId.equals(previous.possessionId()) ? null : previous);
        }
    }
}
