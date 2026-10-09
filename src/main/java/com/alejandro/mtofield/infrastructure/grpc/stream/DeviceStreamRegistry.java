package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.application.service.DeviceStreamPresence;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.configuration.metrics.FieldMetrics;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Los streams abiertos de esta replica, por dispositivo y por posesion.
 *
 * <p>Por posesion hay un <b>carril</b>: el conjunto de sus streams y, con el, la ultima secuencia
 * que el despachador abanico. El carril se crea aqui, al registrar el primer stream de la
 * posesion, con el maximo de la base: creado perezosamente en el despacho podria leer un maximo
 * que incluye ordenes cuyo evento aun no ha llegado y saltarselas para los streams vivos. Y un
 * stream se registra <b>antes</b> de leer su atraso: lo que se confirme entre medias le llega por
 * el carril y se retiene hasta que termine la reproduccion.</p>
 *
 * <p>Quitar es por instancia ({@code remove(key, value)}): el stream sustituido, al cerrarse, no
 * borra al que lo sustituyo.</p>
 */
@Component
public class DeviceStreamRegistry implements DeviceStreamPresence {

    /** Lo que el despachador necesita de una posesion: sus streams y hasta donde ha abanicado. */
    public static final class PossessionLane {

        private final ReentrantLock lock = new ReentrantLock();
        private final Set<DeviceStream> streams = ConcurrentHashMap.newKeySet();
        private long lastDispatched;

        PossessionLane(long lastDispatched) {
            this.lastDispatched = lastDispatched;
        }

        public ReentrantLock lock() {
            return lock;
        }

        /** Solo bajo {@link #lock()}. */
        public long lastDispatched() {
            return lastDispatched;
        }

        /** Solo bajo {@link #lock()}. */
        public void lastDispatched(long sequence) {
            this.lastDispatched = sequence;
        }

        public Set<DeviceStream> streams() {
            return streams;
        }
    }

    private final FieldCommandService commands;
    private final Map<String, DeviceStream> byDevice = new ConcurrentHashMap<>();
    private final Map<UUID, PossessionLane> lanes = new ConcurrentHashMap<>();

    public DeviceStreamRegistry(FieldCommandService commands, FieldMetrics metrics) {
        this.commands = commands;
        metrics.gauge(FieldMetrics.STREAMS_OPEN, "Device streams open on this replica", byDevice::size);
        metrics.gauge(FieldMetrics.OUTBOUND_DEPTH, "Commands waiting in the outbound queues of every device stream",
                () -> byDevice.values().stream().mapToInt(DeviceStream::outboundDepth).sum());
    }

    /**
     * Registra el stream y devuelve el que sustituye (otro stream del mismo dispositivo), o
     * {@code null}. Quien llama le hace {@code supersede()} al devuelto.
     */
    public DeviceStream register(DeviceStream stream) {
        PossessionLane lane = lanes.get(stream.possessionId());
        if (lane == null) {
            PossessionLane candidate = new PossessionLane(commands.maxSequence(stream.possessionId()));
            PossessionLane existing = lanes.putIfAbsent(stream.possessionId(), candidate);
            lane = existing == null ? candidate : existing;
        }
        lane.streams().add(stream);
        DeviceStream previous = byDevice.put(stream.deviceId(), stream);
        return previous == stream ? null : previous;
    }

    /** Quita ese stream, y solo ese: si el dispositivo ya tiene otro, el otro se queda. */
    public void unregister(DeviceStream stream) {
        byDevice.remove(stream.deviceId(), stream);
        PossessionLane lane = lanes.get(stream.possessionId());
        if (lane != null) {
            lane.streams().remove(stream);
        }
    }

    /** El carril de la posesion, o {@code null} si ningun dispositivo suyo se ha conectado a esta replica. */
    public PossessionLane lane(UUID possessionId) {
        return lanes.get(possessionId);
    }

    public DeviceStream ofDevice(String deviceId) {
        return byDevice.get(deviceId);
    }

    public List<DeviceStream> ofPossession(UUID possessionId) {
        PossessionLane lane = lanes.get(possessionId);
        return lane == null ? List.of() : List.copyOf(lane.streams());
    }

    @Override
    public List<StreamPresence> streamsOf(UUID possessionId) {
        return ofPossession(possessionId).stream()
                .map(stream -> new StreamPresence(stream.deviceId(), stream.shiftId(), stream.teamCode(), stream.lastSentSequence()))
                .toList();
    }

    public int openStreams() {
        return byDevice.size();
    }

    /** La posesion se ha cerrado: fin normal de todos sus streams y fuera el carril. */
    public void closeAll(UUID possessionId) {
        PossessionLane lane = lanes.remove(possessionId);
        if (lane == null) {
            return;
        }
        for (DeviceStream stream : List.copyOf(lane.streams())) {
            stream.complete();
        }
    }
}
