package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.BoardSnapshot;
import com.alejandro.mtofield.application.dto.BoardSnapshot.CommandState;
import com.alejandro.mtofield.application.dto.BoardSnapshot.TeamState;
import com.alejandro.mtofield.application.exception.PossessionNotFoundException;
import com.alejandro.mtofield.application.service.BoardPublisher;
import com.alejandro.mtofield.application.service.DeviceStreamPresence;
import com.alejandro.mtofield.application.service.DeviceStreamPresence.StreamPresence;
import com.alejandro.mtofield.application.service.LivenessRegistry;
import com.alejandro.mtofield.application.service.LivenessRegistry.DeviceLiveness;
import com.alejandro.mtofield.application.service.PossessionBoardService;
import com.alejandro.mtofield.application.service.RemoteDeviceStates;
import com.alejandro.mtofield.application.service.RemoteDeviceStates.RemoteDevice;
import com.alejandro.mtofield.application.replicas.ReplicaMessage;
import com.alejandro.mtofield.configuration.grpc.FieldProperties;
import com.alejandro.mtofield.configuration.metrics.FieldMetrics;
import com.alejandro.mtofield.domain.model.CommandAckSummary;
import com.alejandro.mtofield.domain.model.TeamLiveness;
import com.alejandro.mtofield.domain.model.TeamLiveness.Liveness;
import com.alejandro.mtofield.infrastructure.persistence.entity.CommandAckRecord;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldCommandRecord;
import com.alejandro.mtofield.infrastructure.persistence.entity.Possession;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionShift;
import com.alejandro.mtofield.infrastructure.persistence.repository.CommandAckRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.FieldCommandRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.PossessionRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.PossessionShiftRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * El recalculo coalescido del tablero. Por posesion hay un bucle: {@code markDirty} marca y, si
 * no hay bucle en marcha, lanza uno en el ejecutor; el bucle recalcula y publica mientras siga
 * sucio. La marca se pone antes de intentar arrancar el bucle y el bucle la borra antes de
 * calcular, asi que un cambio que llega a mitad de un calculo produce exactamente otro calculo.
 */
@Service
class PossessionBoardServiceImpl implements PossessionBoardService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PossessionBoardServiceImpl.class);

    private final PossessionRepository possessions;
    private final PossessionShiftRepository shifts;
    private final FieldCommandRepository commands;
    private final CommandAckRepository acks;
    private final DeviceStreamPresence presence;
    private final LivenessRegistry liveness;
    private final RemoteDeviceStates remote;
    private final BoardPublisher publisher;
    private final TransactionTemplate readOnly;
    private final Executor executor;
    private final Clock clock;
    private final TeamLiveness.Thresholds thresholds;
    private final Map<UUID, Loop> loops = new ConcurrentHashMap<>();
    private final Map<UUID, BoardSnapshot> lastBoards = new ConcurrentHashMap<>();

    PossessionBoardServiceImpl(PossessionRepository possessions, PossessionShiftRepository shifts, FieldCommandRepository commands,
                               CommandAckRepository acks, DeviceStreamPresence presence, LivenessRegistry liveness, RemoteDeviceStates remote,
                               BoardPublisher publisher,
                               PlatformTransactionManager transactionManager, @Qualifier("fieldStreamExecutor") Executor executor, Clock clock,
                               FieldProperties properties, FieldMetrics metrics) {
        this.possessions = possessions;
        this.shifts = shifts;
        this.commands = commands;
        this.acks = acks;
        this.presence = presence;
        this.liveness = liveness;
        this.remote = remote;
        this.publisher = publisher;
        this.readOnly = new TransactionTemplate(transactionManager);
        this.readOnly.setReadOnly(true);
        this.executor = executor;
        this.clock = clock;
        this.thresholds = new TeamLiveness.Thresholds(properties.liveness().staleAfter(), properties.liveness().disconnectedAfter());
        metrics.gauge(FieldMetrics.TEAMS_CONNECTED, "Teams with a device CONNECTED, over the possessions this replica keeps a board of",
                () -> lastBoards.values().stream().mapToLong(BoardSnapshot::connectedTeams).sum());
        metrics.gauge(FieldMetrics.COMMANDS_PENDING_ACK, "Commands requiring acknowledgement with at least one team still pending",
                () -> lastBoards.values().stream().mapToLong(BoardSnapshot::commandsPendingAck).sum());
    }

    @Override
    public void markDirty(UUID possessionId) {
        Loop loop = loops.computeIfAbsent(possessionId, id -> new Loop(clock.millis()));
        loop.dirty.set(true);
        if (loop.running.compareAndSet(false, true)) {
            try {
                executor.execute(() -> run(possessionId, loop));
            } catch (RejectedExecutionException shuttingDown) {
                loop.running.set(false);
            }
        }
    }

    private void run(UUID possessionId, Loop loop) {
        do {
            loop.dirty.set(false);
            try {
                BoardSnapshot board = compute(possessionId, loop.version.incrementAndGet());
                lastBoards.put(possessionId, board);
                publisher.publish(board);
            } catch (PossessionNotFoundException gone) {
                loops.remove(possessionId, loop);
                lastBoards.remove(possessionId);
                return;
            } catch (RuntimeException failure) {
                LOGGER.warn("Board of possession {} could not be recomputed: {}", possessionId, failure.toString());
            }
            loop.running.set(false);
        } while (loop.dirty.get() && loop.running.compareAndSet(false, true));
    }

    @Override
    public BoardSnapshot snapshot(UUID possessionId) {
        Loop loop = loops.computeIfAbsent(possessionId, id -> new Loop(clock.millis()));
        BoardSnapshot board = compute(possessionId, loop.version.incrementAndGet());
        lastBoards.put(possessionId, board);
        return board;
    }

    @Override
    public void tick() {
        for (UUID possessionId : publisher.watchedPossessions()) {
            markDirty(possessionId);
        }
    }

    @Override
    public void possessionClosed(UUID possessionId) {
        Loop loop = loops.remove(possessionId);
        long version = loop == null ? clock.millis() : loop.version.incrementAndGet();
        BoardSnapshot last;
        try {
            last = compute(possessionId, version);
        } catch (RuntimeException failure) {
            LOGGER.warn("Last board of possession {} could not be computed: {}", possessionId, failure.toString());
            last = lastBoards.get(possessionId);
        }
        lastBoards.remove(possessionId);
        if (last != null) {
            publisher.completeAll(last);
        }
    }

    BoardSnapshot compute(UUID possessionId, long version) {
        return readOnly.execute(status -> read(possessionId, version));
    }

    private BoardSnapshot read(UUID possessionId, long version) {
        Possession possession = possessions.findById(possessionId).orElseThrow(() -> new PossessionNotFoundException(possessionId));
        List<PossessionShift> members = shifts.findByPossession_IdOrderByTeamCodeAsc(possessionId);
        List<FieldCommandRecord> withAck = commands.findByPossessionIdAndRequiresAckTrueOrderBySequenceAsc(possessionId);
        Map<UUID, Set<String>> ackedTeams = new HashMap<>();
        if (!withAck.isEmpty()) {
            Map<UUID, String> teamOfShift = new HashMap<>();
            members.forEach(member -> teamOfShift.put(member.getShiftId(), member.getTeamCode()));
            for (CommandAckRecord ack : acks.findByCommandIdIn(withAck.stream().map(FieldCommandRecord::getId).toList())) {
                String team = teamOfShift.get(ack.getShiftId());
                if (team != null) {
                    ackedTeams.computeIfAbsent(ack.getCommandId(), id -> new HashSet<>()).add(team);
                }
            }
        }
        List<StreamPresence> streams = new ArrayList<>(presence.streamsOf(possessionId));
        Map<UUID, List<DeviceLiveness>> devicesByShift = new HashMap<>();
        for (DeviceLiveness device : mergedDevices(possessionId, streams)) {
            devicesByShift.computeIfAbsent(device.shiftId(), id -> new ArrayList<>()).add(device);
        }
        Instant now = clock.instant();
        List<String> teamCodes = members.stream().map(PossessionShift::getTeamCode).toList();
        List<TeamState> teams = members.stream().map(member -> teamState(member, devicesByShift.getOrDefault(member.getShiftId(), List.of()), now)).toList();
        List<CommandState> commandStates = withAck.stream().map(command -> {
            Set<String> sent = new HashSet<>();
            for (StreamPresence stream : streams) {
                if (stream.lastSentSequence() >= command.getSequence() && command.addressedTo(stream.shiftId())) {
                    sent.add(stream.teamCode());
                }
            }
            List<String> addressed = command.isBroadcast() ? teamCodes : members.stream()
                    .filter(member -> command.addressedTo(member.getShiftId())).map(PossessionShift::getTeamCode).toList();
            CommandAckSummary summary = CommandAckSummary.of(addressed, ackedTeams.getOrDefault(command.getId(), Set.of()), sent);
            return new CommandState(command.getId(), command.getSequence(), command.getKind(), command.getIssuedAt(), summary);
        }).toList();
        boolean allClear = !members.isEmpty() && members.stream().allMatch(PossessionShift::isClearOfTrack);
        return new BoardSnapshot(possessionId, version, possession.getStatus(), possession.getEndsAt(), allClear, teams, commandStates);
    }

    /**
     * Lo local y lo que cuentan las demas replicas, por dispositivo. Un dispositivo con stream
     * abierto aqui es de aqui; uno que aqui solo consta cerrado y otra replica cuenta abierto (se
     * fue alli) es de alli, y su stream remoto cuenta para {@code sent_to}.
     */
    private List<DeviceLiveness> mergedDevices(UUID possessionId, List<StreamPresence> streams) {
        Map<String, DeviceLiveness> byDevice = new HashMap<>();
        for (DeviceLiveness device : liveness.ofPossession(possessionId)) {
            byDevice.put(device.deviceId(), device);
        }
        for (RemoteDevice reported : remote.ofPossession(possessionId)) {
            ReplicaMessage.Device state = reported.state();
            DeviceLiveness local = byDevice.get(state.deviceId());
            if (local != null && (local.streamOpen() || !state.streamOpen() && !state.lastSeen().isAfter(local.lastSeen()))) {
                continue;
            }
            byDevice.put(state.deviceId(), new DeviceLiveness(state.deviceId(), state.shiftId(), state.possessionId(), state.lastSeen(),
                    state.kp() == null ? "" : state.kp(), state.batteryPct(), state.signalDbm(), state.streamOpen()));
            if (state.streamOpen()) {
                streams.add(new StreamPresence(state.deviceId(), state.shiftId(), state.teamCode(), state.lastSentSequence()));
            }
        }
        return List.copyOf(byDevice.values());
    }

    private TeamState teamState(PossessionShift member, List<DeviceLiveness> devices, Instant now) {
        List<Liveness> classified = devices.stream()
                .map(device -> TeamLiveness.classify(device.streamOpen(), Duration.between(device.lastSeen(), now), thresholds))
                .toList();
        DeviceLiveness latest = devices.stream().max(Comparator.comparing(DeviceLiveness::lastSeen)).orElse(null);
        return new TeamState(member.getShiftId(), member.getTeamCode(), TeamLiveness.best(classified),
                latest == null ? null : latest.lastSeen(), latest == null ? "" : latest.kp(), latest == null ? 0 : latest.batteryPct(),
                member.isClearOfTrack());
    }

    private static final class Loop {
        private final AtomicBoolean dirty = new AtomicBoolean();
        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicLong version;

        private Loop(long seed) {
            this.version = new AtomicLong(seed);
        }
    }
}
