package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.application.dto.PossessionView;
import com.alejandro.mtofield.application.dto.StoredCommand;
import com.alejandro.mtofield.application.event.CommandCommitted;
import com.alejandro.mtofield.application.event.CommandsFannedOut;
import com.alejandro.mtofield.application.event.PossessionClosed;
import com.alejandro.mtofield.application.exception.PossessionNotFoundException;
import com.alejandro.mtofield.application.replicas.ReplicaMessage;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.application.service.LivenessRegistry;
import com.alejandro.mtofield.application.service.LivenessRegistry.DeviceLiveness;
import com.alejandro.mtofield.application.service.PossessionBoardService;
import com.alejandro.mtofield.application.service.PossessionService;
import com.alejandro.mtofield.application.service.RemoteDeviceStates;
import com.alejandro.mtofield.application.service.ReplicaBus;
import com.alejandro.mtofield.application.service.ReplicaMessageHandler;
import com.alejandro.mtofield.configuration.grpc.FieldProperties;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * Esta replica entre las demas. Hacia fuera: cada orden confirmada (su numero), cada cierre de
 * posesion, el estado de cada dispositivo cuando se abre, late, se cierra o se le escribe, y la
 * despedida al parar. Hacia dentro: lo que cuentan las otras se aplica como si hubiera pasado
 * aqui (el despachador abanica la orden leida de la base, el estado remoto entra en el tablero,
 * un stream mas nuevo en otra replica sustituye al de aqui, un cierre cierra). Y el tic de puesta
 * al dia, que relee la base para lo que el bus no haya traido: es la garantia; el bus, la latencia.
 */
@Component
public class ReplicaRelay implements ReplicaMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaRelay.class);

    private final ReplicaBus bus;
    private final DeviceStreamRegistry registry;
    private final LivenessRegistry liveness;
    private final RemoteDeviceStates remote;
    private final CommandDispatcher dispatcher;
    private final FieldCommandService commands;
    private final PossessionService possessions;
    private final PossessionLifecycleListener lifecycle;
    private final PossessionBoardService board;
    private final FieldProperties properties;
    private final Clock clock;
    private final ExecutorService executor;
    /** Cuando se abrio cada stream de aqui: decide, frente a una apertura en otra replica, cual es la nueva. */
    private final Map<DeviceStream, Instant> openedAt = new ConcurrentHashMap<>();
    private final Map<String, Long> publishedSent = new ConcurrentHashMap<>();
    /** Parando: la despedida ya lo dice todo, y los cierres de los streams que siguen no se cuentan uno a uno. */
    private volatile boolean stopping;

    public ReplicaRelay(ReplicaBus bus, DeviceStreamRegistry registry, LivenessRegistry liveness, RemoteDeviceStates remote,
                        CommandDispatcher dispatcher, FieldCommandService commands, PossessionService possessions,
                        PossessionLifecycleListener lifecycle, PossessionBoardService board, FieldProperties properties, Clock clock,
                        @Qualifier("fieldStreamExecutor") ExecutorService executor) {
        this.bus = bus;
        this.registry = registry;
        this.liveness = liveness;
        this.remote = remote;
        this.dispatcher = dispatcher;
        this.commands = commands;
        this.possessions = possessions;
        this.lifecycle = lifecycle;
        this.board = board;
        this.properties = properties;
        this.clock = clock;
        this.executor = executor;
    }

    // ---------------------------------------------------------------------------------------
    // Hacia las demas replicas
    // ---------------------------------------------------------------------------------------

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onCommitted(CommandCommitted event) {
        offBand(() -> bus.publish(new ReplicaMessage.Command(event.possessionId(), event.command().getSequence())));
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onClosed(PossessionClosed event) {
        offBand(() -> bus.publish(new ReplicaMessage.Closed(event.possessionId())));
    }

    /** Tras un abanico, lo escrito en cada stream ha cambiado: se cuenta solo lo que cambio. */
    @EventListener
    public void onFannedOut(CommandsFannedOut event) {
        for (DeviceStream stream : registry.ofPossession(event.possessionId())) {
            Long known = publishedSent.get(stream.deviceId());
            if (known == null || known != stream.lastSentSequence()) {
                publishState(stream, true);
            }
        }
    }

    /** La despedida, sincrona: el broker sigue abierto en este momento y las demas dejan de contar con esta. */
    @EventListener
    public void onStopping(ContextClosedEvent event) {
        stopping = true;
        try {
            bus.publish(new ReplicaMessage.Stopped());
        } catch (RuntimeException ignored) {
            LOGGER.debug("Could not say goodbye to the other replicas: {}", ignored.toString());
        }
    }

    public void deviceOpened(DeviceStream stream) {
        openedAt.put(stream, clock.instant());
        publishState(stream, true);
    }

    public void deviceSpoke(DeviceStream stream) {
        publishState(stream, true);
    }

    public void deviceClosed(DeviceStream stream) {
        publishState(stream, false);
        openedAt.remove(stream);
        publishedSent.remove(stream.deviceId(), stream.lastSentSequence());
    }

    private void publishState(DeviceStream stream, boolean open) {
        if (stopping) {
            return;
        }
        DeviceLiveness known = liveness.ofDevice(stream.deviceId()).orElse(null);
        Instant now = clock.instant();
        ReplicaMessage.Device state = new ReplicaMessage.Device(stream.deviceId(), stream.shiftId(), stream.possessionId(), stream.teamCode(),
                openedAt.getOrDefault(stream, now), known == null ? now : known.lastSeen(), known == null ? "" : known.kp(),
                known == null ? 0 : known.batteryPct(), known == null ? 0 : known.signalDbm(), open, stream.lastSentSequence());
        publishedSent.put(stream.deviceId(), stream.lastSentSequence());
        offBand(() -> bus.publish(state));
    }

    private void offBand(Runnable publication) {
        try {
            executor.execute(publication);
        } catch (RejectedExecutionException shuttingDown) {
            LOGGER.debug("Replica message not published: executor shut down");
        }
    }

    // ---------------------------------------------------------------------------------------
    // Desde las demas replicas
    // ---------------------------------------------------------------------------------------

    @Override
    public void onReplicaMessage(String fromReplica, ReplicaMessage message) {
        switch (message) {
            case ReplicaMessage.Command command -> onRemoteCommand(command);
            case ReplicaMessage.Device device -> onRemoteDevice(fromReplica, device);
            case ReplicaMessage.Closed closed -> lifecycle.close(closed.possessionId());
            case ReplicaMessage.Stopped ignored -> {
                LOGGER.info("Replica {} stopped: what it told about its devices no longer counts", fromReplica);
                remote.forgetReplica(fromReplica).forEach(board::markDirty);
            }
        }
    }

    private void onRemoteCommand(ReplicaMessage.Command command) {
        List<StoredCommand> stored = commands.range(command.possessionId(), command.sequence(), command.sequence());
        if (stored.isEmpty()) {
            // Aun no es visible desde aqui, o nunca lo fue: el tic lo encontrara si existe.
            board.markDirty(command.possessionId());
            return;
        }
        StoredCommand first = stored.getFirst();
        dispatcher.dispatch(new CommandCommitted(command.possessionId(), first.targetShiftId(), first.command()));
    }

    private void onRemoteDevice(String fromReplica, ReplicaMessage.Device device) {
        remote.upsert(fromReplica, device, clock.instant());
        if (device.streamOpen()) {
            DeviceStream mine = registry.ofDevice(device.deviceId());
            if (mine != null && device.openedAt() != null) {
                Instant mineOpenedAt = openedAt.get(mine);
                if (mineOpenedAt == null || device.openedAt().isAfter(mineOpenedAt)) {
                    LOGGER.info("Device {} opened a newer stream on replica {}: the one here is superseded", device.deviceId(), fromReplica);
                    mine.supersede();
                }
            }
        }
        board.markDirty(device.possessionId());
    }

    // ---------------------------------------------------------------------------------------
    // El tic de puesta al dia
    // ---------------------------------------------------------------------------------------

    /** Lo que el bus no haya traido, desde la base: ordenes sin abanicar, posesiones ya cerradas, y lo remoto caducado. */
    public void catchUp() {
        remote.expire(clock.instant().minus(properties.replicas().remoteTtl())).forEach(board::markDirty);
        for (UUID possessionId : registry.possessionsWithLanes()) {
            try {
                PossessionView view = possessions.get(possessionId);
                if (view.status() == PossessionStatus.CLOSED) {
                    LOGGER.info("Possession {} is closed in the database: closing what this replica holds of it", possessionId);
                    lifecycle.close(possessionId);
                    continue;
                }
            } catch (PossessionNotFoundException gone) {
                lifecycle.close(possessionId);
                continue;
            } catch (RuntimeException failure) {
                LOGGER.warn("Catch-up of possession {} skipped: {}", possessionId, failure.toString());
                continue;
            }
            dispatcher.catchUp(possessionId);
        }
    }
}
