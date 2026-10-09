package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.application.event.PossessionClosed;
import com.alejandro.mtofield.application.service.LivenessRegistry;
import com.alejandro.mtofield.application.service.PossessionBoardService;
import com.alejandro.mtofield.application.service.RemoteDeviceStates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.UUID;

/**
 * Cuando una posesion queda cerrada en la base: fin normal de los streams de sus dispositivos
 * ({@code onCompleted}), el ultimo tablero y fin normal de sus observadores, y fuera lo que esta
 * replica guardaba de ella en memoria.
 */
@Component
public class PossessionLifecycleListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(PossessionLifecycleListener.class);

    private final DeviceStreamRegistry registry;
    private final LivenessRegistry liveness;
    private final PossessionBoardService board;
    private final RemoteDeviceStates remote;

    public PossessionLifecycleListener(DeviceStreamRegistry registry, LivenessRegistry liveness, PossessionBoardService board,
                                       RemoteDeviceStates remote) {
        this.registry = registry;
        this.liveness = liveness;
        this.board = board;
        this.remote = remote;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onClosed(PossessionClosed event) {
        close(event.possessionId());
    }

    /** Lo mismo cuando el cierre lo cuenta otra replica, o lo encuentra el tic de puesta al dia en la base. */
    public void close(UUID possessionId) {
        LOGGER.info("Possession {} closed: completing {} device stream(s)", possessionId, registry.ofPossession(possessionId).size());
        registry.closeAll(possessionId);
        board.possessionClosed(possessionId);
        liveness.forget(possessionId);
        remote.forget(possessionId);
    }
}
