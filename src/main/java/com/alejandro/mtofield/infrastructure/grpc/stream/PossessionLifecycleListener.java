package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.application.event.PossessionClosed;
import com.alejandro.mtofield.application.service.LivenessRegistry;
import com.alejandro.mtofield.application.service.PossessionBoardService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

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

    public PossessionLifecycleListener(DeviceStreamRegistry registry, LivenessRegistry liveness, PossessionBoardService board) {
        this.registry = registry;
        this.liveness = liveness;
        this.board = board;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onClosed(PossessionClosed event) {
        LOGGER.info("Possession {} closed: completing {} device stream(s)", event.possessionId(), registry.ofPossession(event.possessionId()).size());
        registry.closeAll(event.possessionId());
        board.possessionClosed(event.possessionId());
        liveness.forget(event.possessionId());
    }
}
