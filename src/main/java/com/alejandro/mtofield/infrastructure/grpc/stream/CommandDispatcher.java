package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.application.dto.StoredCommand;
import com.alejandro.mtofield.application.event.CommandCommitted;
import com.alejandro.mtofield.application.event.CommandsFannedOut;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.application.service.PossessionBoardService;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.infrastructure.grpc.stream.DeviceStreamRegistry.PossessionLane;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * El abanico ordenado de las ordenes confirmadas hacia los streams de su posesion.
 *
 * <p>Recibe {@link CommandCommitted} <b>despues del commit</b> y sale del hilo que confirmo
 * (la lectura de relleno no debe correr con la sincronizacion de esa transaccion aun atada).
 * Bajo el candado del carril: lo ya abanicado se ignora; si hay un salto respecto a lo ultimo
 * abanicado, se rellena de la base (existe, porque las secuencias se confirman en orden); y se
 * abanica a cada stream de la posesion (difusion) o del turno destino. Asi cada stream recibe
 * ofertas en orden, y su dedupe absorbe cualquier reenvio: el despachador puede repetir, nunca
 * deja un hueco.</p>
 */
@Component
public class CommandDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandDispatcher.class);

    private final DeviceStreamRegistry registry;
    private final FieldCommandService commands;
    private final PossessionBoardService board;
    private final ExecutorService executor;
    private final ApplicationEventPublisher events;

    public CommandDispatcher(DeviceStreamRegistry registry, FieldCommandService commands, PossessionBoardService board,
                             @Qualifier("fieldStreamExecutor") ExecutorService executor, ApplicationEventPublisher events) {
        this.registry = registry;
        this.commands = commands;
        this.board = board;
        this.executor = executor;
        this.events = events;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onCommitted(CommandCommitted event) {
        try {
            executor.execute(() -> dispatch(event));
        } catch (RejectedExecutionException shuttingDown) {
            LOGGER.debug("Command #{} of possession {} not dispatched: executor shut down", event.command().getSequence(), event.possessionId());
        }
    }

    /** Visible para los tests: el abanico de una orden ya confirmada, en el hilo que llama. */
    public void dispatch(CommandCommitted event) {
        try {
            fanOut(event);
        } finally {
            // Una orden nueva, o el resultado de un evento, cambian el tablero aunque nadie este conectado.
            board.markDirty(event.possessionId());
        }
    }

    private void fanOut(CommandCommitted event) {
        PossessionLane lane = registry.lane(event.possessionId());
        if (lane == null) {
            // Ningun dispositivo de la posesion en esta replica: la reanudacion lo recuperara.
            return;
        }
        long sequence = event.command().getSequence();
        lane.lock().lock();
        try {
            long last = lane.lastDispatched();
            if (sequence <= last) {
                return;
            }
            if (sequence > last + 1) {
                List<StoredCommand> gap = commands.range(event.possessionId(), last + 1, sequence - 1);
                LOGGER.debug("Filling {} command(s) of possession {} between #{} and #{}", gap.size(), event.possessionId(), last + 1, sequence - 1);
                for (StoredCommand stored : gap) {
                    fanOut(lane, stored.targetShiftId(), stored.command());
                }
            }
            fanOut(lane, event.targetShiftId(), event.command());
            lane.lastDispatched(sequence);
        } finally {
            lane.lock().unlock();
        }
        events.publishEvent(new CommandsFannedOut(event.possessionId()));
    }

    /**
     * La puesta al dia desde la base: lo que el carril de la posesion aun no abanico (porque el
     * aviso de otra replica no llego, o porque no hay bus) se lee y se abanica ahora. Es la
     * garantia de que ninguna replica se queda atras; el bus solo adelanta este momento.
     *
     * @return cuantas ordenes se abanicaron
     */
    public int catchUp(UUID possessionId) {
        PossessionLane lane = registry.lane(possessionId);
        if (lane == null) {
            return 0;
        }
        long max = commands.maxSequence(possessionId);
        int fanned = 0;
        lane.lock().lock();
        try {
            long last = lane.lastDispatched();
            if (max <= last) {
                return 0;
            }
            for (StoredCommand stored : commands.range(possessionId, last + 1, max)) {
                fanOut(lane, stored.targetShiftId(), stored.command());
                fanned++;
            }
            lane.lastDispatched(max);
        } finally {
            lane.lock().unlock();
        }
        LOGGER.info("Catch-up of possession {}: {} command(s) up to #{} fanned out from the database", possessionId, fanned, max);
        events.publishEvent(new CommandsFannedOut(possessionId));
        board.markDirty(possessionId);
        return fanned;
    }

    private static void fanOut(PossessionLane lane, UUID targetShiftId, FieldCommand command) {
        for (DeviceStream stream : lane.streams()) {
            if (targetShiftId == null || targetShiftId.equals(stream.shiftId())) {
                stream.offer(command);
            }
        }
    }
}
