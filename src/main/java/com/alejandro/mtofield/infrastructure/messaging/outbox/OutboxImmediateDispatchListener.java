package com.alejandro.mtofield.infrastructure.messaging.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Despierta al relay cuando la transaccion de negocio ha confirmado.
 * <p>
 * AFTER_COMMIT y no antes: si se publicara con la transaccion aun abierta, el relay
 * podria no ver todavia la fila, o peor, publicar un evento de un cambio que despues
 * hace rollback. Es la misma razon por la que existe el outbox.
 */
@RequiredArgsConstructor
public class OutboxImmediateDispatchListener {

    private final OutboxDispatchTrigger outboxDispatchTrigger;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onOutboxMessageSaved(OutboxMessageSavedEvent event) {
        outboxDispatchTrigger.requestDispatch();
    }
}
