package com.alejandro.mtofield.infrastructure.messaging.outbox;

import java.time.Instant;

/**
 * Estado del circuito de publicacion, con los recuentos BARATOS del outbox.
 * <p>
 * Los tres estados que lleva estan cubiertos por indices parciales diminutos, de
 * modo que se puede consultar cada pocos segundos sin coste apreciable. El total de
 * PUBLISHED se deja fuera aposta: contarlo obliga a recorrer el grueso de la tabla,
 * y lo que se quiere saber de verdad (el ritmo de publicacion) lo da el contador
 * outbox.publish.total. El total absoluto sigue estando en /actuator/outbox, que se
 * consulta a mano.
 *
 * @param oldestPendingCreatedAt fecha del PENDING mas antiguo, o null si no hay
 */
public record OutboxRelayHealth(
        long pending,
        long inProgress,
        long failed,
        Instant oldestPendingCreatedAt
) {
}
