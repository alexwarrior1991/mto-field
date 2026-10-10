package com.alejandro.mtofield.infrastructure.messaging.outbox;

import java.time.Instant;

/**
 * Foto del estado del outbox.
 *
 * @param oldestPendingCreatedAt fecha del PENDING mas antiguo, o null si no hay.
 *                               Es la senal mas util: si envejece, el relay esta roto,
 *                               sea cual sea la causa.
 */
public record OutboxStats(
        long pending,
        long inProgress,
        long published,
        long failed,
        Instant oldestPendingCreatedAt
) {
}
