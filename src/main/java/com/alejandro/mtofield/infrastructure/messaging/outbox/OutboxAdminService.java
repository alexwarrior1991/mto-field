package com.alejandro.mtofield.infrastructure.messaging.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Limit;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Operaciones de explotacion sobre el outbox.
 * <p>
 * FAILED era un estado terminal sin salida: un mensaje que agotaba los intentos se
 * quedaba ahi para siempre y el dato maestro quedaba desincronizado sin remedio. El
 * redrive lo devuelve a la cola de publicacion una vez corregida la causa.
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxAdminService {

    private final OutboxMessageRepository outboxMessageRepository;

    /**
     * Foto completa, incluido el total de PUBLISHED. Es una consulta bajo demanda:
     * ese recuento recorre el grueso de la tabla, asi que no vale para metricas
     * periodicas. Para eso esta {@link #metricsSnapshot()}.
     */
    @Transactional(readOnly = true)
    public OutboxStats stats() {
        OutboxRelayHealth health = metricsSnapshot();

        return new OutboxStats(
                health.pending(),
                health.inProgress(),
                outboxMessageRepository.countByStatus(OutboxStatus.PUBLISHED),
                health.failed(),
                health.oldestPendingCreatedAt()
        );
    }

    /** Recuentos baratos, servidos por los indices parciales de outbox_message. */
    @Transactional(readOnly = true)
    public OutboxRelayHealth metricsSnapshot() {
        return new OutboxRelayHealth(
                outboxMessageRepository.countByStatus(OutboxStatus.PENDING),
                outboxMessageRepository.countByStatus(OutboxStatus.IN_PROGRESS),
                outboxMessageRepository.countByStatus(OutboxStatus.FAILED),
                outboxMessageRepository.findOldestCreatedAt(OutboxStatus.PENDING)
        );
    }

    /**
     * Devuelve a PENDING los mensajes FAILED mas antiguos, reseteando su contador de
     * intentos para que vuelvan a disponer de la ventana completa de backoff.
     *
     * @return numero de mensajes reencolados
     */
    @Transactional
    public int redriveFailed(int limit) {
        if (limit <= 0) {
            return 0;
        }

        List<OutboxMessage> failed = outboxMessageRepository
                .findByStatusOrderByCreatedAtAsc(OutboxStatus.FAILED, Limit.of(limit));

        Instant now = Instant.now();

        failed.forEach(message -> {
            message.setStatus(OutboxStatus.PENDING);
            message.setAttempts(0);
            message.setNextAttemptAt(now);
            message.setLastError(null);
        });

        if (!failed.isEmpty()) {
            log.warn("Outbox: {} mensajes FAILED devueltos a PENDING por redrive manual", failed.size());
        }

        return failed.size();
    }
}
