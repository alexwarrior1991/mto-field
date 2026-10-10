package com.alejandro.mtofield.infrastructure.messaging.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Frontera transaccional del relay: reclamar el lote y cerrar el estado de cada
 * mensaje, siempre en transacciones CORTAS.
 * <p>
 * La publicacion a RabbitMQ ocurre fuera de estas transacciones, y es deliberado.
 * Antes el scheduler entero era {@code @Transactional} y mantenia abierta una
 * conexion del pool mientras hacia hasta 50 llamadas de red al broker; ademas, si
 * el commit final fallaba, los 50 mensajes ya estaban enviados pero seguian en
 * PENDING y se reenviaban enteros. Nunca I/O de red dentro de una transaccion.
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxRelayService {

    private final OutboxMessageRepository outboxMessageRepository;
    private final OutboxProperties outboxProperties;
    private final OutboxRetryPolicy outboxRetryPolicy;

    /**
     * Reclama un lote y lo deja invisible para el resto de replicas durante
     * {@code claimVisibilityTimeout}.
     */
    @Transactional
    public List<OutboxRecord> claimBatch() {
        Instant now = Instant.now();
        Instant invisibleUntil = now.plus(outboxProperties.getClaimVisibilityTimeout());

        List<OutboxMessage> claimed = outboxProperties.isStrictOrderingPerAggregate()
                ? outboxMessageRepository.claimBatchInOrder(now, outboxProperties.getBatchSize())
                : outboxMessageRepository.claimBatch(now, outboxProperties.getBatchSize());

        return claimed.stream()
                .map(message -> claim(message, invisibleUntil))
                .toList();
    }

    @Transactional
    public void markPublished(UUID id) {
        outboxMessageRepository.findById(id).ifPresent(message -> {
            message.setStatus(OutboxStatus.PUBLISHED);
            message.setPublishedAt(Instant.now());
            message.setNextAttemptAt(null);
            message.setLastError(null);
        });
    }

    /**
     * Contabiliza el fallo y programa el siguiente intento, o da el mensaje por
     * perdido si ya no quedan.
     */
    @Transactional
    public void markFailed(UUID id, Throwable error) {
        outboxMessageRepository.findById(id).ifPresent(message -> {
            int attempts = message.getAttempts() + 1;

            message.setAttempts(attempts);
            message.setLastError(OutboxErrors.describe(error));

            if (outboxRetryPolicy.isExhausted(attempts, message.getMaxAttempts())) {
                message.setStatus(OutboxStatus.FAILED);
                message.setNextAttemptAt(null);

                log.error(
                        "Outbox message agotado tras {} intentos, queda en FAILED. id={}, eventType={}",
                        attempts, message.getId(), message.getEventType(), error
                );
                return;
            }

            Duration delay = outboxRetryPolicy.nextDelay(attempts);
            message.setStatus(OutboxStatus.PENDING);
            message.setNextAttemptAt(Instant.now().plus(delay));

            log.warn(
                    "Error publicando outbox message, reintento en {}. id={}, intento={}/{}, eventType={}",
                    delay, message.getId(), attempts, message.getMaxAttempts(), message.getEventType(), error
            );
        });
    }

    private OutboxRecord claim(OutboxMessage message, Instant invisibleUntil) {
        if (message.getStatus() == OutboxStatus.IN_PROGRESS) {
            // La replica que lo reclamo antes murio sin cerrarlo. Se cuenta como intento
            // consumido para que un mensaje que tumba al proceso no gire indefinidamente.
            message.setAttempts(message.getAttempts() + 1);

            log.warn(
                    "Outbox message recuperado tras expirar su visibilidad. id={}, intento={}",
                    message.getId(), message.getAttempts()
            );
        }

        message.setStatus(OutboxStatus.IN_PROGRESS);
        message.setNextAttemptAt(invisibleUntil);

        return OutboxRecord.from(message);
    }
}
