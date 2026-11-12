package com.alejandro.mtofield.infrastructure.messaging.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Frontera transaccional del relay: reclamar el lote, renovar el reclamo de cada
 * mensaje antes de publicarlo y cerrar su estado, siempre en transacciones CORTAS.
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
     * <p>
     * El intento se cuenta AQUI, al reclamar, y no al fallar: asi lo consume tambien
     * el mensaje que tumba al proceso, o cuyo fallo no se llega a apuntar, y acaba en
     * FAILED como cualquier otro en vez de girar para siempre reteniendo a su agregado.
     * Lo que llega con los intentos ya agotados (solo puede ser un IN_PROGRESS que
     * nadie cerro) no se entrega: queda en FAILED en esta misma transaccion.
     */
    @Transactional
    public List<OutboxRecord> claimBatch() {
        Instant now = Instant.now();
        Instant invisibleUntil = claimToken(now.plus(outboxProperties.getClaimVisibilityTimeout()));

        while (true) {
            List<OutboxMessage> candidates = outboxProperties.isStrictOrderingPerAggregate()
                    ? outboxMessageRepository.claimBatchInOrder(now, outboxProperties.getBatchSize())
                    : outboxMessageRepository.claimBatch(now, outboxProperties.getBatchSize());

            List<OutboxRecord> claimed = candidates.stream()
                    .map(message -> claim(message, invisibleUntil))
                    .flatMap(Optional::stream)
                    .toList();

            if (!claimed.isEmpty() || candidates.isEmpty()) {
                return claimed;
            }

            // Todo lo que habia eran mensajes agotados, que acaban de quedar en FAILED y
            // ya no retienen a los siguientes de su agregado: se vuelve a mirar, para que
            // un lote vacio signifique siempre "no hay trabajo". Termina porque cada
            // vuelta cierra al menos un mensaje.
            outboxMessageRepository.flush();
        }
    }

    /**
     * Renueva el reclamo de un mensaje justo antes de publicarlo.
     * <p>
     * Es a la vez la comprobacion de que el mensaje sigue siendo de este relay y la
     * prorroga que le da una visibilidad entera para publicarlo. Si otra replica se lo
     * ha llevado (el lote ha durado mas que su visibilidad, los relojes difieren) o ya
     * esta cerrado, no cambia nada y devuelve vacio: ese mensaje NO se publica aqui.
     *
     * @return el record con su reclamo renovado, o vacio si el reclamo ya no es suyo
     */
    @Transactional
    public Optional<OutboxRecord> renewClaim(OutboxRecord record) {
        Instant renewedUntil = claimToken(Instant.now().plus(outboxProperties.getClaimVisibilityTimeout()));

        if (record.claimedUntil() != null && renewedUntil.isBefore(record.claimedUntil())) {
            // Nunca se acorta un reclamo: el testigo solo avanza.
            renewedUntil = record.claimedUntil();
        }

        int renewed = outboxMessageRepository.renewClaim(record.id(), record.claimedUntil(), renewedUntil);

        if (renewed == 0) {
            log.warn(
                    "Outbox message no publicado: su reclamo ya no es de este relay. id={}, eventType={}",
                    record.id(), record.eventType()
            );
            return Optional.empty();
        }

        return Optional.of(record.renewedUntil(renewedUntil));
    }

    /**
     * Suelta un mensaje reclamado que la pasada no ha llegado a intentar: vuelve a
     * PENDING, disponible ya, sin el intento que se le conto al reclamarlo.
     */
    @Transactional
    public void releaseClaim(OutboxRecord record) {
        outboxMessageRepository.releaseClaimed(record.id(), record.claimedUntil(), Instant.now());
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
     * Programa el siguiente intento, o da el mensaje por perdido si este era el ultimo.
     * <p>
     * El intento ya esta contado desde el reclamo, asi que aqui solo se decide el
     * destino. La escritura es condicional al reclamo: si el mensaje ya no es de este
     * relay no se toca, porque devolverlo a PENDING lo pondria en manos de un tercero
     * mientras su dueno actual lo esta publicando.
     *
     * @return false si el reclamo ya no era de este relay y no se ha escrito nada
     */
    @Transactional
    public boolean markFailed(OutboxRecord record, Throwable error) {
        String lastError = OutboxErrors.describe(error);

        if (outboxRetryPolicy.isExhausted(record.attempts(), record.maxAttempts())) {
            boolean failed = outboxMessageRepository
                    .failClaimed(record.id(), record.claimedUntil(), lastError) == 1;

            if (failed) {
                log.error(
                        "Outbox message agotado tras {} intentos, queda en FAILED. id={}, eventType={}",
                        record.attempts(), record.id(), record.eventType(), error
                );
            }
            return failed;
        }

        Duration delay = outboxRetryPolicy.nextDelay(record.attempts());
        boolean rescheduled = outboxMessageRepository.rescheduleClaimed(
                record.id(), record.claimedUntil(), Instant.now().plus(delay), lastError) == 1;

        if (rescheduled) {
            log.warn(
                    "Error publicando outbox message, reintento en {}. id={}, intento={}/{}, eventType={}",
                    delay, record.id(), record.attempts(), record.maxAttempts(), record.eventType(), error
            );
        } else {
            log.warn(
                    "Fallo de publicacion no apuntado: el reclamo ya no es de este relay. id={}, eventType={}",
                    record.id(), record.eventType(), error
            );
        }
        return rescheduled;
    }

    private Optional<OutboxRecord> claim(OutboxMessage message, Instant invisibleUntil) {
        boolean abandoned = message.getStatus() == OutboxStatus.IN_PROGRESS;

        if (outboxRetryPolicy.isExhausted(message.getAttempts(), message.getMaxAttempts())) {
            // Reclamado tantas veces como intentos tiene y ninguna se cerro: o tumba al
            // relay, o su cierre falla siempre. Queda en FAILED, que es donde lo ven la
            // metrica y /actuator/outbox, y deja de retener a los siguientes de su agregado.
            message.setStatus(OutboxStatus.FAILED);
            message.setNextAttemptAt(null);
            message.setLastError(OutboxErrors.describe(new IllegalStateException(
                    "Reclamado %d veces sin que ningun relay llegara a cerrarlo".formatted(message.getAttempts()))));

            log.error(
                    "Outbox message agotado tras {} intentos sin cerrar, queda en FAILED. id={}, eventType={}",
                    message.getAttempts(), message.getId(), message.getEventType()
            );
            return Optional.empty();
        }

        message.setAttempts(message.getAttempts() + 1);

        if (abandoned) {
            log.warn(
                    "Outbox message recuperado tras expirar su visibilidad. id={}, intento={}/{}",
                    message.getId(), message.getAttempts(), message.getMaxAttempts()
            );
        }

        message.setStatus(OutboxStatus.IN_PROGRESS);
        message.setNextAttemptAt(invisibleUntil);

        return Optional.of(OutboxRecord.from(message));
    }

    /**
     * PostgreSQL guarda microsegundos: el testigo se recorta a esa precision para que
     * el instante que se lleva el relay sea exactamente el que queda en la fila.
     */
    private static Instant claimToken(Instant instant) {
        return instant.truncatedTo(ChronoUnit.MICROS);
    }
}
