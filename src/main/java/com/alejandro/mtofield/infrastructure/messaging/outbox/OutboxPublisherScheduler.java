package com.alejandro.mtofield.infrastructure.messaging.outbox;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Motor del outbox: reclama lotes mientras haya trabajo, publica cada mensaje y cierra
 * su estado.
 * <p>
 * Deliberadamente SIN {@code @Transactional}. Las unicas transacciones son las cortas
 * de {@link OutboxRelayService}; entre ellas se hace la I/O contra RabbitMQ, que no
 * debe retener una conexion de base de datos.
 * <p>
 * Tres reglas sostienen "al menos una vez, en orden por agregado y sin duplicar por
 * lentitud":
 * <ul>
 *   <li><b>El reclamo se renueva antes de cada publicacion.</b> Un lote puede durar mas
 *       que su visibilidad (un broker que no confirma cuesta {@code confirmTimeout} por
 *       mensaje); el mensaje cuyo reclamo ya se ha llevado otra replica no se publica
 *       aqui. La visibilidad solo tiene que cubrir UNA publicacion, no el lote.</li>
 *   <li><b>La pasada tiene un plazo</b> ({@code passTimeBudget}). Al cumplirse, lo
 *       reclamado sin empezar se suelta en vez de dejarlo caducar.</li>
 *   <li><b>La pasada vacia.</b> Con el orden por agregado cada lote lleva un mensaje por
 *       agregado; se siguen reclamando lotes hasta que no quede nada, se acabe el plazo
 *       o un lote entero no consiga publicar nada (el broker no esta: insistir solo
 *       quemaria intentos).</li>
 * </ul>
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxPublisherScheduler {

    private final OutboxRelayService outboxRelayService;
    private final OutboxRabbitPublisher outboxRabbitPublisher;
    private final OutboxMetrics outboxMetrics;
    private final OutboxProperties outboxProperties;

    /** Reloj monotono del plazo de la pasada; los tests lo sustituyen. */
    @Setter(AccessLevel.PACKAGE)
    private LongSupplier nanoClock = System::nanoTime;

    @Scheduled(fixedDelayString = "${app.outbox.publisher-fixed-delay:5000}")
    public void publishPendingMessages() {
        long deadline = nanoClock.getAsLong() + outboxProperties.getPassTimeBudget().toNanos();

        while (!expired(deadline)) {
            List<OutboxRecord> batch = outboxRelayService.claimBatch();

            if (batch.isEmpty()) {
                return;
            }

            log.debug("Outbox: lote reclamado con {} mensajes", batch.size());

            if (publishBatch(batch, deadline) == 0) {
                // Nada ha salido: el broker no confirma o el lote entero ya era de otro.
                // Lo que falla queda aplazado por su backoff; seguir reclamando solo
                // gastaria los intentos del resto.
                return;
            }
        }
    }

    /** @return cuantos mensajes del lote han quedado publicados */
    private int publishBatch(List<OutboxRecord> batch, long deadline) {
        int published = 0;

        for (int index = 0; index < batch.size(); index++) {
            if (expired(deadline)) {
                releaseQuietly(batch.subList(index, batch.size()));
                break;
            }

            if (publish(batch.get(index))) {
                published++;
            }
        }

        return published;
    }

    /**
     * Un mensaje no puede tumbar el resto del lote. Los que no se cierren aqui por lo
     * que sea siguen protegidos por la expiracion de visibilidad: otra pasada volvera
     * a cogerlos, con ese intento ya contado.
     */
    private boolean publish(OutboxRecord claimed) {
        Optional<OutboxRecord> renewed = renewQuietly(claimed);

        if (renewed.isEmpty()) {
            return false;
        }

        OutboxRecord record = renewed.get();

        try {
            outboxRabbitPublisher.publish(record);
            outboxRelayService.markPublished(record.id());
            outboxMetrics.recordPublished();
            return true;
        } catch (Exception exception) {
            outboxMetrics.recordPublishFailure();
            markFailedQuietly(record, exception);
            return false;
        }
    }

    private Optional<OutboxRecord> renewQuietly(OutboxRecord record) {
        try {
            Optional<OutboxRecord> renewed = outboxRelayService.renewClaim(record);

            if (renewed.isEmpty()) {
                outboxMetrics.recordClaimLost();
            }
            return renewed;
        } catch (Exception exception) {
            // Sin saber si el reclamo sigue siendo nuestro no se publica.
            log.error(
                    "No se ha podido renovar el reclamo del outbox message id={}. Se recuperara al expirar su visibilidad.",
                    record.id(), exception
            );
            return Optional.empty();
        }
    }

    private void markFailedQuietly(OutboxRecord record, Exception cause) {
        try {
            outboxRelayService.markFailed(record, cause);
        } catch (Exception exception) {
            log.error(
                    "No se ha podido registrar el fallo del outbox message id={}. Se recuperara al expirar su visibilidad.",
                    record.id(), exception
            );
        }
    }

    private void releaseQuietly(List<OutboxRecord> notAttempted) {
        log.debug("Outbox: plazo de la pasada cumplido, se sueltan {} mensajes sin intentar", notAttempted.size());

        for (OutboxRecord record : notAttempted) {
            try {
                outboxRelayService.releaseClaim(record);
            } catch (Exception exception) {
                log.warn(
                        "No se ha podido soltar el outbox message id={}. Se recuperara al expirar su visibilidad.",
                        record.id(), exception
                );
            }
        }
    }

    private boolean expired(long deadline) {
        return nanoClock.getAsLong() - deadline >= 0;
    }
}
