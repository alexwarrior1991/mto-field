package com.alejandro.mtofield.infrastructure.messaging.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

/**
 * Motor del outbox: reclama un lote, lo publica y cierra el estado de cada mensaje.
 * <p>
 * Deliberadamente SIN {@code @Transactional}. Las unicas transacciones son las cortas
 * de {@link OutboxRelayService}; entre ellas se hace la I/O contra RabbitMQ, que no
 * debe retener una conexion de base de datos.
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxPublisherScheduler {

    private final OutboxRelayService outboxRelayService;
    private final OutboxRabbitPublisher outboxRabbitPublisher;
    private final OutboxMetrics outboxMetrics;

    @Scheduled(fixedDelayString = "${app.outbox.publisher-fixed-delay:5000}")
    public void publishPendingMessages() {
        List<OutboxRecord> batch = outboxRelayService.claimBatch();

        if (batch.isEmpty()) {
            return;
        }

        log.debug("Outbox: lote reclamado con {} mensajes", batch.size());

        batch.forEach(this::publish);
    }

    /**
     * Un mensaje no puede tumbar el resto del lote. Los que no se cierren aqui por lo
     * que sea siguen protegidos por la expiracion de visibilidad: otra pasada volvera
     * a cogerlos.
     */
    private void publish(OutboxRecord record) {
        try {
            outboxRabbitPublisher.publish(record);
            outboxRelayService.markPublished(record.id());
            outboxMetrics.recordPublished();
        } catch (Exception exception) {
            outboxMetrics.recordPublishFailure();
            markFailedQuietly(record, exception);
        }
    }

    private void markFailedQuietly(OutboxRecord record, Exception cause) {
        try {
            outboxRelayService.markFailed(record.id(), cause);
        } catch (Exception exception) {
            log.error(
                    "No se ha podido registrar el fallo del outbox message id={}. Se recuperara al expirar su visibilidad.",
                    record.id(), exception
            );
        }
    }
}
