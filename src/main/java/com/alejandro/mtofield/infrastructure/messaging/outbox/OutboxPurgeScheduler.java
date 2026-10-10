package com.alejandro.mtofield.infrastructure.messaging.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Duration;
import java.time.Instant;

/**
 * Borra los mensajes ya publicados que han superado el periodo de retencion.
 * <p>
 * Sin esto outbox_message solo crece: cada alta, modificacion y baja de dato maestro
 * deja una fila con su JSON, y bulkCreate/bulkUpdate publican un evento POR ENTIDAD.
 * Acaban siendo millones de filas de payload dentro de la base transaccional de
 * negocio.
 * <p>
 * Solo se tocan los PUBLISHED. Los FAILED se quedan: son justo los que hay que mirar,
 * y borrarlos seria tapar el problema.
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxPurgeScheduler {

    private final OutboxPurgeService outboxPurgeService;
    private final OutboxProperties outboxProperties;

    @Scheduled(fixedDelayString = "${app.outbox.purge.fixed-delay:1h}")
    public void purgePublishedMessages() {
        OutboxProperties.Purge purge = outboxProperties.getPurge();

        Instant threshold = Instant.now().minus(purge.getRetention());
        int borrados = 0;

        for (int lote = 0; lote < purge.getMaxBatchesPerRun(); lote++) {
            int borradosEnLote = outboxPurgeService.purgeBatch(threshold, purge.getBatchSize());
            borrados += borradosEnLote;

            // Lote incompleto: ya no queda nada mas que borrar por debajo del umbral.
            if (borradosEnLote < purge.getBatchSize()) {
                break;
            }
        }

        if (borrados > 0) {
            log.info(
                    "Outbox: purgados {} mensajes publicados anteriores a {} (retencion {})",
                    borrados, threshold, formatRetention(purge.getRetention())
            );
        }
    }

    private String formatRetention(Duration retention) {
        return retention.toDays() > 0 ? retention.toDays() + "d" : retention.toHours() + "h";
    }
}
