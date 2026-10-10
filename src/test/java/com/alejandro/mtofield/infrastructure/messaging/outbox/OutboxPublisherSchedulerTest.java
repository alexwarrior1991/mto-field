package com.alejandro.mtofield.infrastructure.messaging.outbox;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Orquestacion del relay: reclamar, publicar fuera de transaccion y cerrar el estado.
 */
@ExtendWith(MockitoExtension.class)
class OutboxPublisherSchedulerTest {

    @Mock
    private OutboxRelayService outboxRelayService;

    @Mock
    private OutboxRabbitPublisher outboxRabbitPublisher;

    @Spy
    private final OutboxMetrics outboxMetrics = new OutboxMetrics(new SimpleMeterRegistry());

    @InjectMocks
    private OutboxPublisherScheduler scheduler;

    /**
     * El publisher falla solo para un mensaje concreto. Se estubea sobre any() en vez
     * de sobre el argumento: con stubbing estricto, un stub por argumento hace que
     * Mockito aborte en la primera llamada con otro mensaje.
     */
    private void failFor(OutboxRecord objetivo) {
        doAnswer(invocation -> {
            OutboxRecord record = invocation.getArgument(0);
            if (record.id().equals(objetivo.id())) {
                throw new OutboxPublishException("nack");
            }
            return null;
        }).when(outboxRabbitPublisher).publish(any());
    }

    private OutboxRecord record(String eventType) {
        return new OutboxRecord(
                UUID.randomUUID(), "station", "1", eventType,
                "mto.master-data.exchange", "mto.master-data.station.created", "{}", 0, 1L, null, null);
    }

    @Test
    void marcaPublishedSoloDespuesDeQueElPublisherConfirme() {
        OutboxRecord record = record("MASTER_DATA_STATION_CREATED");
        when(outboxRelayService.claimBatch()).thenReturn(List.of(record));

        scheduler.publishPendingMessages();

        InOrder orden = inOrder(outboxRabbitPublisher, outboxRelayService);
        orden.verify(outboxRabbitPublisher).publish(record);
        orden.verify(outboxRelayService).markPublished(record.id());
        verify(outboxRelayService, never()).markFailed(any(), any());
        verify(outboxMetrics).recordPublished();
    }

    @Test
    void unFalloDePublicacionSeRegistraSinMarcarPublished() {
        OutboxRecord record = record("MASTER_DATA_STATION_CREATED");
        when(outboxRelayService.claimBatch()).thenReturn(List.of(record));
        OutboxPublishException fallo = new OutboxPublishException("nack");
        doThrow(fallo).when(outboxRabbitPublisher).publish(record);

        scheduler.publishPendingMessages();

        verify(outboxRelayService).markFailed(record.id(), fallo);
        verify(outboxRelayService, never()).markPublished(any());
        verify(outboxMetrics).recordPublishFailure();
        verify(outboxMetrics, never()).recordPublished();
    }

    @Test
    void unMensajeQueFallaNoImpideProcesarElRestoDelLote() {
        OutboxRecord primero = record("MASTER_DATA_STATION_CREATED");
        OutboxRecord segundo = record("MASTER_DATA_TRACK_UPDATED");
        OutboxRecord tercero = record("MASTER_DATA_PROFILE_DELETED");
        when(outboxRelayService.claimBatch()).thenReturn(List.of(primero, segundo, tercero));
        failFor(segundo);

        scheduler.publishPendingMessages();

        verify(outboxRelayService).markPublished(primero.id());
        verify(outboxRelayService).markFailed(eq(segundo.id()), any());
        verify(outboxRelayService).markPublished(tercero.id());
    }

    @Test
    void siNoSePuedeRegistrarElFalloElLoteContinua() {
        OutboxRecord primero = record("MASTER_DATA_STATION_CREATED");
        OutboxRecord segundo = record("MASTER_DATA_TRACK_UPDATED");
        when(outboxRelayService.claimBatch()).thenReturn(List.of(primero, segundo));
        failFor(primero);
        doThrow(new IllegalStateException("base de datos caida"))
                .when(outboxRelayService).markFailed(eq(primero.id()), any());

        scheduler.publishPendingMessages();

        // El mensaje se recuperara al expirar su visibilidad; el resto del lote sigue.
        verify(outboxRelayService).markPublished(segundo.id());
    }

    @Test
    void sinMensajesReclamadosNoSeTocaElBroker() {
        when(outboxRelayService.claimBatch()).thenReturn(List.of());

        scheduler.publishPendingMessages();

        verifyNoInteractions(outboxRabbitPublisher);
    }
}
