package com.alejandro.mtofield.infrastructure.messaging.outbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
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

    private final OutboxProperties outboxProperties = new OutboxProperties();

    /** Reloj del plazo de la pasada, en nanosegundos; cada test lo mueve a mano. */
    private final AtomicLong reloj = new AtomicLong();

    private OutboxPublisherScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new OutboxPublisherScheduler(
                outboxRelayService, outboxRabbitPublisher, outboxMetrics, outboxProperties);
        scheduler.setNanoClock(reloj::get);

        // Por defecto el reclamo sigue siendo de este relay: renovar devuelve el mismo record.
        lenient().when(outboxRelayService.renewClaim(any()))
                .thenAnswer(invocation -> Optional.of(invocation.<OutboxRecord>getArgument(0)));
    }

    /** Un lote y despues nada: lo que ve una pasada cuando ya no queda trabajo. */
    @SafeVarargs
    private void lotes(List<OutboxRecord>... lotes) {
        var stubbing = when(outboxRelayService.claimBatch());
        for (List<OutboxRecord> lote : lotes) {
            stubbing = stubbing.thenReturn(lote);
        }
        stubbing.thenReturn(List.of());
    }

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
                "mto.master-data.exchange", "mto.master-data.station.created", "{}", 1, 1L, null, null,
                20, Instant.parse("2026-01-01T00:05:00Z"));
    }

    @Test
    void marcaPublishedSoloDespuesDeQueElPublisherConfirme() {
        OutboxRecord record = record("MASTER_DATA_STATION_CREATED");
        lotes(List.of(record));

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
        lotes(List.of(record));
        OutboxPublishException fallo = new OutboxPublishException("nack");
        doThrow(fallo).when(outboxRabbitPublisher).publish(record);

        scheduler.publishPendingMessages();

        verify(outboxRelayService).markFailed(record, fallo);
        verify(outboxRelayService, never()).markPublished(any());
        verify(outboxMetrics).recordPublishFailure();
        verify(outboxMetrics, never()).recordPublished();
    }

    @Test
    void unMensajeQueFallaNoImpideProcesarElRestoDelLote() {
        OutboxRecord primero = record("MASTER_DATA_STATION_CREATED");
        OutboxRecord segundo = record("MASTER_DATA_TRACK_UPDATED");
        OutboxRecord tercero = record("MASTER_DATA_PROFILE_DELETED");
        lotes(List.of(primero, segundo, tercero));
        failFor(segundo);

        scheduler.publishPendingMessages();

        verify(outboxRelayService).markPublished(primero.id());
        verify(outboxRelayService).markFailed(eq(segundo), any());
        verify(outboxRelayService).markPublished(tercero.id());
    }

    @Test
    void siNoSePuedeRegistrarElFalloElLoteContinua() {
        OutboxRecord primero = record("MASTER_DATA_STATION_CREATED");
        OutboxRecord segundo = record("MASTER_DATA_TRACK_UPDATED");
        lotes(List.of(primero, segundo));
        failFor(primero);
        doThrow(new IllegalStateException("base de datos caida"))
                .when(outboxRelayService).markFailed(eq(primero), any());

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

    // ------------------------------------------------------------ reclamo vigente

    @Test
    void elReclamoSeRenuevaAntesDeCadaPublicacionYSePublicaElRecordRenovado() {
        OutboxRecord reclamado = record("MASTER_DATA_STATION_CREATED");
        OutboxRecord renovado = reclamado.renewedUntil(Instant.parse("2026-01-01T00:10:00Z"));
        lotes(List.of(reclamado));
        when(outboxRelayService.renewClaim(reclamado)).thenReturn(Optional.of(renovado));
        OutboxPublishException fallo = new OutboxPublishException("nack");
        doThrow(fallo).when(outboxRabbitPublisher).publish(renovado);

        scheduler.publishPendingMessages();

        InOrder orden = inOrder(outboxRelayService, outboxRabbitPublisher);
        orden.verify(outboxRelayService).renewClaim(reclamado);
        orden.verify(outboxRabbitPublisher).publish(renovado);
        // El cierre lleva el testigo renovado: con el viejo la escritura no casaria.
        orden.verify(outboxRelayService).markFailed(renovado, fallo);
    }

    @Test
    void unMensajeCuyoReclamoSeHaPerdidoNoSePublicaNiSeCierra() {
        OutboxRecord perdido = record("MASTER_DATA_STATION_CREATED");
        OutboxRecord vigente = record("MASTER_DATA_TRACK_UPDATED");
        lotes(List.of(perdido, vigente));
        when(outboxRelayService.renewClaim(perdido)).thenReturn(Optional.empty());

        scheduler.publishPendingMessages();

        verify(outboxRabbitPublisher, never()).publish(perdido);
        verify(outboxRelayService, never()).markPublished(perdido.id());
        verify(outboxRelayService, never()).markFailed(eq(perdido), any());
        verify(outboxMetrics).recordClaimLost();
        verify(outboxRelayService).markPublished(vigente.id());
    }

    @Test
    void siNoSePuedeComprobarElReclamoNoSePublica() {
        OutboxRecord record = record("MASTER_DATA_STATION_CREATED");
        lotes(List.of(record));
        when(outboxRelayService.renewClaim(record)).thenThrow(new IllegalStateException("base de datos caida"));

        scheduler.publishPendingMessages();

        verifyNoInteractions(outboxRabbitPublisher);
    }

    // ------------------------------------------------------- vaciado y su plazo

    @Test
    void laPasadaSigueReclamandoLotesMientrasHayaTrabajo() {
        OutboxRecord primero = record("MASTER_DATA_STATION_CREATED");
        OutboxRecord segundo = record("MASTER_DATA_STATION_UPDATED");
        OutboxRecord tercero = record("MASTER_DATA_STATION_DELETED");
        lotes(List.of(primero), List.of(segundo), List.of(tercero));

        scheduler.publishPendingMessages();

        InOrder orden = inOrder(outboxRabbitPublisher);
        orden.verify(outboxRabbitPublisher).publish(primero);
        orden.verify(outboxRabbitPublisher).publish(segundo);
        orden.verify(outboxRabbitPublisher).publish(tercero);
        verify(outboxRelayService, times(4)).claimBatch();
    }

    @Test
    void unLoteDelQueNoSaleNadaCortaLaPasada() {
        // El broker no confirma: seguir reclamando solo quemaria los intentos del resto.
        OutboxRecord record = record("MASTER_DATA_STATION_CREATED");
        lotes(List.of(record), List.of(record("MASTER_DATA_TRACK_UPDATED")));
        doThrow(new OutboxPublishException("sin confirmacion")).when(outboxRabbitPublisher).publish(any());

        scheduler.publishPendingMessages();

        verify(outboxRelayService, times(1)).claimBatch();
        verify(outboxRabbitPublisher, times(1)).publish(any());
    }

    @Test
    void alCumplirseElPlazoLaPasadaSueltaLoQueNoHaEmpezadoYNoReclamaMas() {
        outboxProperties.setPassTimeBudget(Duration.ofSeconds(30));
        OutboxRecord primero = record("MASTER_DATA_STATION_CREATED");
        OutboxRecord segundo = record("MASTER_DATA_TRACK_UPDATED");
        OutboxRecord tercero = record("MASTER_DATA_PROFILE_DELETED");
        lotes(List.of(primero, segundo, tercero), List.of(record("MASTER_DATA_STATION_UPDATED")));
        // Publicar el primero se come el plazo entero.
        doAnswer(invocation -> {
            reloj.addAndGet(Duration.ofSeconds(31).toNanos());
            return null;
        }).when(outboxRabbitPublisher).publish(primero);

        scheduler.publishPendingMessages();

        verify(outboxRelayService).markPublished(primero.id());
        verify(outboxRabbitPublisher, never()).publish(segundo);
        verify(outboxRabbitPublisher, never()).publish(tercero);
        verify(outboxRelayService).releaseClaim(segundo);
        verify(outboxRelayService).releaseClaim(tercero);
        verify(outboxRelayService, times(1)).claimBatch();
    }

    @Test
    void siSoltarUnMensajeFallaSeSueltanLosDemas() {
        outboxProperties.setPassTimeBudget(Duration.ofSeconds(30));
        OutboxRecord primero = record("MASTER_DATA_STATION_CREATED");
        OutboxRecord segundo = record("MASTER_DATA_TRACK_UPDATED");
        OutboxRecord tercero = record("MASTER_DATA_PROFILE_DELETED");
        lotes(List.of(primero, segundo, tercero));
        doAnswer(invocation -> {
            reloj.addAndGet(Duration.ofSeconds(31).toNanos());
            return null;
        }).when(outboxRabbitPublisher).publish(primero);
        doThrow(new IllegalStateException("base de datos caida")).when(outboxRelayService).releaseClaim(segundo);

        scheduler.publishPendingMessages();

        verify(outboxRelayService).releaseClaim(tercero);
    }
}
