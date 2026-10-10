package com.alejandro.mtofield.infrastructure.messaging.outbox;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.Nullness;

import java.lang.reflect.Method;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxEndpointTest {

    @Mock
    private OutboxAdminService outboxAdminService;

    @InjectMocks
    private OutboxEndpoint outboxEndpoint;

    @Test
    void devuelveElEstadoDelOutbox() {
        OutboxStats stats = new OutboxStats(3, 1, 100, 2, Instant.parse("2026-08-24T10:00:00Z"));
        when(outboxAdminService.stats()).thenReturn(stats);

        assertThat(outboxEndpoint.stats()).isEqualTo(stats);
    }

    @Test
    void elRedriveSinLimiteUsaUnValorPorDefectoAcotado() {
        when(outboxAdminService.redriveFailed(100)).thenReturn(7);

        assertThat(outboxEndpoint.redrive(null)).containsEntry("redriven", 7);
        verify(outboxAdminService).redriveFailed(100);
    }

    @Test
    void elRedriveRespetaElLimiteIndicado() {
        when(outboxAdminService.redriveFailed(5)).thenReturn(5);

        assertThat(outboxEndpoint.redrive(5)).containsEntry("redriven", 5);
    }

    @Test
    void elParametroLimitEsOpcionalParaActuator() throws Exception {
        // Actuator decide si un parametro es obligatorio con Nullness.forParameter. Si
        // este parametro dejara de verse como nullable, un POST sin cuerpo pasaria a
        // responder 400 en lugar de reencolar con el limite por defecto.
        Method redrive = OutboxEndpoint.class.getMethod("redrive", Integer.class);

        assertThat(Nullness.forParameter(redrive.getParameters()[0])).isEqualTo(Nullness.NULLABLE);
    }
}
