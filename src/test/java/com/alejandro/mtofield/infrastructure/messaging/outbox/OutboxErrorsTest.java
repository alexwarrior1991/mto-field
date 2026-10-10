package com.alejandro.mtofield.infrastructure.messaging.outbox;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * last_error tiene 1000 caracteres y antes recibia exception.getMessage() en crudo.
 * Un mensaje de AMQP encadenado los pasa sin esfuerzo, y entonces el flush rompia la
 * transaccion ENTERA del relay: se perdian tambien los mensajes ya publicados en esa
 * tanda y el fallo se repetia en cada pasada.
 */
class OutboxErrorsTest {

    @Test
    void recortaLosMensajesQueNoCabenEnLaColumna() {
        String largo = "x".repeat(5_000);

        String descripcion = OutboxErrors.describe(new IllegalStateException(largo));

        assertThat(descripcion).hasSizeLessThanOrEqualTo(OutboxErrors.MAX_LAST_ERROR_LENGTH);
        assertThat(descripcion).endsWith("...[truncado]");
    }

    @Test
    void incluyeElTipoDeExcepcionParaPoderDiagnosticar() {
        String descripcion = OutboxErrors.describe(new IllegalStateException("conexion rechazada"));

        assertThat(descripcion).isEqualTo("IllegalStateException: conexion rechazada");
    }

    @Test
    void unaExcepcionSinMensajeSigueDejandoRastro() {
        assertThat(OutboxErrors.describe(new NullPointerException())).isEqualTo("NullPointerException");
        assertThat(OutboxErrors.describe(new IllegalArgumentException("   "))).isEqualTo("IllegalArgumentException");
    }

    @Test
    void nuncaDevuelveNull() {
        assertThat(OutboxErrors.describe(null)).isNotNull();
    }

    @Test
    void dejaIntactoLoQueSiCabe() {
        String corto = "y".repeat(OutboxErrors.MAX_LAST_ERROR_LENGTH);

        assertThat(OutboxErrors.truncate(corto)).isEqualTo(corto);
        assertThat(OutboxErrors.truncate(null)).isNull();
    }
}
