package com.alejandro.mtofield.infrastructure.messaging.outbox;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El backoff anterior era LINEAL pese a que la documentacion decia lo contrario:
 * 5s, 10s, 15s... 45s. Diez intentos se agotaban en 225 segundos, de modo que un
 * mantenimiento de RabbitMQ de cuatro minutos mandaba todos los eventos a FAILED,
 * que era un estado terminal. Estos tests fijan la progresion real.
 */
class OutboxRetryPolicyTest {

    private OutboxProperties properties(Duration initial, Duration max, double jitter) {
        OutboxProperties properties = new OutboxProperties();
        properties.setInitialRetryDelay(initial);
        properties.setMaxRetryDelay(max);
        properties.setRetryJitter(jitter);
        return properties;
    }

    private OutboxRetryPolicy policy(Duration initial, Duration max) {
        return new OutboxRetryPolicy(properties(initial, max, 0d));
    }

    @ParameterizedTest(name = "intento {0} -> {1}s")
    @CsvSource({
            "1, 5",
            "2, 10",
            "3, 20",
            "4, 40",
            "5, 80",
            "6, 160",
            "7, 300",
            "8, 300"
    })
    void elRetardoSeDuplicaEnCadaIntentoHastaElTope(int attempts, long expectedSeconds) {
        OutboxRetryPolicy retryPolicy = policy(Duration.ofSeconds(5), Duration.ofMinutes(5));

        assertThat(retryPolicy.baseDelay(attempts)).isEqualTo(Duration.ofSeconds(expectedSeconds));
    }

    @Test
    void veinteIntentosCubrenMasDeUnaHoraDeBrokerCaido() {
        OutboxRetryPolicy retryPolicy = policy(Duration.ofSeconds(5), Duration.ofMinutes(5));

        Duration ventana = Duration.ZERO;
        for (int attempts = 1; attempts <= 20; attempts++) {
            ventana = ventana.plus(retryPolicy.baseDelay(attempts));
        }

        assertThat(ventana)
                .as("con el backoff lineal anterior la ventana era de 225 segundos")
                .isGreaterThan(Duration.ofHours(1));
    }

    @Test
    void nuncaSuperaElTopeAunqueLosIntentosSeanAbsurdos() {
        OutboxRetryPolicy retryPolicy = policy(Duration.ofSeconds(5), Duration.ofMinutes(5));

        assertThat(retryPolicy.baseDelay(Integer.MAX_VALUE))
                .as("un contador desbocado no debe desbordar el calculo")
                .isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void elPrimerIntentoUsaElRetardoInicial() {
        OutboxRetryPolicy retryPolicy = policy(Duration.ofSeconds(5), Duration.ofMinutes(5));

        assertThat(retryPolicy.baseDelay(0)).isEqualTo(Duration.ofSeconds(5));
        assertThat(retryPolicy.baseDelay(1)).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void elJitterMueveElRetardoDentroDeLaBandaConfigurada() {
        OutboxProperties properties = properties(Duration.ofSeconds(10), Duration.ofMinutes(5), 0.2d);

        // random = 0 -> extremo inferior; random = 1 -> extremo superior
        assertThat(new OutboxRetryPolicy(properties, () -> 0d).nextDelay(1))
                .isEqualTo(Duration.ofSeconds(8));
        assertThat(new OutboxRetryPolicy(properties, () -> 1d).nextDelay(1))
                .isEqualTo(Duration.ofSeconds(12));
        assertThat(new OutboxRetryPolicy(properties, () -> 0.5d).nextDelay(1))
                .isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void sinJitterElRetardoEsExactamenteElCalculado() {
        OutboxProperties properties = properties(Duration.ofSeconds(10), Duration.ofMinutes(5), 0d);

        assertThat(new OutboxRetryPolicy(properties, () -> 0d).nextDelay(3))
                .isEqualTo(Duration.ofSeconds(40));
    }

    @Test
    void elJitterNuncaProduceUnRetardoNoPositivo() {
        OutboxProperties properties = properties(Duration.ofMillis(1), Duration.ofMinutes(5), 1d);

        assertThat(new OutboxRetryPolicy(properties, () -> 0d).nextDelay(1))
                .isGreaterThanOrEqualTo(Duration.ofMillis(1));
    }

    @Test
    void seAgotaAlAlcanzarElMaximoDeIntentos() {
        OutboxRetryPolicy retryPolicy = policy(Duration.ofSeconds(5), Duration.ofMinutes(5));

        assertThat(retryPolicy.isExhausted(19, 20)).isFalse();
        assertThat(retryPolicy.isExhausted(20, 20)).isTrue();
        assertThat(retryPolicy.isExhausted(21, 20)).isTrue();
    }
}
