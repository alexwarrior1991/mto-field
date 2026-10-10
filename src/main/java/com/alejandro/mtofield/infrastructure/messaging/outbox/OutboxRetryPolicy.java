package com.alejandro.mtofield.infrastructure.messaging.outbox;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Calcula cuando se reintenta un mensaje del outbox que no ha podido publicarse.
 * <p>
 * Backoff EXPONENCIAL con tope y jitter. El backoff lineal anterior
 * ({@code initialRetryDelay * intentos}) agotaba los 10 intentos en 225 segundos:
 * cualquier mantenimiento o failover de RabbitMQ de mas de cuatro minutos dejaba
 * los eventos en FAILED, que es un estado terminal. Con 5s iniciales, tope de 5
 * minutos y 20 intentos la ventana pasa a ser de mas de una hora.
 * <p>
 * Se separa del scheduler para poder probar la progresion sin broker ni base de datos.
 */
public class OutboxRetryPolicy {

    private final OutboxProperties properties;

    /** Aleatoriedad inyectable: los tests necesitan un jitter determinista. */
    private final DoubleSupplier randomSupplier;

    public OutboxRetryPolicy(OutboxProperties properties) {
        this(properties, () -> ThreadLocalRandom.current().nextDouble());
    }

    OutboxRetryPolicy(OutboxProperties properties, DoubleSupplier randomSupplier) {
        this.properties = properties;
        this.randomSupplier = randomSupplier;
    }

    /**
     * @param attempts intentos ya consumidos por el mensaje (>= 1)
     * @return retardo hasta el siguiente intento, con jitter aplicado
     */
    public Duration nextDelay(int attempts) {
        Duration base = baseDelay(attempts);

        double jitter = clampJitter(properties.getRetryJitter());
        if (jitter == 0d) {
            return base;
        }

        // randomSupplier devuelve [0,1) -> factor en [1-jitter, 1+jitter)
        double factor = 1d + ((randomSupplier.getAsDouble() * 2d) - 1d) * jitter;
        long millis = Math.round(base.toMillis() * factor);

        return Duration.ofMillis(Math.max(1L, millis));
    }

    /** Retardo sin jitter: initialRetryDelay * 2^(attempts-1), acotado a maxRetryDelay. */
    public Duration baseDelay(int attempts) {
        long max = Math.max(1L, properties.getMaxRetryDelay().toMillis());
        long delay = Math.max(1L, properties.getInitialRetryDelay().toMillis());

        // El bucle no puede desbocarse: para en cuanto alcanza el tope, asi que
        // tampoco hay desbordamiento aunque lleguen intentos absurdamente altos.
        for (int i = 1; i < Math.max(1, attempts) && delay < max; i++) {
            delay = Math.min(delay * 2L, max);
        }

        return Duration.ofMillis(Math.min(delay, max));
    }

    public boolean isExhausted(int attempts, int maxAttempts) {
        return attempts >= maxAttempts;
    }

    private double clampJitter(double jitter) {
        if (jitter <= 0d) {
            return 0d;
        }
        return Math.min(jitter, 1d);
    }
}
