package com.alejandro.mtofield.infrastructure.messaging.outbox;

import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * El {@code correlationId} bajo el que viaja lo que se publica desde el hilo actual.
 *
 * <p>En gRPC no hay cabecera {@code X-Correlation-Id} ni MDC que cruce las llamadas de una noche,
 * asi que la correlacion la fija quien escribe: el codigo de la posesion ({@code PO-000012}), que
 * agrupa en {@code mto-notification} todo lo que paso en esa noche, como los trabajos de
 * {@code mto-configuration} viajan bajo su {@code jobId}. Se fija alrededor del gancho que publica
 * y se limpia siempre: un hilo virtual no se reutiliza, pero el del planificador si.</p>
 */
public final class MessagingCorrelation {

    /** Lo que cabe en lo que los consumidores guardan: imprimible y acotado. */
    static final Pattern VALID_CORRELATION_ID = Pattern.compile("[\\x21-\\x7E]{1,200}");

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private MessagingCorrelation() {
    }

    /** El de este hilo, o nulo. */
    public static String current() {
        return CURRENT.get();
    }

    /** Corre la tarea con esa correlacion y restaura la anterior despues, pase lo que pase. */
    public static <T> T with(String correlationId, Supplier<T> task) {
        String previous = CURRENT.get();
        set(correlationId);
        try {
            return task.get();
        } finally {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }

    public static void with(String correlationId, Runnable task) {
        with(correlationId, () -> {
            task.run();
            return null;
        });
    }

    private static void set(String correlationId) {
        if (correlationId == null || !VALID_CORRELATION_ID.matcher(correlationId).matches()) {
            CURRENT.remove();
        } else {
            CURRENT.set(correlationId);
        }
    }
}
