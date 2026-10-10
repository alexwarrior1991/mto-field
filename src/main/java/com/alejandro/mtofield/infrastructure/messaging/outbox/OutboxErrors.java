package com.alejandro.mtofield.infrastructure.messaging.outbox;

/**
 * Convierte una excepcion en el texto que se guarda en {@code outbox_message.last_error}.
 * <p>
 * La columna tiene 1000 caracteres y antes se le asignaba {@code exception.getMessage()}
 * en crudo. Un mensaje de AMQP encadenado los supera con facilidad, y entonces el flush
 * de la transaccion revienta con un error de dato demasiado largo: se perdia la tanda
 * ENTERA, incluidos los mensajes que si se habian publicado, y el fallo se repetia en
 * cada pasada. Por eso el truncado no es cosmetico.
 */
public final class OutboxErrors {

    /** Debe coincidir con la longitud declarada en {@link OutboxMessage#getLastError()}. */
    public static final int MAX_LAST_ERROR_LENGTH = 1000;

    private static final String TRUNCATION_SUFFIX = "...[truncado]";

    private OutboxErrors() {
    }

    /**
     * @return "TipoDeExcepcion: mensaje", recortado a {@value #MAX_LAST_ERROR_LENGTH}
     * caracteres. Nunca devuelve null: una excepcion sin mensaje sigue siendo
     * informacion util para diagnosticar.
     */
    public static String describe(Throwable throwable) {
        if (throwable == null) {
            return "Error desconocido";
        }

        String type = throwable.getClass().getSimpleName();
        String message = throwable.getMessage();

        String description = (message == null || message.isBlank())
                ? type
                : type + ": " + message;

        return truncate(description);
    }

    public static String truncate(String value) {
        if (value == null || value.length() <= MAX_LAST_ERROR_LENGTH) {
            return value;
        }

        return value.substring(0, MAX_LAST_ERROR_LENGTH - TRUNCATION_SUFFIX.length()) + TRUNCATION_SUFFIX;
    }
}
