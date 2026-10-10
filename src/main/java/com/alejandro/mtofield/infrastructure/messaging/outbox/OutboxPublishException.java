package com.alejandro.mtofield.infrastructure.messaging.outbox;

/**
 * El broker no ha confirmado la publicacion: nack, mensaje no enrutable, o sin
 * respuesta dentro del plazo.
 */
public class OutboxPublishException extends RuntimeException {

    public OutboxPublishException(String message) {
        super(message);
    }

    public OutboxPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
