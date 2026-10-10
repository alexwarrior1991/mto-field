package com.alejandro.mtofield.infrastructure.messaging.outbox;

/**
 * Se ha escrito un mensaje en el outbox dentro de la transaccion en curso.
 * <p>
 * No lleva datos: quien lo escucha no necesita saber cual es el mensaje, solo que hay
 * trabajo pendiente. El relay ya sabe encontrarlo.
 */
public record OutboxMessageSavedEvent() {
}
