package com.alejandro.mtofield.infrastructure.messaging.outbox;

/**
 * Cose la traza de la operacion de negocio con la publicacion del evento.
 * <p>
 * El outbox parte en dos la traza por construccion: el mensaje se guarda dentro de la
 * peticion HTTP y se publica segundos o minutos despues, desde el hilo del scheduler.
 * Sin nada que los una, el span de publicacion cuelga del scheduler y no de la
 * operacion que lo origino, y se pierde justo la trazabilidad que el {@code operationId}
 * intenta reconstruir a mano.
 * <p>
 * La solucion es guardar el contexto W3C en la fila del outbox al escribirla y
 * recuperarlo al publicar.
 */
public interface OutboxTracing {

    /** Contexto activo ahora mismo, o {@link OutboxTraceContext#EMPTY} si no hay traza. */
    OutboxTraceContext capture();

    /**
     * Abre un ambito de publicacion enganchado al contexto guardado en el mensaje.
     * Devuelve siempre algo cerrable, aunque no haya contexto que recuperar.
     */
    Scope startPublishScope(OutboxRecord record);

    /** Ambito cerrable sin excepcion comprobada, para poder usarlo en try-with-resources. */
    interface Scope extends AutoCloseable {

        Scope NOOP = () -> { };

        @Override
        void close();
    }
}
