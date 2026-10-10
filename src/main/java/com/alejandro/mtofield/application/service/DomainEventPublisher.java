package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.application.dto.messaging.DomainEvent;

import java.util.UUID;

/**
 * La unica puerta por la que un servicio cuenta lo que acaba de hacer (una posesion abierta, un
 * desalojo emitido, un equipo que acusa, la via libre...) a quien escuche fuera: hoy
 * {@code mto-notification}.
 *
 * <p>Se llama <b>dentro</b> de la transaccion de negocio: el evento se escribe en el outbox junto con
 * el cambio que cuenta, y se publica despues, o no se publica nada si la transaccion no confirma.
 * Con {@code app.rabbitmq.enabled=false} la implementacion no escribe nada (es lo que usan los
 * tests y un entorno sin broker).</p>
 *
 * <p>Que evento cuenta cada escritura esta en {@code docs/06-messaging.md}, y los nombres y los
 * valores de cada uno los construye {@code FieldEvents}.</p>
 */
public interface DomainEventPublisher {

    /** Publica el evento con un identificador de operacion nuevo. */
    void publish(DomainEvent event);

    /**
     * Publica el evento con un identificador de operacion dado. Es la clave de idempotencia del
     * inbox de los consumidores: un trabajo que se repite (dos instancias, una segunda pasada) publica
     * el mismo identificador y el consumidor lo descarta como duplicado.
     */
    void publish(UUID operationId, DomainEvent event);

    /** {@code false} cuando nada de lo publicado sale de este proceso ({@code app.rabbitmq.enabled=false}). */
    boolean isEnabled();
}
