package com.alejandro.mtofield.infrastructure.messaging.rabbitmq;

import java.util.Locale;

/**
 * Nombres del exchange propio de este servicio y de lo que se enruta por el.
 *
 * <p>Es un exchange distinto de {@code mto.field.replicas.exchange} a proposito: aquel es el bus
 * con el que las replicas se cuentan lo que no esta en la base (fanout, transitorio, de este
 * servicio consigo mismo), y lo que este servicio cuenta de si mismo a los demas (una posesion
 * abierta o cerrada, un desalojo y sus acuses, la salida de via) sale por aqui, por un outbox. Solo
 * se declara el exchange: la cola es de quien la consume ({@code mto-notification}), que la bindea
 * con {@code mto.field.#} o con lo que le interese.</p>
 *
 * <p>La clave de enrutado y el {@code eventType} son contrato: el consumidor deriva del
 * {@code DomainEvent} el tipo de actividad ({@code field.<entidad>.<evento>}), asi que el formato
 * no es cosmetico.</p>
 */
public final class FieldRabbitMqNames {

    public static final String FIELD_EXCHANGE = "mto.field.exchange";

    public static final String FIELD_ROUTING_PREFIX = "mto.field";
    public static final String FIELD_ROUTING_PATTERN = "mto.field.#";

    private static final String EVENT_TYPE_PREFIX = "FIELD";

    private FieldRabbitMqNames() {
    }

    /** {@code mto.field.<entidad>.<evento>}: lo que decide a que colas llega el mensaje. */
    public static String routingKey(String entityName, String eventName) {
        return FIELD_ROUTING_PREFIX + "." + normalize(entityName) + "." + normalize(eventName);
    }

    /** {@code FIELD_<ENTIDAD>_<EVENTO>}: el {@code eventType} del sobre y de la cabecera. */
    public static String eventType(String entityName, String eventName) {
        return EVENT_TYPE_PREFIX + "_" + constant(entityName) + "_" + constant(eventName);
    }

    private static String normalize(String value) {
        return value.trim()
                .toLowerCase(Locale.ROOT)
                .replace("_", "-")
                .replace(" ", "-");
    }

    private static String constant(String value) {
        return normalize(value)
                .toUpperCase(Locale.ROOT)
                .replace("-", "_");
    }
}
