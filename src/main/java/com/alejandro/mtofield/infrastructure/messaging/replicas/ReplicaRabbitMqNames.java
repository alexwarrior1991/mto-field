package com.alejandro.mtofield.infrastructure.messaging.replicas;

/** Los nombres fijos del bus de replicas; el exchange se puede cambiar por configuracion. */
public final class ReplicaRabbitMqNames {

    /** Prefijo de la cola de cada replica: {@code mto.field.replicas.<id>}, exclusiva y auto-delete. */
    public static final String QUEUE_PREFIX = "mto.field.replicas.";
    /** Cabecera con la replica que publica; el cuerpo la repite, la cabecera permite filtrar sin leerlo. */
    public static final String HEADER_REPLICA = "x-mto-replica";
    public static final String CONTENT_TYPE_JSON = "application/json";

    private ReplicaRabbitMqNames() {
    }

    public static String queueOf(String replicaId) {
        return QUEUE_PREFIX + replicaId;
    }
}
