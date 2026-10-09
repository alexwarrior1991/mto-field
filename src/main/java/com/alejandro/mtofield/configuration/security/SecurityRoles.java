package com.alejandro.mtofield.configuration.security;

/**
 * Roles de cliente de Keycloak que comprueba este servicio, ya normalizados a autoridad de Spring.
 *
 * <p>Son los nombres que declara {@code keycloak/mto-field-partial-import.json} en mayusculas y con
 * guion bajo: {@code field-team} llega como {@code ROLE_FIELD_TEAM}. Un rol que se anade aqui sin
 * anadirlo alli no lo tiene nadie y todo responde {@code PERMISSION_DENIED}.</p>
 *
 * <p>En gRPC no hay verbos HTTP: el permiso se comprueba RPC a RPC con {@code @PreAuthorize} en
 * {@code FieldGrpcService}.</p>
 */
public final class SecurityRoles {

    private SecurityRoles() {
    }

    /** Un dispositivo de un equipo en via: {@code TeamChannel} y {@code SyncBufferedEvents}. */
    public static final String FIELD_TEAM = "FIELD_TEAM";

    /**
     * El responsable del bloqueo: abrir y cerrar la posesion, emitir ordenes (desalojo, mensajes,
     * cambio de ventana) y ver el tablero.
     */
    public static final String FIELD_SUPERVISE = "FIELD_SUPERVISE";

    /** Lectura de los endpoints de Actuator. */
    public static final String OPS_METRICS = "OPS_METRICS";

    /** Operaciones de Actuator que modifican estado. */
    public static final String OPS_WRITE = "OPS_WRITE";
}
