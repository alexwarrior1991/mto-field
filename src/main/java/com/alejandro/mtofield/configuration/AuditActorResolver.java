package com.alejandro.mtofield.configuration;

import com.alejandro.mtofield.configuration.security.CurrentUserService;

import java.util.Optional;

/**
 * Resuelve quien esta detras de una escritura de JPA.
 *
 * <p>El autor sale del usuario autenticado, no de una cabecera. Dentro de una llamada gRPC el
 * interceptor de seguridad deja puesto el {@code SecurityContext} alrededor de cada callback, asi
 * que una escritura hecha desde el callback lleva el nombre de la persona. En los hilos propios del
 * servicio (las colas de trabajo por dispositivo, el despacho tras el commit) no hay contexto a
 * proposito y la fila se registra como {@code system}; quien hizo que, en esas tablas, va en la
 * propia fila ({@code reported_by}, {@code acked_by}, {@code issued_by}).</p>
 *
 * <p>Es estatico y no un bean para no depender del orden de arranque, como en los hermanos.</p>
 */
public final class AuditActorResolver {

    /** Escritura sin usuario: procesos internos de la propia aplicacion. */
    public static final String SYSTEM_ACTOR = "system";

    private static final CurrentUserService CURRENT_USER = new CurrentUserService();

    private AuditActorResolver() {
    }

    /** El nombre de usuario autenticado, o {@link #SYSTEM_ACTOR}. Nunca {@code null}. */
    public static String currentActor() {
        return CURRENT_USER.getUsername().orElse(SYSTEM_ACTOR);
    }

    /** El {@code sub} del token, que no cambia si alguien se renombra en Keycloak. */
    public static Optional<String> currentUserId() {
        return CURRENT_USER.getUserId();
    }
}
