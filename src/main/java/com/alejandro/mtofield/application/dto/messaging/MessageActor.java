package com.alejandro.mtofield.application.dto.messaging;

import java.util.Objects;

/**
 * El {@code actor} del sobre: quien hizo lo que el evento cuenta.
 *
 * <p>Viaja con el mensaje porque el consumidor no tiene otra forma de saberlo. El evento se escribe
 * en el outbox dentro de la transaccion de negocio, que es el unico momento en el que el token de
 * quien la pidio sigue a mano; segundos despues, cuando el relay publica, ese contexto ya no existe.</p>
 *
 * @param id       el {@code sub} del token; nulo sin usuario
 * @param username el {@code preferred_username}; nulo sin usuario
 * @param kind     como lo clasifico este servicio
 */
public record MessageActor(String id, String username, MessageActorKind kind) {

    /** Prefijo con el que Keycloak nombra al usuario de una cuenta de servicio. */
    public static final String SERVICE_ACCOUNT_PREFIX = "service-account-";

    public MessageActor {
        Objects.requireNonNull(kind, "kind is required");
    }

    /** Nadie autenticado: un proceso de fondo de este servicio, o un mensaje de datos maestros. */
    public static MessageActor system() {
        return new MessageActor(null, null, MessageActorKind.SYSTEM);
    }

    /**
     * Un usuario autenticado, clasificado por su nombre: una cuenta de servicio de Keycloak se
     * llama siempre {@code service-account-<cliente>}, y cualquier otro nombre es una persona.
     */
    public static MessageActor of(String id, String username) {
        boolean service = username != null && username.startsWith(SERVICE_ACCOUNT_PREFIX);

        return new MessageActor(id, username, service ? MessageActorKind.SERVICE : MessageActorKind.PERSON);
    }
}
