package com.alejandro.mtofield.application.dto.messaging;

/**
 * Quien hizo lo que cuenta un mensaje, tal como lo clasifica este servicio al escribirlo.
 *
 * <p>Lo clasifica el emisor y no el consumidor porque solo el emisor tiene el token delante: el
 * consumidor recibe un nombre de usuario y no puede saber si detras habia una persona o la cuenta
 * de servicio de otro servicio del dominio.</p>
 */
public enum MessageActorKind {

    /** Una persona con sesion: el {@code preferred_username} de su token. */
    PERSON,

    /**
     * La cuenta de servicio de otro servicio del dominio, que en Keycloak se llama
     * {@code service-account-<cliente>}.
     */
    SERVICE,

    /** Nadie autenticado: un proceso de fondo de este servicio, o un evento de datos maestros. */
    SYSTEM
}
