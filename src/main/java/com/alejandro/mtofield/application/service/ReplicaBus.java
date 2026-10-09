package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.application.replicas.ReplicaMessage;

/**
 * La puerta hacia las demas replicas. Publicar nunca bloquea ni falla hacia quien llama: un bus
 * caido se registra y se cuenta, y el tic de puesta al dia hace el resto leyendo de la base.
 * Quien recibe es cada {@link ReplicaMessageHandler} del contexto.
 */
public interface ReplicaBus {

    void publish(ReplicaMessage message);
}
