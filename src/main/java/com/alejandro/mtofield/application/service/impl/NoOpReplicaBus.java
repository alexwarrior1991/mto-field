package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.replicas.ReplicaMessage;
import com.alejandro.mtofield.application.service.ReplicaBus;

/**
 * El bus con {@code app.rabbitmq.enabled=false}: una sola replica, o varias que se enteran de
 * todo por el tic de puesta al dia (mas despacio, nunca mal). Lo usan los tests y el smoke del CI.
 */
public class NoOpReplicaBus implements ReplicaBus {

    @Override
    public void publish(ReplicaMessage message) {
        // Nadie escucha: la base es la verdad y el tic la relee.
    }
}
