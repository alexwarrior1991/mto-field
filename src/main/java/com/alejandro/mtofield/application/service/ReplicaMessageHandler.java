package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.application.replicas.ReplicaMessage;

/** Quien quiere enterarse de lo que cuentan las demas replicas. Los mensajes propios no llegan. */
public interface ReplicaMessageHandler {

    void onReplicaMessage(String fromReplica, ReplicaMessage message);
}
