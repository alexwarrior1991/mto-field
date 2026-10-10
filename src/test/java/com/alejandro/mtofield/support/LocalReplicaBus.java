package com.alejandro.mtofield.support;

import com.alejandro.mtofield.application.replicas.ReplicaId;
import com.alejandro.mtofield.application.replicas.ReplicaMessage;
import com.alejandro.mtofield.application.service.ReplicaBus;
import com.alejandro.mtofield.application.service.ReplicaMessageHandler;
import org.springframework.beans.factory.ObjectProvider;

import java.util.List;

/** El bus de un contexto sobre {@link LocalReplicaHub}; los manejadores se resuelven al entregar, para no cerrar un ciclo de beans. */
public final class LocalReplicaBus implements ReplicaBus {

    private final ReplicaId replicaId;
    private final ObjectProvider<ReplicaMessageHandler> handlers;

    public LocalReplicaBus(ReplicaId replicaId, ObjectProvider<ReplicaMessageHandler> handlers) {
        this.replicaId = replicaId;
        this.handlers = handlers;
        LocalReplicaHub.join(this);
    }

    String replicaId() {
        return replicaId.value();
    }

    @Override
    public void publish(ReplicaMessage message) {
        LocalReplicaHub.broadcast(replicaId.value(), message);
    }

    void deliver(String from, ReplicaMessage message) {
        List<ReplicaMessageHandler> targets = handlers.stream().toList();
        for (ReplicaMessageHandler handler : targets) {
            handler.onReplicaMessage(from, message);
        }
    }

    public void leave() {
        LocalReplicaHub.leave(this);
    }
}
