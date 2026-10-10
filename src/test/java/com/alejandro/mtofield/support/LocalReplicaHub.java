package com.alejandro.mtofield.support;

import com.alejandro.mtofield.application.replicas.ReplicaMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * El fanout en memoria de los tests: las replicas (contextos de Spring de la misma JVM) que se
 * apuntan reciben lo que publican las demas, en el hilo de quien publica. {@link #cut(boolean)}
 * tira los mensajes, como un broker caido, para ver al tic de puesta al dia hacer el resto.
 */
public final class LocalReplicaHub {

    private static final Map<String, LocalReplicaBus> MEMBERS = new ConcurrentHashMap<>();
    private static final AtomicLong DROPPED = new AtomicLong();
    private static volatile boolean cut;

    private LocalReplicaHub() {
    }

    static void join(LocalReplicaBus bus) {
        MEMBERS.put(bus.replicaId(), bus);
    }

    static void leave(LocalReplicaBus bus) {
        MEMBERS.remove(bus.replicaId(), bus);
    }

    static void broadcast(String from, ReplicaMessage message) {
        if (cut) {
            DROPPED.incrementAndGet();
            return;
        }
        for (LocalReplicaBus member : MEMBERS.values()) {
            if (!member.replicaId().equals(from)) {
                member.deliver(from, message);
            }
        }
    }

    public static void cut(boolean isCut) {
        cut = isCut;
    }

    public static long dropped() {
        return DROPPED.get();
    }

    public static int members() {
        return MEMBERS.size();
    }
}
