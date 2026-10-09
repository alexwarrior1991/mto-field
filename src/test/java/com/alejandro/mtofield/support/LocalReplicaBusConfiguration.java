package com.alejandro.mtofield.support;

import com.alejandro.mtofield.application.replicas.ReplicaId;
import com.alejandro.mtofield.application.service.ReplicaBus;
import com.alejandro.mtofield.application.service.ReplicaMessageHandler;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/** Sustituye al bus del contexto por el del hub en memoria, para que dos contextos de una JVM sean dos replicas. */
@TestConfiguration(proxyBeanMethods = false)
public class LocalReplicaBusConfiguration {

    @Bean(destroyMethod = "leave")
    @Primary
    public LocalReplicaBus localReplicaBus(ReplicaId replicaId, ObjectProvider<ReplicaMessageHandler> handlers) {
        return new LocalReplicaBus(replicaId, handlers);
    }
}
