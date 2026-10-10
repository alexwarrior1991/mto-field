package com.alejandro.mtofield.configuration.scheduling;

import com.alejandro.mtofield.infrastructure.grpc.stream.ReplicaRelay;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * El tic de puesta al dia entre replicas, cada {@code app.field.replicas.catch-up}: relee de la
 * base lo que el bus no haya traido. Corre siempre, tambien con el bus apagado y con una sola
 * replica (donde no encuentra nada que hacer). El {@code @EnableScheduling} esta en
 * {@link FieldSchedulingConfiguration}.
 */
@Configuration
public class ReplicaCatchUpConfiguration {

    private final ReplicaRelay relay;

    public ReplicaCatchUpConfiguration(ReplicaRelay relay) {
        this.relay = relay;
    }

    @Scheduled(fixedDelayString = "${app.field.replicas.catch-up:2s}", initialDelayString = "${app.field.replicas.catch-up:2s}")
    public void catchUp() {
        relay.catchUp();
    }
}
