package com.alejandro.mtofield.configuration.replicas;

import com.alejandro.mtofield.application.replicas.ReplicaId;
import com.alejandro.mtofield.application.service.ReplicaBus;
import com.alejandro.mtofield.application.service.impl.NoOpReplicaBus;
import com.alejandro.mtofield.configuration.grpc.FieldProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Lo que toda replica tiene aunque el bus este apagado: su nombre y, sin broker, el bus NoOp. Con
 * {@code app.rabbitmq.enabled=true} el bus lo pone {@link ReplicasRabbitConfiguration}.
 */
@Configuration
public class ReplicasConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicasConfiguration.class);

    @Bean
    public ReplicaId replicaId(FieldProperties properties) {
        ReplicaId id = ReplicaId.of(properties.replicas().id());
        LOGGER.info("This replica is {}", id);
        return id;
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.rabbitmq", name = "enabled", havingValue = "false")
    public ReplicaBus noOpReplicaBus() {
        LOGGER.info("Replica bus off (app.rabbitmq.enabled=false): other replicas learn of this one only through the database");
        return new NoOpReplicaBus();
    }
}
