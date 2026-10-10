package com.alejandro.mtofield.support;

import org.junit.jupiter.api.Assumptions;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.rabbitmq.RabbitMQContainer;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * El RabbitMQ real de los tests que lo necesitan ({@code RabbitReplicaBusIT}): con Docker, el
 * contenedor oficial; sin Docker, {@code TEST_RABBITMQ_URI} ({@code amqp://usuario:clave@host:puerto/vhost})
 * apunta a un broker a mano, como {@code TEST_DATABASE_URL} con PostgreSQL. Sin ninguna de las dos
 * cosas el test se omite, no falla.
 */
public final class RabbitMqTestBroker {

    private static final String EXTERNAL_URI = System.getenv("TEST_RABBITMQ_URI");
    private static final String IMAGE = "rabbitmq:4-management-alpine";

    private static RabbitMQContainer container;

    private RabbitMqTestBroker() {
    }

    /** Las propiedades {@code spring.rabbitmq.*} del broker, para un contexto de test. */
    public static synchronized Map<String, Object> properties() {
        Map<String, Object> properties = new LinkedHashMap<>();
        if (EXTERNAL_URI != null && !EXTERNAL_URI.isBlank()) {
            URI uri = URI.create(EXTERNAL_URI);
            String[] userInfo = uri.getUserInfo() == null ? new String[] {"guest", "guest"} : uri.getUserInfo().split(":", 2);
            properties.put("spring.rabbitmq.host", uri.getHost());
            properties.put("spring.rabbitmq.port", uri.getPort() == -1 ? 5672 : uri.getPort());
            properties.put("spring.rabbitmq.username", userInfo[0]);
            properties.put("spring.rabbitmq.password", userInfo.length > 1 ? userInfo[1] : "");
            properties.put("spring.rabbitmq.virtual-host", uri.getPath() == null || uri.getPath().length() <= 1 ? "/" : uri.getPath().substring(1));
            return properties;
        }
        Assumptions.assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                "Neither Docker nor TEST_RABBITMQ_URI is available: skipping the RabbitMQ-backed test");
        if (container == null) {
            container = new RabbitMQContainer(IMAGE);
            container.start();
        }
        properties.put("spring.rabbitmq.host", container.getHost());
        properties.put("spring.rabbitmq.port", container.getAmqpPort());
        properties.put("spring.rabbitmq.username", container.getAdminUsername());
        properties.put("spring.rabbitmq.password", container.getAdminPassword());
        properties.put("spring.rabbitmq.virtual-host", "/");
        return properties;
    }
}
