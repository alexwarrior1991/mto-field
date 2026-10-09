package com.alejandro.mtofield.support;

import org.junit.jupiter.api.Assumptions;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Base de los tests que necesitan un PostgreSQL real con las migraciones de Flyway aplicadas.
 *
 * <p>Por defecto levanta un contenedor {@code postgres:17-alpine}, la misma version que corre en
 * {@code mto-platform}, para que CI y local no prueben contra motores distintos. Si no hay Docker
 * pero si un PostgreSQL a mano, {@code TEST_DATABASE_URL} (con {@code TEST_DATABASE_USERNAME} y
 * {@code TEST_DATABASE_PASSWORD}) apunta a el y el contenedor no se arranca: es lo que permite
 * ejecutar la suite en un entorno sin daemon de Docker. Sin Docker ni variable, el test se marca
 * como omitido en lugar de fallar por infraestructura.</p>
 *
 * <p>El contenedor es estatico y se arranca una sola vez por JVM: cada contexto de Spring aplica las
 * migraciones sobre la misma base, que Flyway deja como no-op a partir de la segunda vez.</p>
 */
public abstract class PostgreSQLTestContainer {

    private static final String EXTERNAL_URL = System.getenv("TEST_DATABASE_URL");

    private static PostgreSQLContainer container;

    protected static void registerPostgreSQLProperties(DynamicPropertyRegistry registry) {
        if (EXTERNAL_URL != null && !EXTERNAL_URL.isBlank()) {
            registry.add("spring.datasource.url", () -> EXTERNAL_URL);
            registry.add("spring.datasource.username", () -> envOrEmpty("TEST_DATABASE_USERNAME"));
            registry.add("spring.datasource.password", () -> envOrEmpty("TEST_DATABASE_PASSWORD"));
        } else {
            Assumptions.assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                    "Neither Docker nor TEST_DATABASE_URL is available: skipping the PostgreSQL-backed test");
            PostgreSQLContainer postgres = container();
            registry.add("spring.datasource.url", postgres::getJdbcUrl);
            registry.add("spring.datasource.username", postgres::getUsername);
            registry.add("spring.datasource.password", postgres::getPassword);
        }
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
        registry.add("spring.flyway.enabled", () -> "true");
    }

    private static synchronized PostgreSQLContainer container() {
        if (container == null) {
            container = new PostgreSQLContainer("postgres:17-alpine")
                    .withDatabaseName("mto_field_test")
                    .withUsername("mto_field")
                    .withPassword("mto_field");
            container.start();
        }
        return container;
    }

    private static String envOrEmpty(String name) {
        String value = System.getenv(name);
        return value == null ? "" : value;
    }
}
