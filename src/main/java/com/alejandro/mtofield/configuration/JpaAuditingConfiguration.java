package com.alejandro.mtofield.configuration;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;

import java.util.Optional;

/**
 * Configures Spring Data JPA auditing for persistence entities.
 *
 * <p>Estas columnas ({@code created_by} / {@code updated_by} y sus fechas) guardan quien toco la
 * fila la ultima vez. No hay Envers: las tablas de ordenes, acuses y eventos son de solo insercion
 * y ya son la historia (ver {@code docs/07-auditing.md}). Las inserciones nativas de esas tablas
 * rellenan las columnas con quien reporto o acuso; estas de aqui valen para lo que escribe JPA (la
 * posesion y sus turnos).</p>
 *
 * <p>Quien escribe lo decide {@link AuditActorResolver}: el usuario del token dentro de una
 * llamada gRPC o HTTP, {@code system} en los hilos propios del servicio.</p>
 */
@Configuration
@EnableJpaAuditing(auditorAwareRef = "auditorAware")
@ConditionalOnProperty(name = "spring.data.jpa.auditing.enabled", havingValue = "true", matchIfMissing = true)
public class JpaAuditingConfiguration {

    @Bean
    public AuditorAware<String> auditorAware() {
        return () -> Optional.of(AuditActorResolver.currentActor());
    }
}
