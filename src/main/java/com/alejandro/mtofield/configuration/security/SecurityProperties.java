package com.alejandro.mtofield.configuration.security;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * No existe un interruptor para apagar la seguridad: lo que cambia entre entornos son estas
 * properties, no la existencia de la cadena de filtros ni del interceptor gRPC. Un flag de ese tipo
 * tampoco desactivaria nada: con {@code spring-boot-starter-security} en el classpath, quedarse sin
 * {@code SecurityFilterChain} propio devuelve el control a la cadena por defecto de Boot.
 */
@Validated
@ConfigurationProperties(prefix = "app.security")
public record SecurityProperties(
        @NotBlank String clientId,
        @NotBlank String principalClaim,
        boolean audienceValidationEnabled,
        String requiredAudience
) {

    /**
     * Sin esta comprobacion, un {@code KEYCLOAK_AUDIENCE} vacio apagaba la validacion de audiencia
     * en tiempo de peticion y sin dejar rastro en el log: la API pasaba a aceptar cualquier token
     * emitido por el realm, incluido el del frontal de otra aplicacion. Mejor no arrancar.
     */
    @AssertTrue(message = "app.security.required-audience es obligatorio cuando "
            + "app.security.audience-validation-enabled es true")
    public boolean isAudienceConfigurationConsistent() {
        return !audienceValidationEnabled || (requiredAudience != null && !requiredAudience.isBlank());
    }
}
