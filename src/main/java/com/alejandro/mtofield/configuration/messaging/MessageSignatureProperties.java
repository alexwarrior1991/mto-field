package com.alejandro.mtofield.configuration.messaging;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Shared secret and strictness of the message signature check.
 *
 * <p>El prefijo es el mismo que usa {@code mto-configuration} a propósito: el secreto tiene que ser
 * <b>el mismo valor</b> en los dos servicios, y que la property se llame igual a los dos lados evita
 * que alguien lo reparta en uno y no en el otro.</p>
 */
@Validated
@ConfigurationProperties(prefix = "app.messaging.signature")
public record MessageSignatureProperties(
        String secret,
        @NotNull MessageSignatureMode mode
) {

    public MessageSignatureProperties {
        mode = mode == null ? MessageSignatureMode.OPTIONAL : mode;
    }

    public boolean hasSecret() {
        return secret != null && !secret.isBlank();
    }

    /**
     * Un record imprime todos sus campos, y este lleva un secreto compartido: basta con que alguien
     * registre las properties, o con que aparezcan dentro del mensaje de una excepción, para que
     * acabe en un fichero de log o en un sistema de trazas. Se enmascara aquí y no en cada sitio que
     * pudiera imprimirlo, porque esa lista no se puede cerrar.
     */
    @Override
    public String toString() {
        return "MessageSignatureProperties[mode=" + mode + ", secret=" + (hasSecret() ? "***" : "<empty>") + "]";
    }
}
