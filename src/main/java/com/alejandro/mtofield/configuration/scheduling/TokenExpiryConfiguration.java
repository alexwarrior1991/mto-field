package com.alejandro.mtofield.configuration.scheduling;

import com.alejandro.mtofield.infrastructure.grpc.stream.TokenExpirySweeper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * El barrido de tokens caducados, cada {@code app.field.token-expiry.sweep}, con
 * {@code app.field.token-expiry.enabled} (encendido por defecto). El {@code @EnableScheduling}
 * esta en {@link FieldSchedulingConfiguration}.
 */
@Configuration
@ConditionalOnProperty(prefix = "app.field.token-expiry", name = "enabled", havingValue = "true", matchIfMissing = true)
public class TokenExpiryConfiguration {

    private final TokenExpirySweeper sweeper;

    public TokenExpiryConfiguration(TokenExpirySweeper sweeper) {
        this.sweeper = sweeper;
    }

    @Scheduled(fixedDelayString = "${app.field.token-expiry.sweep:30s}", initialDelayString = "${app.field.token-expiry.sweep:30s}")
    public void closeExpiredStreams() {
        sweeper.sweep();
    }
}
