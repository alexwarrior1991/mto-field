package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.configuration.metrics.FieldMetrics;
import com.alejandro.mtofield.infrastructure.grpc.GrpcErrors;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;

/**
 * Cierra con {@code UNAUTHENTICATED TOKEN_EXPIRED} los streams cuyo token ha caducado. El JWT solo
 * se valida al abrir la llamada, y un {@code TeamChannel} o un tablero duran toda la noche: sin
 * esto, un token revocado o caducado seguiria valiendo mientras el stream viviera. Quien abre el
 * stream captura la caducidad ({@code CurrentUserService.getTokenExpiresAt()}); aqui solo se
 * compara con el reloj. El dispositivo o la consola reanudan con un token nuevo y
 * {@code Join.last_command_sequence}: no se pierde nada.
 */
@Component
public class TokenExpirySweeper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenExpirySweeper.class);

    public static final String REASON_TOKEN_EXPIRED = "TOKEN_EXPIRED";
    public static final String EXPIRED_AT = "expired_at";

    private final DeviceStreamRegistry streams;
    private final BoardWatcherRegistry watchers;
    private final Clock clock;
    private final FieldMetrics metrics;

    public TokenExpirySweeper(DeviceStreamRegistry streams, BoardWatcherRegistry watchers, Clock clock, FieldMetrics metrics) {
        this.streams = streams;
        this.watchers = watchers;
        this.clock = clock;
        this.metrics = metrics;
    }

    /** @return cuantos streams se cerraron en esta pasada */
    public int sweep() {
        Instant now = clock.instant();
        int closed = 0;
        for (DeviceStream stream : streams.all()) {
            Instant expiresAt = stream.tokenExpiresAt();
            if (expiresAt != null && !now.isBefore(expiresAt) && !stream.isClosed()) {
                LOGGER.info("Token of device {} expired at {}: closing its TeamChannel", stream.deviceId(), expiresAt);
                stream.fail(expired("device " + stream.deviceId(), expiresAt));
                metrics.recordExpiredStream("team");
                closed++;
            }
        }
        for (BoardWatcher watcher : watchers.all()) {
            Instant expiresAt = watcher.tokenExpiresAt();
            if (expiresAt != null && !now.isBefore(expiresAt) && !watcher.isClosed()) {
                LOGGER.info("Token of a board watcher expired at {}: closing it", expiresAt);
                watcher.fail(expired("the board watcher", expiresAt));
                metrics.recordExpiredStream("board");
                closed++;
            }
        }
        return closed;
    }

    private static io.grpc.StatusRuntimeException expired(String who, Instant expiresAt) {
        return GrpcErrors.of(Status.Code.UNAUTHENTICATED, REASON_TOKEN_EXPIRED, "the token of " + who + " expired at " + expiresAt
                + "; reconnect with a fresh token", Map.of(EXPIRED_AT, expiresAt.toString()));
    }
}
