package com.alejandro.mtofield.domain.model;

import java.time.Duration;
import java.util.Collection;

/**
 * El estado de vida de un equipo a partir de lo que dicen sus dispositivos. No es el keepalive de
 * transporte (que detecta una conexion TCP muerta) sino el latido de aplicacion, cada 10 s.
 */
public final class TeamLiveness {

    /** En orden de mejor a peor: {@link #best} se queda con el menor. */
    public enum Liveness {
        CONNECTED,
        STALE,
        DISCONNECTED
    }

    /**
     * @param staleAfter        silencio a partir del cual un stream abierto se considera STALE (30 s)
     * @param disconnectedAfter silencio a partir del cual ni un stream abierto cuenta como vivo (60 s)
     */
    public record Thresholds(Duration staleAfter, Duration disconnectedAfter) {

        public Thresholds {
            if (staleAfter == null || disconnectedAfter == null || staleAfter.isNegative() || !disconnectedAfter.minus(staleAfter).isPositive()) {
                throw new IllegalArgumentException("staleAfter must be >= 0 and smaller than disconnectedAfter");
            }
        }

        public static Thresholds standard() {
            return new Thresholds(Duration.ofSeconds(30), Duration.ofSeconds(60));
        }
    }

    private TeamLiveness() {
    }

    /**
     * Un dispositivo esta CONNECTED si tiene el stream abierto y ha hablado hace menos de
     * {@code staleAfter}; STALE si tiene el stream abierto pero lleva entre {@code staleAfter} y
     * {@code disconnectedAfter} callado (aplicacion colgada o red degradada); DISCONNECTED en
     * cualquier otro caso.
     */
    public static Liveness classify(boolean streamOpen, Duration silence, Thresholds thresholds) {
        if (!streamOpen || silence == null) {
            return Liveness.DISCONNECTED;
        }
        if (silence.compareTo(thresholds.staleAfter()) < 0) {
            return Liveness.CONNECTED;
        }
        if (silence.compareTo(thresholds.disconnectedAfter()) < 0) {
            return Liveness.STALE;
        }
        return Liveness.DISCONNECTED;
    }

    /** Un equipo esta tan vivo como el mejor de sus dispositivos; sin dispositivos, DISCONNECTED. */
    public static Liveness best(Collection<Liveness> devices) {
        Liveness best = Liveness.DISCONNECTED;
        for (Liveness device : devices) {
            if (device.ordinal() < best.ordinal()) {
                best = device;
            }
        }
        return best;
    }
}
