package com.alejandro.mtofield.domain.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Como va una orden con acuse en el tablero, equipo a equipo y en el orden de los equipos:
 * acusada, o pendiente; y una pendiente esta enviada (escrita en un stream vivo, que no es lo
 * mismo que entregada) o en cola (el equipo no tiene stream y la recibira al reanudar).
 */
public record CommandAckSummary(
        List<String> ackedBy,
        List<String> pending,
        List<String> sentTo,
        List<String> queuedFor
) {

    public CommandAckSummary {
        ackedBy = List.copyOf(ackedBy);
        pending = List.copyOf(pending);
        sentTo = List.copyOf(sentTo);
        queuedFor = List.copyOf(queuedFor);
    }

    /**
     * @param teams  los equipos de la posesion, en el orden del tablero
     * @param acked  los que han acusado la orden
     * @param sent   los que tienen algun stream en el que la orden ya se ha escrito
     */
    public static CommandAckSummary of(Collection<String> teams, Set<String> acked, Set<String> sent) {
        List<String> ackedBy = new ArrayList<>();
        List<String> pending = new ArrayList<>();
        List<String> sentTo = new ArrayList<>();
        List<String> queuedFor = new ArrayList<>();
        for (String team : teams) {
            if (acked.contains(team)) {
                ackedBy.add(team);
                continue;
            }
            pending.add(team);
            if (sent.contains(team)) {
                sentTo.add(team);
            } else {
                queuedFor.add(team);
            }
        }
        return new CommandAckSummary(ackedBy, pending, sentTo, queuedFor);
    }

    public boolean allAcked() {
        return pending.isEmpty();
    }
}
