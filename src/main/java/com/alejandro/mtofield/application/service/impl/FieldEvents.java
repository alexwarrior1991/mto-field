package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.messaging.DomainEvent;
import com.alejandro.mtofield.infrastructure.persistence.entity.Possession;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionShift;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Los eventos que este servicio cuenta de si mismo (docs/06-messaging.md): sus nombres y, campo a
 * campo, lo que viaja en {@code values}. Es el unico sitio donde se construyen: la lista blanca de
 * lo que sale es esta, y {@code DomainEvent} rechaza ademas cualquier clave que huela a secreto.
 *
 * <p>Un solo agregado, {@code possession}: todos los eventos de una noche llevan la posesion como
 * entidad, asi que el orden estricto por agregado del relay mantiene {@code opened} antes que
 * {@code evacuation-issued} y este antes que {@code closed} aunque uno falle y se reintente. Las
 * claves solo se anaden: quitar o renombrar una rompe a {@code mto-notification}, que copia los
 * ejemplos de {@code docs/messaging/examples} como contrato.</p>
 */
public final class FieldEvents {

    public static final String POSSESSION = "possession";

    public static final String OPENED = "opened";
    public static final String CLOSED = "closed";
    public static final String EVACUATION_ISSUED = "evacuation-issued";
    public static final String EVACUATION_ACKNOWLEDGED = "evacuation-acknowledged";
    public static final String EVACUATION_UNACKNOWLEDGED = "evacuation-unacknowledged";
    public static final String CLEAR_OF_TRACK = "clear-of-track";

    private FieldEvents() {
    }

    /** La posesion recien abierta, con sus turnos y sus equipos. */
    public static DomainEvent possessionOpened(Possession possession) {
        return event(possession, OPENED, possession(possession));
    }

    /**
     * La posesion cerrada: forzada o no, con el motivo y los equipos que seguian en la via
     * ({@code pendingTeams}, vacio si {@code allClear}).
     */
    public static DomainEvent possessionClosed(Possession possession, List<String> pendingTeams) {
        Map<String, Object> values = possession(possession);
        values.put("closedAt", possession.getClosedAt());
        values.put("closedBy", possession.getClosedBy());
        values.put("forced", possession.isForced());
        values.put("closeReason", possession.getCloseReason());
        values.put("pendingTeams", List.copyOf(pendingTeams));
        values.put("allClear", pendingTeams.isEmpty());
        return event(possession, CLOSED, values);
    }

    /** La orden de desalojo emitida a todos los equipos de la posesion. */
    public static DomainEvent evacuationIssued(Possession possession, UUID commandId, long sequence, String reason, Instant issuedAt,
                                               String issuedBy) {
        Map<String, Object> values = possession(possession);
        values.put("commandId", commandId.toString());
        values.put("sequence", sequence);
        values.put("reason", reason);
        values.put("issuedAt", issuedAt);
        values.put("issuedBy", issuedBy);
        return event(possession, EVACUATION_ISSUED, values);
    }

    /**
     * El primer acuse de un equipo a un desalojo (los reenvios no cuentan), con los equipos que
     * todavia no han acusado y si con este ya han acusado todos.
     */
    public static DomainEvent evacuationAcknowledged(Possession possession, UUID commandId, long sequence, PossessionShift shift, String deviceId,
                                                     String ackedBy, boolean accepted, String reason, Instant ackedAt, List<String> pendingTeams) {
        Map<String, Object> values = possession(possession);
        values.put("commandId", commandId.toString());
        values.put("sequence", sequence);
        values.putAll(shift(shift));
        values.put("deviceId", deviceId);
        values.put("ackedBy", ackedBy);
        values.put("accepted", accepted);
        values.put("reason", reason);
        values.put("ackedAt", ackedAt);
        values.put("pendingTeams", List.copyOf(pendingTeams));
        values.put("allAcknowledged", pendingTeams.isEmpty());
        return event(possession, EVACUATION_ACKNOWLEDGED, values);
    }

    /** Un desalojo con equipos sin acusar pasado el plazo: lo publica el vigilante, una sola vez por orden. */
    public static DomainEvent evacuationUnacknowledged(Possession possession, UUID commandId, long sequence, Instant issuedAt, String issuedBy,
                                                       List<String> pendingTeams, Instant now) {
        Map<String, Object> values = possession(possession);
        values.put("commandId", commandId.toString());
        values.put("sequence", sequence);
        values.put("issuedAt", issuedAt);
        values.put("issuedBy", issuedBy);
        values.put("pendingTeams", List.copyOf(pendingTeams));
        values.put("overdueSeconds", Duration.between(issuedAt, now).toSeconds());
        return event(possession, EVACUATION_UNACKNOWLEDGED, values);
    }

    /** Un equipo fuera de la via (la primera vez; los reenvios no cuentan) y si con el ya estan todos. */
    public static DomainEvent clearOfTrack(Possession possession, PossessionShift shift, String deviceId, String clearedBy, boolean earthingRemoved,
                                           Instant clearedAt, List<String> pendingTeams) {
        Map<String, Object> values = possession(possession);
        values.putAll(shift(shift));
        values.put("deviceId", deviceId);
        values.put("clearedBy", clearedBy);
        values.put("earthingRemoved", earthingRemoved);
        values.put("clearedAt", clearedAt);
        values.put("pendingTeams", List.copyOf(pendingTeams));
        values.put("allClear", pendingTeams.isEmpty());
        return event(possession, CLEAR_OF_TRACK, values);
    }

    /** Lo comun a todos: la posesion, sus turnos y sus equipos, en orden de equipo. */
    static Map<String, Object> possession(Possession possession) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("code", possession.getCode());
        values.put("status", name(possession.getStatus()));
        values.put("shiftDate", possession.getShiftDate());
        values.put("endsAt", possession.getEndsAt());
        values.put("openedAt", possession.getOpenedAt());
        values.put("openedBy", possession.getOpenedBy());
        List<PossessionShift> shifts = new ArrayList<>(possession.getShifts());
        shifts.sort(Comparator.comparing(PossessionShift::getTeamCode, Comparator.nullsLast(Comparator.naturalOrder())));
        values.put("shiftCount", shifts.size());
        values.put("teamCodes", shifts.stream().map(PossessionShift::getTeamCode).toList());
        List<Map<String, Object>> entries = new ArrayList<>(shifts.size());
        for (PossessionShift shift : shifts) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("shiftId", text(shift.getShiftId()));
            entry.put("shiftCode", shift.getShiftCode());
            entry.put("teamCode", shift.getTeamCode());
            entry.put("teamName", shift.getTeamName());
            entry.put("plannedEnd", shift.getPlannedEnd());
            entries.add(entry);
        }
        values.put("shifts", entries);
        return values;
    }

    private static Map<String, Object> shift(PossessionShift shift) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("shiftId", text(shift.getShiftId()));
        values.put("shiftCode", shift.getShiftCode());
        values.put("teamCode", shift.getTeamCode());
        return values;
    }

    private static DomainEvent event(Possession possession, String eventName, Map<String, Object> values) {
        return new DomainEvent(POSSESSION, id(possession.getId()), eventName, values);
    }

    private static String id(UUID id) {
        return Objects.requireNonNull(id, "the possession must be saved before publishing its event").toString();
    }

    private static String text(UUID id) {
        return id == null ? null : id.toString();
    }

    private static String name(Enum<?> value) {
        return value == null ? null : value.name();
    }
}
