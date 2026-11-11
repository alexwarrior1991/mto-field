package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.ShiftMembership;
import com.alejandro.mtofield.application.service.DomainEventPublisher;
import com.alejandro.mtofield.application.service.VehiclePositionReporter;
import com.alejandro.mtofield.configuration.metrics.FieldMetrics;
import com.alejandro.mtofield.configuration.positions.PositionsProperties;
import com.alejandro.mtofield.infrastructure.messaging.outbox.MessagingCorrelation;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionShift;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionStatus;
import com.alejandro.mtofield.infrastructure.persistence.repository.PossessionShiftRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lo que dice {@link VehiclePositionReporter}. Un latido no se guarda (es el tablero); lo unico que
 * escribe es la posicion en el outbox, en una transaccion corta que antes relee el turno: un equipo que
 * ya salio de la via no informa mas, porque su vehiculo lo aparca el cierre del turno en
 * {@code mto-resources} y una posicion posterior lo devolveria a la via.
 *
 * <p>Lo que declara el dispositivo se comprueba aqui, porque quien lo consume no puede: la via tiene
 * que ser una de las del turno (las que {@code mto-maintenance} dio al abrir la posesion) y el kp,
 * un decimal que quepa en su columna. Lo que no pasa no se publica y se cuenta.</p>
 */
@Service
class VehiclePositionReporterImpl implements VehiclePositionReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(VehiclePositionReporterImpl.class);

    public static final String REASON_VEHICLE_NOT_IN_SHIFT = "VEHICLE_NOT_IN_SHIFT";
    /** El kp de mto-resources es numeric(12,3): lo que no cabe acabaria en su DLQ. */
    static final int KP_INTEGER_DIGITS = 9;
    static final int KP_DECIMALS = 3;

    /** El vehiculo de un dispositivo y lo ultimo que se publico de el. */
    private record Binding(Object owner, ShiftMembership shift, UUID vehicleId, Long lastTrackId, BigDecimal lastKp, Instant lastPublishedAt,
                           boolean stopped, Set<String> warned) {

        Binding published(long trackId, BigDecimal kp, Instant at) {
            return new Binding(owner, shift, vehicleId, trackId, kp, at, false, warned);
        }

        Binding stop() {
            return new Binding(owner, shift, vehicleId, lastTrackId, lastKp, lastPublishedAt, true, warned);
        }
    }

    private final Map<String, Binding> bindings = new ConcurrentHashMap<>();
    private final PossessionShiftRepository shifts;
    private final DomainEventPublisher domainEvents;
    private final PositionsProperties properties;
    private final TransactionTemplate transactions;
    private final Clock clock;
    private final FieldMetrics metrics;

    VehiclePositionReporterImpl(PossessionShiftRepository shifts, DomainEventPublisher domainEvents, PositionsProperties properties,
                                PlatformTransactionManager transactionManager, Clock clock, FieldMetrics metrics) {
        this.metrics = metrics;
        this.shifts = shifts;
        this.domainEvents = domainEvents;
        this.properties = properties;
        this.transactions = new TransactionTemplate(transactionManager);
        this.clock = clock;
    }

    @Override
    public Optional<String> bind(Object owner, String deviceId, ShiftMembership shift, String vehicleId) {
        UUID vehicle;
        try {
            vehicle = UUID.fromString(vehicleId.trim());
        } catch (IllegalArgumentException invalid) {
            return Optional.of(REASON_VEHICLE_NOT_IN_SHIFT + ": Join.vehicle_id is not a UUID");
        }
        if (!shift.vehicleIds().contains(vehicle)) {
            return Optional.of(REASON_VEHICLE_NOT_IN_SHIFT + ": vehicle " + vehicle + " is not a vehicle of shift " + shift.shiftId()
                    + "; its position is not reported");
        }
        bindings.put(deviceId, new Binding(owner, shift, vehicle, null, null, null, false, ConcurrentHashMap.newKeySet()));
        LOGGER.info("Device {} reports the position of vehicle {} in shift {}", deviceId, vehicle, shift.shiftId());
        return Optional.empty();
    }

    @Override
    public void report(String deviceId, String kp, long trackId, String reportedBy) {
        Binding binding = bindings.get(deviceId);
        if (!properties.enabled() || binding == null || binding.stopped() || kp == null || kp.isBlank()) {
            return;
        }
        BigDecimal position;
        try {
            position = new BigDecimal(kp.trim());
        } catch (NumberFormatException invalid) {
            discard(binding, deviceId, FieldMetrics.POSITION_INVALID_KP, "a kp that is not a number");
            return;
        }
        if (!fitsTheKpColumn(position)) {
            discard(binding, deviceId, FieldMetrics.POSITION_INVALID_KP, "a kp outside " + KP_INTEGER_DIGITS + " integer digits and "
                    + KP_DECIMALS + " decimals");
            return;
        }
        List<Long> shiftTracks = binding.shift().trackIds();
        if (trackId > 0 && !shiftTracks.contains(trackId)) {
            discard(binding, deviceId, FieldMetrics.POSITION_TRACK_NOT_IN_SHIFT, "track " + trackId + ", which is not one of " + shiftTracks);
            return;
        }
        Long track = trackOf(shiftTracks, trackId);
        if (track == null) {
            discard(binding, deviceId, FieldMetrics.POSITION_TRACK_NOT_SAID, "no track, and the shift has " + shiftTracks);
            return;
        }
        Instant now = clock.instant();
        boolean unchanged = track.equals(binding.lastTrackId()) && binding.lastKp() != null && binding.lastKp().compareTo(position) == 0;
        boolean tooSoon = binding.lastPublishedAt() != null && now.isBefore(binding.lastPublishedAt().plus(properties.minInterval()));
        if (unchanged || tooSoon) {
            return;
        }
        try {
            Boolean published = transactions.execute(status -> publish(binding, deviceId, track, position, now, reportedBy));
            bindings.computeIfPresent(deviceId, (device, current) -> current.owner() != binding.owner() ? current
                    : Boolean.TRUE.equals(published) ? current.published(track, position, now) : current.stop());
        } catch (RuntimeException failure) {
            LOGGER.warn("Position of vehicle {} (device {}) not written to the outbox: {}", binding.vehicleId(), deviceId, failure.toString());
        }
    }

    /** {@code false} si el turno ya no esta en la via (o en una posesion abierta): el dispositivo deja de informar. */
    private boolean publish(Binding binding, String deviceId, long track, BigDecimal position, Instant now, String reportedBy) {
        Optional<PossessionShift> open = shifts.findOpenShift(binding.shift().shiftId(), PossessionStatus.OPEN);
        if (open.isEmpty() || open.get().isClearOfTrack()) {
            LOGGER.info("Shift {} left the track: device {} stops reporting the position of vehicle {}", binding.shift().shiftId(), deviceId,
                    binding.vehicleId());
            return false;
        }
        PossessionShift shift = open.get();
        String possessionCode = shift.getPossession().getCode();
        MessagingCorrelation.with(possessionCode, () -> domainEvents.publishTelemetry(FieldEvents.positionReported(possessionCode, shift, deviceId,
                binding.vehicleId(), track, position, now, reportedBy)));
        return true;
    }

    @Override
    public void unbind(Object owner, String deviceId) {
        bindings.computeIfPresent(deviceId, (device, current) -> current.owner() == owner ? null : current);
    }

    /**
     * La via del latido si es una de las del turno, o la unica del turno; con varias y sin decirla, o
     * diciendo una que no es suya (tambien cuando del turno no se conoce ninguna), ninguna.
     */
    static Long trackOf(List<Long> shiftTracks, long declared) {
        if (declared > 0) {
            return shiftTracks.contains(declared) ? declared : null;
        }
        return shiftTracks.size() == 1 ? shiftTracks.getFirst() : null;
    }

    /**
     * El kp cabe en el {@code numeric(12,3)} de quien lo consume: 9 cifras enteras y 3 decimales, con
     * signo (un punto anterior al origen de la kilometracion es negativo). Los ceros de la derecha no
     * cuentan ({@code 34.1000} es {@code 34.1}).
     */
    static boolean fitsTheKpColumn(BigDecimal kp) {
        BigDecimal plain = kp.stripTrailingZeros();
        return plain.scale() <= KP_DECIMALS && plain.precision() - plain.scale() <= KP_INTEGER_DIGITS;
    }

    /**
     * Una posicion que no se publica: se cuenta siempre y se registra una vez por dispositivo y motivo
     * mientras dure su stream (un latido llega cada pocos segundos y repetiria la linea toda la noche).
     * El latido en si no se rechaza ni cierra nada: sigue contando para el tablero.
     */
    private void discard(Binding binding, String deviceId, String reason, String what) {
        metrics.recordDiscardedPosition(reason);
        if (binding.warned().add(reason)) {
            LOGGER.warn("Device {} sent {}: the position of vehicle {} in shift {} is not published (logged once per stream)", deviceId, what,
                    binding.vehicleId(), binding.shift().shiftId());
        } else {
            LOGGER.debug("Device {} sent {}: no position", deviceId, what);
        }
    }
}
