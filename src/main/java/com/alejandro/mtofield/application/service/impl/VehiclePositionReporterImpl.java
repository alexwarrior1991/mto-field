package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.ShiftMembership;
import com.alejandro.mtofield.application.service.DomainEventPublisher;
import com.alejandro.mtofield.application.service.VehiclePositionReporter;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lo que dice {@link VehiclePositionReporter}. Un latido no se guarda (es el tablero); lo unico que
 * escribe es la posicion en el outbox, en una transaccion corta que antes relee el turno: un equipo que
 * ya salio de la via no informa mas, porque su vehiculo lo aparca el cierre del turno en
 * {@code mto-resources} y una posicion posterior lo devolveria a la via.
 */
@Service
class VehiclePositionReporterImpl implements VehiclePositionReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(VehiclePositionReporterImpl.class);

    public static final String REASON_VEHICLE_NOT_IN_SHIFT = "VEHICLE_NOT_IN_SHIFT";

    /** El vehiculo de un dispositivo y lo ultimo que se publico de el. */
    private record Binding(Object owner, ShiftMembership shift, UUID vehicleId, Long lastTrackId, BigDecimal lastKp, Instant lastPublishedAt,
                           boolean stopped) {

        Binding published(long trackId, BigDecimal kp, Instant at) {
            return new Binding(owner, shift, vehicleId, trackId, kp, at, false);
        }

        Binding stop() {
            return new Binding(owner, shift, vehicleId, lastTrackId, lastKp, lastPublishedAt, true);
        }
    }

    private final Map<String, Binding> bindings = new ConcurrentHashMap<>();
    private final PossessionShiftRepository shifts;
    private final DomainEventPublisher domainEvents;
    private final PositionsProperties properties;
    private final TransactionTemplate transactions;
    private final Clock clock;

    VehiclePositionReporterImpl(PossessionShiftRepository shifts, DomainEventPublisher domainEvents, PositionsProperties properties,
                                PlatformTransactionManager transactionManager, Clock clock) {
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
        bindings.put(deviceId, new Binding(owner, shift, vehicle, null, null, null, false));
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
            LOGGER.debug("Device {} sent a kp that is not a number ({}): no position", deviceId, kp);
            return;
        }
        Long track = trackOf(binding.shift().trackIds(), trackId);
        if (track == null) {
            LOGGER.debug("Device {} did not say its track and shift {} has {}: no position", deviceId, binding.shift().shiftId(),
                    binding.shift().trackIds());
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

    /** La via del latido, o la unica del turno; con varias y sin decirla, ninguna. */
    static Long trackOf(List<Long> shiftTracks, long declared) {
        if (declared > 0) {
            return declared;
        }
        return shiftTracks.size() == 1 ? shiftTracks.getFirst() : null;
    }
}
