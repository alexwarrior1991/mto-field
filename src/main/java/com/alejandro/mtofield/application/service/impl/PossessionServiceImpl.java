package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.PossessionView;
import com.alejandro.mtofield.application.dto.ShiftMembership;
import com.alejandro.mtofield.application.event.PossessionClosed;
import com.alejandro.mtofield.application.exception.InvalidPossessionRequestException;
import com.alejandro.mtofield.application.exception.PossessionNotAllClearException;
import com.alejandro.mtofield.application.exception.PossessionNotFoundException;
import com.alejandro.mtofield.application.exception.PossessionNotOpenException;
import com.alejandro.mtofield.application.exception.ShiftAlreadyInOpenPossessionException;
import com.alejandro.mtofield.application.service.DomainEventPublisher;
import com.alejandro.mtofield.application.service.FieldCodeGenerator;
import com.alejandro.mtofield.application.service.MaintenanceClient;
import com.alejandro.mtofield.application.service.PossessionService;
import com.alejandro.mtofield.domain.model.PossessionRules;
import com.alejandro.mtofield.domain.model.PossessionStateMachine;
import com.alejandro.mtofield.domain.model.ShiftSnapshot;
import com.alejandro.mtofield.infrastructure.persistence.entity.Possession;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionShift;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionStatus;
import com.alejandro.mtofield.infrastructure.messaging.outbox.MessagingCorrelation;
import com.alejandro.mtofield.infrastructure.persistence.repository.PossessionRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.PossessionShiftRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
class PossessionServiceImpl implements PossessionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PossessionServiceImpl.class);

    private final PossessionRepository possessions;
    private final PossessionShiftRepository shifts;
    private final MaintenanceClient maintenance;
    private final FieldCodeGenerator codes;
    private final ApplicationEventPublisher events;
    private final DomainEventPublisher domainEvents;
    private final Clock clock;

    /**
     * Los turnos se leen de mto-maintenance (o del cliente apagado) y se validan con las reglas
     * del dominio. Que un turno no este ya en otra posesion abierta lo garantiza el indice parcial
     * {@code uq_possession_shift_open}: la comprobacion previa solo da un mensaje mejor, y el
     * {@code flush} dentro del metodo es lo que convierte la violacion en la excepcion de negocio.
     */
    @Override
    @Transactional
    public PossessionView open(List<UUID> shiftIds, Instant endsAt, String openedBy) {
        if (shiftIds == null || shiftIds.isEmpty()) {
            throw new InvalidPossessionRequestException("A possession needs at least one shift");
        }
        List<ShiftSnapshot> snapshots = new ArrayList<>(shiftIds.size());
        for (UUID shiftId : shiftIds) {
            snapshots.add(maintenance.findShift(shiftId)
                    .orElseThrow(() -> new InvalidPossessionRequestException("Shift " + shiftId + " does not exist in mto-maintenance")));
        }
        LocalDate shiftDate;
        Instant effectiveEndsAt;
        try {
            shiftDate = PossessionRules.validateShifts(snapshots);
            effectiveEndsAt = endsAt != null ? endsAt : PossessionRules.defaultEndsAt(snapshots);
        } catch (IllegalArgumentException invalid) {
            throw new InvalidPossessionRequestException(invalid.getMessage());
        }
        for (ShiftSnapshot snapshot : snapshots) {
            if (snapshot.teamCode() == null || snapshot.teamCode().isBlank()) {
                throw new InvalidPossessionRequestException("Shift " + snapshot.id() + " has no team");
            }
            if (shifts.existsByShiftIdAndOpenTrue(snapshot.id())) {
                throw new ShiftAlreadyInOpenPossessionException(snapshot.id());
            }
        }
        Possession possession = Possession.builder()
                .code(codes.nextPossessionCode())
                .status(PossessionStatus.OPEN)
                .shiftDate(shiftDate)
                .endsAt(effectiveEndsAt)
                .openedAt(clock.instant())
                .openedBy(openedBy)
                .build();
        for (ShiftSnapshot snapshot : snapshots) {
            possession.addShift(PossessionShift.builder()
                    .shiftId(snapshot.id())
                    .shiftCode(snapshot.code())
                    .teamCode(snapshot.teamCode())
                    .teamName(snapshot.teamName())
                    .plannedEnd(snapshot.plannedEnd())
                    .open(true)
                    .build());
        }
        try {
            possession = possessions.saveAndFlush(possession);
        } catch (DataIntegrityViolationException violation) {
            // Dos responsables abriendo a la vez con un turno en comun: el indice parcial decide.
            throw new ShiftAlreadyInOpenPossessionException(shiftIds.getFirst());
        }
        Possession opened = possession;
        MessagingCorrelation.with(opened.getCode(), () -> domainEvents.publish(FieldEvents.possessionOpened(opened)));
        LOGGER.info("Possession {} opened by {} with {} shift(s), ends at {}", possession.getCode(), openedBy, snapshots.size(), effectiveEndsAt);
        return toView(possession);
    }

    /**
     * Bloquea la fila de la posesion, la misma que bloquea el contador de ordenes: una orden que se
     * esta emitiendo termina antes de que la posesion se cierre, o ve la posesion cerrada.
     */
    @Override
    @Transactional
    public PossessionView close(UUID possessionId, boolean force, String reason, String closedBy) {
        try {
            PossessionRules.validateCloseRequest(force, reason);
        } catch (IllegalArgumentException invalid) {
            throw new InvalidPossessionRequestException(invalid.getMessage());
        }
        Possession possession = possessions.findWithLockById(possessionId)
                .orElseThrow(() -> new PossessionNotFoundException(possessionId));
        if (!PossessionStateMachine.canClose(possession.getStatus())) {
            throw new PossessionNotOpenException(possessionId);
        }
        List<String> pendingTeams = shifts.findByPossession_IdOrderByTeamCodeAsc(possessionId).stream()
                .filter(shift -> !shift.isClearOfTrack())
                .map(PossessionShift::getTeamCode)
                .toList();
        if (!PossessionStateMachine.closeAllowed(pendingTeams.isEmpty(), force)) {
            throw new PossessionNotAllClearException(possessionId, pendingTeams);
        }
        possession.setStatus(PossessionStatus.CLOSED);
        possession.setClosedAt(clock.instant());
        possession.setClosedBy(closedBy);
        possession.setForced(force && !pendingTeams.isEmpty());
        possession.setCloseReason(reason == null || reason.isBlank() ? null : reason);
        shifts.closeAll(possessionId, closedBy);
        possession = possessions.save(possession);
        events.publishEvent(new PossessionClosed(possessionId));
        Possession closed = possession;
        MessagingCorrelation.with(closed.getCode(), () -> domainEvents.publish(FieldEvents.possessionClosed(closed, pendingTeams)));
        LOGGER.info("Possession {} closed by {}{}", possession.getCode(), closedBy,
                possession.isForced() ? " (forced, teams still on the track: " + String.join(", ", pendingTeams) + ")" : "");
        return toView(possession);
    }

    @Override
    @Transactional
    public PossessionView changeEndsAt(UUID possessionId, Instant endsAt, String changedBy) {
        if (endsAt == null) {
            throw new InvalidPossessionRequestException("ends_at is required");
        }
        Possession possession = possessions.findWithLockById(possessionId)
                .orElseThrow(() -> new PossessionNotFoundException(possessionId));
        if (!PossessionStateMachine.acceptsTraffic(possession.getStatus())) {
            throw new PossessionNotOpenException(possessionId);
        }
        possession.setEndsAt(endsAt);
        LOGGER.info("Possession {} now ends at {} (changed by {})", possession.getCode(), endsAt, changedBy);
        return toView(possessions.save(possession));
    }

    @Override
    @Transactional(readOnly = true)
    public PossessionView get(UUID possessionId) {
        return possessions.findById(possessionId).map(this::toView)
                .orElseThrow(() -> new PossessionNotFoundException(possessionId));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<ShiftMembership> membershipOfOpenPossession(UUID shiftId) {
        return shifts.findOpenMembership(shiftId)
                .map(membership -> new ShiftMembership(membership.getPossessionId(), membership.getShiftId(),
                        membership.getTeamCode(), membership.getEndsAt()));
    }

    private PossessionView toView(Possession possession) {
        List<UUID> shiftIds = possession.getShifts().stream().map(PossessionShift::getShiftId).toList();
        return new PossessionView(possession.getId(), possession.getCode(), shiftIds, possession.getEndsAt(), possession.getStatus());
    }
}
