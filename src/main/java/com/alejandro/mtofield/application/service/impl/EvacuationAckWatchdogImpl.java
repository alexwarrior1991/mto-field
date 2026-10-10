package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.service.DomainEventPublisher;
import com.alejandro.mtofield.application.service.EvacuationAckWatchdog;
import com.alejandro.mtofield.configuration.scheduling.EvacuationWatchProperties;
import com.alejandro.mtofield.infrastructure.messaging.outbox.MessagingCorrelation;
import com.alejandro.mtofield.infrastructure.persistence.entity.CommandAckRecord;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldCommandRecord;
import com.alejandro.mtofield.infrastructure.persistence.entity.Possession;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionShift;
import com.alejandro.mtofield.infrastructure.persistence.repository.CommandAckRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.FieldCommandRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.PossessionRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.PossessionShiftRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Una pasada del vigilante: por cada desalojo vencido que nadie miro, en su propia transaccion,
 * quien gana la marca ({@code ack_watched_at}) decide. Si quedan equipos sin acusar publica el
 * aviso, con un {@code operationId} derivado de la orden para que el inbox del consumidor descarte
 * cualquier repeticion; si ya habian acusado todos, solo marca. Ningun hilo propio tiene usuario:
 * el actor del aviso es {@code SYSTEM}, y la correlacion, la noche.
 */
@Service
class EvacuationAckWatchdogImpl implements EvacuationAckWatchdog {

    private static final Logger LOGGER = LoggerFactory.getLogger(EvacuationAckWatchdogImpl.class);

    static final int BATCH_SIZE = 100;
    static final String OPERATION_PREFIX = "evacuation-unacknowledged:";

    private final FieldCommandRepository commands;
    private final PossessionRepository possessions;
    private final PossessionShiftRepository shifts;
    private final CommandAckRepository acks;
    private final DomainEventPublisher domainEvents;
    private final EvacuationWatchProperties properties;
    private final TransactionTemplate transaction;
    private final Clock clock;

    EvacuationAckWatchdogImpl(FieldCommandRepository commands, PossessionRepository possessions, PossessionShiftRepository shifts,
                              CommandAckRepository acks, DomainEventPublisher domainEvents, EvacuationWatchProperties properties,
                              PlatformTransactionManager transactionManager, Clock clock) {
        this.commands = commands;
        this.possessions = possessions;
        this.shifts = shifts;
        this.acks = acks;
        this.domainEvents = domainEvents;
        this.properties = properties;
        this.transaction = new TransactionTemplate(transactionManager);
        this.clock = clock;
    }

    @Override
    public int check() {
        Instant now = clock.instant();
        Instant before = now.minus(properties.ackTimeout());
        int published = 0;
        for (FieldCommandRecord command : commands.findEvacuationsToWatch(before, Limit.of(BATCH_SIZE))) {
            if (Boolean.TRUE.equals(transaction.execute(status -> watch(command, now)))) {
                published++;
            }
        }
        return published;
    }

    /** Dentro de su transaccion: la marca primero, y solo si se gana se decide y se publica. */
    private boolean watch(FieldCommandRecord command, Instant now) {
        if (commands.markAckWatched(command.getId(), now) != 1) {
            return false;
        }
        List<UUID> acknowledged = acks.findByCommandId(command.getId()).stream().map(CommandAckRecord::getShiftId).toList();
        List<String> pendingTeams = shifts.findByPossession_IdOrderByTeamCodeAsc(command.getPossessionId()).stream()
                .filter(PossessionShift::isOpen)
                .filter(shift -> !acknowledged.contains(shift.getShiftId()))
                .map(PossessionShift::getTeamCode)
                .toList();
        if (pendingTeams.isEmpty()) {
            return false;
        }
        Possession possession = possessions.findById(command.getPossessionId()).orElse(null);
        if (possession == null || !possession.isOpen()) {
            return false;
        }
        LOGGER.warn("Evacuation {} #{} of possession {} unacknowledged by {} after {}", command.getId(), command.getSequence(), possession.getCode(),
                pendingTeams, properties.ackTimeout());
        MessagingCorrelation.with(possession.getCode(), () -> domainEvents.publish(operationId(command.getId()),
                FieldEvents.evacuationUnacknowledged(possession, command.getId(), command.getSequence(), command.getIssuedAt(), command.getIssuedBy(),
                        pendingTeams, now)));
        return true;
    }

    /** Derivado de la orden: dos pasadas o dos replicas que llegaran a publicar dirian lo mismo al inbox del consumidor. */
    static UUID operationId(UUID commandId) {
        return UUID.nameUUIDFromBytes((OPERATION_PREFIX + commandId).getBytes(StandardCharsets.UTF_8));
    }
}
