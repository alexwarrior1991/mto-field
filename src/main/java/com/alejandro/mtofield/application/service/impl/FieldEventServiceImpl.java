package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.CommandDraft;
import com.alejandro.mtofield.application.dto.EventContext;
import com.alejandro.mtofield.application.dto.StoredEvent;
import com.alejandro.mtofield.application.mapper.ProtoJson;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.application.service.FieldEventService;
import com.alejandro.mtofield.configuration.AuditActorResolver;
import com.alejandro.mtofield.grpc.v1.CommandAck;
import com.alejandro.mtofield.grpc.v1.EventResult;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldCommandRecord;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldEventKind;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldEventRecord;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldEventSyncStatus;
import com.alejandro.mtofield.infrastructure.persistence.repository.CommandAckRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.FieldCommandRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.FieldEventRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.PossessionShiftRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Cada evento empieza por {@code insert ... on conflict (device_id, sequence) do nothing}. Lo que
 * se aplica despues (el acuse, la salida de via) es a su vez idempotente en la base, asi que un
 * reenvio recorre el mismo camino, no escribe nada nuevo y recibe la misma respuesta. El
 * {@code EventResult} se emite con {@link FieldCommandService#issue} dentro de la misma
 * transaccion: nunca se contesta algo que no quede escrito.
 */
@Service
@RequiredArgsConstructor
class FieldEventServiceImpl implements FieldEventService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldEventServiceImpl.class);

    static final String UNKNOWN_COMMAND = "unknown command";
    static final String COMMAND_WITHOUT_ACK = "command does not require an acknowledgement";
    static final String SEQUENCE_REQUIRED = "sequence must be greater than 0";

    private final FieldEventRepository eventRepository;
    private final FieldCommandRepository commandRepository;
    private final CommandAckRepository ackRepository;
    private final PossessionShiftRepository shiftRepository;
    private final FieldCommandService commands;
    private final Clock clock;

    @Override
    @Transactional
    public void recordAck(EventContext context) {
        if (context.sequence() <= 0) {
            answer(context, EventResult.Outcome.REJECTED, SEQUENCE_REQUIRED);
            return;
        }
        store(context, FieldEventKind.COMMAND_ACK, FieldEventSyncStatus.NOT_REQUIRED, null);
        CommandAck ack = context.message().getCommandAck();
        Optional<FieldCommandRecord> command = parseUuid(ack.getCommandId()).flatMap(commandRepository::findById)
                .filter(record -> record.getPossessionId().equals(context.possessionId()));
        if (command.isEmpty()) {
            answer(context, EventResult.Outcome.REJECTED, UNKNOWN_COMMAND);
            return;
        }
        if (!command.get().isRequiresAck()) {
            answer(context, EventResult.Outcome.REJECTED, COMMAND_WITHOUT_ACK);
            return;
        }
        int inserted = ackRepository.insertIfMissing(command.get().getId(), context.shiftId(), context.deviceId(),
                context.principal().username(), ack.getAccepted(), ack.getReason().isBlank() ? null : ack.getReason());
        if (inserted == 1) {
            LOGGER.info("Command {} #{} acknowledged by shift {} from device {} ({})", command.get().getId(), command.get().getSequence(),
                    context.shiftId(), context.deviceId(), ack.getAccepted() ? "accepted" : "not accepted: " + ack.getReason());
        }
        answer(context, EventResult.Outcome.APPLIED, null);
    }

    @Override
    @Transactional
    public void recordClearOfTrack(EventContext context) {
        if (context.sequence() <= 0) {
            answer(context, EventResult.Outcome.REJECTED, SEQUENCE_REQUIRED);
            return;
        }
        store(context, FieldEventKind.CLEAR_OF_TRACK, FieldEventSyncStatus.NOT_REQUIRED, null);
        int marked = shiftRepository.markClear(context.possessionId(), context.shiftId(), context.principal().username(), context.deviceId(),
                context.message().getClearOfTrack().getEarthingRemoved());
        if (marked == 1) {
            LOGGER.info("Shift {} is clear of track (device {}, earthing removed: {})", context.shiftId(), context.deviceId(),
                    context.message().getClearOfTrack().getEarthingRemoved());
        }
        answer(context, EventResult.Outcome.APPLIED, null);
    }

    /**
     * Un evento nuevo queda PENDING y su resultado lo emite quien lo sincroniza; un reenvio
     * recibe aqui mismo el resultado que corresponde al estado guardado, y no se vuelve a encolar.
     */
    @Override
    @Transactional
    public StoredEvent recordTaskEvent(EventContext context) {
        if (context.sequence() <= 0) {
            answer(context, EventResult.Outcome.REJECTED, SEQUENCE_REQUIRED);
            return StoredEvent.rejected();
        }
        FieldEventKind kind = context.message().hasTaskStarted() ? FieldEventKind.TASK_STARTED : FieldEventKind.TASK_COMPLETED;
        StoredEvent stored = store(context, kind, FieldEventSyncStatus.PENDING, clock.instant());
        if (!stored.inserted()) {
            answerStored(context, stored);
        }
        return stored;
    }

    @Override
    @Transactional
    public void rejectInline(EventContext context, String reason) {
        answer(context, EventResult.Outcome.REJECTED, reason);
    }

    @Override
    @Transactional
    public void markSynced(UUID eventId) {
        eventRepository.markSynced(eventId);
    }

    @Override
    @Transactional
    public void markFailed(UUID eventId, String error, Instant nextAttemptAt) {
        eventRepository.markFailed(eventId, error, nextAttemptAt);
    }

    @Override
    @Transactional
    public void markRejected(UUID eventId, String reason) {
        eventRepository.markRejected(eventId, reason);
    }

    @Override
    @Transactional(readOnly = true)
    public long contiguousWatermark(String deviceId) {
        return eventRepository.contiguousWatermark(deviceId);
    }

    private StoredEvent store(EventContext context, FieldEventKind kind, FieldEventSyncStatus status, Instant nextAttemptAt) {
        TeamMessage message = context.message();
        UUID id = UUID.randomUUID();
        Instant occurredAt = message.hasOccurredAt()
                ? Instant.ofEpochSecond(message.getOccurredAt().getSeconds(), message.getOccurredAt().getNanos())
                : clock.instant();
        int inserted = eventRepository.insertIfMissing(id, context.deviceId(), context.sequence(), context.possessionId(), context.shiftId(),
                kind.name(), occurredAt, context.principal().username(), ProtoJson.print(message), status.name(), nextAttemptAt);
        if (inserted == 1) {
            return new StoredEvent(id, true, status);
        }
        FieldEventRecord existing = eventRepository.findByDeviceIdAndSequence(context.deviceId(), context.sequence())
                .orElseThrow(() -> new IllegalStateException("Event " + context.deviceId() + "/" + context.sequence() + " neither inserted nor found"));
        LOGGER.debug("Event {}/{} ({}) already stored as {}: resend", context.deviceId(), context.sequence(), kind, existing.getSyncStatus());
        return new StoredEvent(existing.getId(), false, existing.getSyncStatus());
    }

    /** El resultado que corresponde a un evento de tarea ya guardado, segun como quedo su sincronizacion. */
    private void answerStored(EventContext context, StoredEvent stored) {
        switch (stored.status()) {
            case SYNCED, NOT_REQUIRED -> answer(context, EventResult.Outcome.APPLIED, null);
            case REJECTED -> answer(context, EventResult.Outcome.REJECTED, eventRepository.findById(stored.id())
                    .map(FieldEventRecord::getLastError).orElse(null));
            case PENDING, FAILED -> answer(context, EventResult.Outcome.PENDING_SYNC, null);
        }
    }

    private void answer(EventContext context, EventResult.Outcome outcome, String reason) {
        commands.issue(context.possessionId(), CommandDraft.eventResult(context.shiftId(), context.sequence(), outcome, reason),
                AuditActorResolver.SYSTEM_ACTOR);
    }

    private static Optional<UUID> parseUuid(String value) {
        try {
            return Optional.of(UUID.fromString(value));
        } catch (IllegalArgumentException invalid) {
            return Optional.empty();
        }
    }
}
