package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.CommandDraft;
import com.alejandro.mtofield.application.dto.IssuedCommand;
import com.alejandro.mtofield.application.dto.StoredCommand;
import com.alejandro.mtofield.application.event.CommandCommitted;
import com.alejandro.mtofield.application.exception.PossessionNotFoundException;
import com.alejandro.mtofield.application.exception.PossessionNotOpenException;
import com.alejandro.mtofield.application.mapper.ProtoJson;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldCommandRecord;
import com.alejandro.mtofield.infrastructure.persistence.repository.FieldCommandRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.PossessionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Emitir una orden es una transaccion que toma el numero, inserta la fila y publica
 * {@link CommandCommitted}; el despachador la recibe despues del commit. Se usa un
 * {@link TransactionTemplate} y no {@code @Transactional} porque el reintento de una clave de
 * idempotencia repetida se resuelve releyendo FUERA de la transaccion que fallo (PostgreSQL la da
 * por abortada), y dentro de una transaccion ajena (el acuse que contesta en linea) se une a ella.
 */
@Service
class FieldCommandServiceImpl implements FieldCommandService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldCommandServiceImpl.class);

    private final FieldCommandRepository commands;
    private final PossessionRepository possessions;
    private final ApplicationEventPublisher events;
    private final TransactionTemplate transaction;
    private final Clock clock;

    FieldCommandServiceImpl(FieldCommandRepository commands, PossessionRepository possessions, ApplicationEventPublisher events,
                            PlatformTransactionManager transactionManager, Clock clock) {
        this.commands = commands;
        this.possessions = possessions;
        this.events = events;
        this.transaction = new TransactionTemplate(transactionManager);
        this.clock = clock;
    }

    @Override
    public IssuedCommand issue(UUID possessionId, CommandDraft draft, String issuedBy) {
        String key = draft.idempotencyKey();
        if (key != null) {
            Optional<FieldCommandRecord> existing = commands.findByPossessionIdAndIdempotencyKey(possessionId, key);
            if (existing.isPresent()) {
                LOGGER.debug("Command with idempotency key {} already issued on possession {}: sequence {}", key, possessionId, existing.get().getSequence());
                return issued(existing.get());
            }
        }
        try {
            return transaction.execute(status -> insert(possessionId, draft, issuedBy));
        } catch (DataIntegrityViolationException violation) {
            if (key == null) {
                throw violation;
            }
            // Dos reintentos a la vez con la misma clave: gana el que confirmo, y este devuelve el suyo.
            return commands.findByPossessionIdAndIdempotencyKey(possessionId, key)
                    .map(FieldCommandServiceImpl::issued)
                    .orElseThrow(() -> violation);
        }
    }

    private IssuedCommand insert(UUID possessionId, CommandDraft draft, String issuedBy) {
        long sequence = possessions.nextCommandSequence(possessionId).orElseThrow(() -> {
            if (possessions.existsById(possessionId)) {
                return new PossessionNotOpenException(possessionId);
            }
            return new PossessionNotFoundException(possessionId);
        });
        UUID commandId = UUID.randomUUID();
        Instant issuedAt = clock.instant();
        FieldCommand command = draft.build(commandId, sequence, issuedAt);
        commands.insert(commandId, possessionId, sequence, draft.kind().name(), draft.targetShiftId(), draft.idempotencyKey(),
                draft.requiresAck(), issuedAt, issuedBy, ProtoJson.print(command));
        events.publishEvent(new CommandCommitted(possessionId, draft.targetShiftId(), command));
        LOGGER.debug("Command {} #{} ({}) issued on possession {} by {}{}", commandId, sequence, draft.kind(), possessionId, issuedBy,
                draft.targetShiftId() == null ? " to all teams" : " to shift " + draft.targetShiftId());
        return new IssuedCommand(commandId, sequence);
    }

    @Override
    @Transactional(readOnly = true)
    public List<StoredCommand> replayAfter(UUID possessionId, UUID shiftId, long after, int limit) {
        return commands.findReplay(possessionId, shiftId, after, Limit.of(limit)).stream().map(FieldCommandServiceImpl::stored).toList();
    }

    @Override
    @Transactional(readOnly = true)
    public List<StoredCommand> range(UUID possessionId, long from, long to) {
        if (from > to) {
            return List.of();
        }
        return commands.findByPossessionIdAndSequenceBetweenOrderBySequenceAsc(possessionId, from, to).stream()
                .map(FieldCommandServiceImpl::stored)
                .toList();
    }

    @Override
    @Transactional(readOnly = true)
    public long maxSequence(UUID possessionId) {
        return commands.maxSequence(possessionId);
    }

    private static IssuedCommand issued(FieldCommandRecord record) {
        return new IssuedCommand(record.getId(), record.getSequence());
    }

    static StoredCommand stored(FieldCommandRecord record) {
        FieldCommand command = ProtoJson.parse(record.getPayload(), FieldCommand.newBuilder()).build();
        return new StoredCommand(record.getId(), record.getSequence(), record.getTargetShiftId(), record.isRequiresAck(), command);
    }
}
