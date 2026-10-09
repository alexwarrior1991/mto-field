package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.CommandDraft;
import com.alejandro.mtofield.application.dto.CompleteTaskCommand;
import com.alejandro.mtofield.application.dto.EventContext;
import com.alejandro.mtofield.application.dto.SyncJob;
import com.alejandro.mtofield.application.exception.BusinessException;
import com.alejandro.mtofield.application.exception.MaintenanceRejectedException;
import com.alejandro.mtofield.application.exception.MaintenanceUnavailableException;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.application.service.FieldEventService;
import com.alejandro.mtofield.application.service.FieldEventSynchronizer;
import com.alejandro.mtofield.application.service.MaintenanceClient;
import com.alejandro.mtofield.configuration.AuditActorResolver;
import com.alejandro.mtofield.configuration.maintenance.MaintenanceProperties;
import com.alejandro.mtofield.configuration.metrics.FieldMetrics;
import com.alejandro.mtofield.domain.model.TaskSnapshot;
import com.alejandro.mtofield.grpc.v1.DefectSeverity;
import com.alejandro.mtofield.grpc.v1.EventResult;
import com.alejandro.mtofield.grpc.v1.InlineDefect;
import com.alejandro.mtofield.grpc.v1.TaskCompleted;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

/**
 * Le cuenta a mto-maintenance el inicio y el fin de cada tarea que un dispositivo subio, fuera del
 * hilo del stream (la cola de trabajo del dispositivo o el reintento programado), y contesta al
 * dispositivo con un {@code EventResult}. El evento ya esta persistido cuando llega aqui.
 *
 * <p>Ni {@code start} ni {@code complete} son idempotentes en mantenimiento, asi que una respuesta
 * perdida (la JVM murio con la peticion en vuelo, o el reintento repite una que si llego) vuelve
 * como un 409 {@code TRN-001}. Se reconcilia leyendo la tarea: un inicio con la tarea ya
 * {@code IN_PROGRESS} o {@code COMPLETED}, y un fin con la tarea ya {@code COMPLETED}, cuentan
 * como aplicados; cualquier otro rechazo es definitivo y no se reintenta. Mantenimiento acepta
 * completar una tarea {@code PENDING} (le pone el inicio), asi que un fin que adelanta a un inicio
 * que quedo FAILED se aplica igual, y ese inicio, al reintentarse, encuentra la tarea completada.</p>
 *
 * <p>Lo que no contesta queda FAILED hasta el siguiente intento; solo el primero avisa al
 * dispositivo con {@code PENDING_SYNC}, los reintentos contestan cuando se resuelve. Una posesion
 * que se cerro mientras tanto no tiene a quien contestar: el evento queda igualmente como quedo.</p>
 */
@Service
@ConditionalOnProperty(prefix = "app.maintenance", name = "enabled", havingValue = "true", matchIfMissing = true)
class MaintenanceEventSynchronizer implements FieldEventSynchronizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaintenanceEventSynchronizer.class);

    static final String INVALID_IDS = "order_id and task_id must be UUIDs";
    static final String NOT_A_TASK_EVENT = "not a task event";
    private static final String SEVERITY_PREFIX = "DEFECT_SEVERITY_";

    private final MaintenanceClient maintenance;
    private final FieldEventService events;
    private final FieldCommandService commands;
    private final MaintenanceProperties properties;
    private final Clock clock;
    private final FieldMetrics metrics;

    MaintenanceEventSynchronizer(MaintenanceClient maintenance, FieldEventService events, FieldCommandService commands,
                                 MaintenanceProperties properties, Clock clock, FieldMetrics metrics) {
        this.maintenance = maintenance;
        this.events = events;
        this.commands = commands;
        this.properties = properties;
        this.clock = clock;
        this.metrics = metrics;
    }

    @Override
    public Outcome process(SyncJob job) {
        EventContext context = job.context();
        TeamMessage message = context.message();
        String orderText;
        String taskText;
        if (message.hasTaskStarted()) {
            orderText = message.getTaskStarted().getOrderId();
            taskText = message.getTaskStarted().getTaskId();
        } else if (message.hasTaskCompleted()) {
            orderText = message.getTaskCompleted().getOrderId();
            taskText = message.getTaskCompleted().getTaskId();
        } else {
            return rejected(job, NOT_A_TASK_EVENT);
        }
        Optional<UUID> orderId = parseUuid(orderText);
        Optional<UUID> taskId = parseUuid(taskText);
        if (orderId.isEmpty() || taskId.isEmpty()) {
            return rejected(job, INVALID_IDS);
        }
        try {
            if (message.hasTaskStarted()) {
                start(orderId.get(), taskId.get(), context);
            } else {
                complete(orderId.get(), taskId.get(), context);
            }
            return synced(job);
        } catch (MaintenanceRejectedException rejected) {
            return rejected(job, rejected.reason());
        } catch (MaintenanceUnavailableException unavailable) {
            return failed(job, unavailable);
        }
    }

    private void start(UUID orderId, UUID taskId, EventContext context) {
        try {
            TaskSnapshot task = maintenance.startTask(orderId, taskId, context.shiftId(), context.principal().username());
            LOGGER.info("Task {} of order {} started in mto-maintenance by {} (device {}): {}", taskId, orderId, context.principal().username(),
                    context.deviceId(), task.status());
        } catch (MaintenanceRejectedException rejected) {
            if (!rejected.isTransitionConflict()) {
                throw rejected;
            }
            TaskSnapshot task = maintenance.findTask(orderId, taskId).orElseThrow(() -> rejected);
            if (!task.isInProgress() && !task.isCompleted()) {
                throw rejected;
            }
            LOGGER.info("Task {} of order {} is already {} in mto-maintenance: the start from device {} counts as applied", taskId, orderId,
                    task.status(), context.deviceId());
        }
    }

    private void complete(UUID orderId, UUID taskId, EventContext context) {
        CompleteTaskCommand command = toCommand(context.shiftId(), context.message().getTaskCompleted());
        try {
            TaskSnapshot task = maintenance.completeTask(orderId, taskId, command);
            LOGGER.info("Task {} of order {} completed in mto-maintenance from device {}: {}", taskId, orderId, context.deviceId(), task.status());
        } catch (MaintenanceRejectedException rejected) {
            if (!rejected.isTransitionConflict()) {
                throw rejected;
            }
            TaskSnapshot task = maintenance.findTask(orderId, taskId).orElseThrow(() -> rejected);
            if (!task.isCompleted()) {
                throw rejected;
            }
            LOGGER.info("Task {} of order {} is already COMPLETED in mto-maintenance: the completion from device {} counts as applied", taskId,
                    orderId, context.deviceId());
        }
    }

    static CompleteTaskCommand toCommand(UUID shiftId, TaskCompleted completed) {
        return new CompleteTaskCommand(shiftId, completed.getTaskTypeCodesList(), blankToNull(completed.getNotes()), completed.getWorkComplete(),
                completed.getDefectsList().stream().map(MaintenanceEventSynchronizer::toDefect).toList(), completed.getPhotoRefsList());
    }

    private static CompleteTaskCommand.InlineDefect toDefect(InlineDefect defect) {
        return new CompleteTaskCommand.InlineDefect(severityOf(defect.getSeverity()), blankToNull(defect.getDescription()),
                blankToNull(defect.getTechnicalNotes()), blankToNull(defect.getCorrectionType()), blankToNull(defect.getPartsReplaced()));
    }

    /** {@code DEFECT_SEVERITY_HIGH} viaja como {@code HIGH}; sin severidad no viaja nada y mantenimiento lo rechaza, que es lo correcto. */
    static String severityOf(DefectSeverity severity) {
        if (severity == null || severity == DefectSeverity.DEFECT_SEVERITY_UNSPECIFIED || severity == DefectSeverity.UNRECOGNIZED) {
            return null;
        }
        return severity.name().substring(SEVERITY_PREFIX.length());
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private Outcome synced(SyncJob job) {
        events.markSynced(job.eventId());
        metrics.recordSync(Outcome.SYNCED);
        answer(job.context(), EventResult.Outcome.APPLIED, null);
        return Outcome.SYNCED;
    }

    private Outcome rejected(SyncJob job, String reason) {
        LOGGER.warn("Event {}/{} rejected by mto-maintenance: {}", job.context().deviceId(), job.context().sequence(), reason);
        events.markRejected(job.eventId(), reason);
        metrics.recordSync(Outcome.REJECTED);
        answer(job.context(), EventResult.Outcome.REJECTED, reason);
        return Outcome.REJECTED;
    }

    private Outcome failed(SyncJob job, MaintenanceUnavailableException unavailable) {
        LOGGER.warn("Event {}/{} could not be synchronized ({}): retry after {}", job.context().deviceId(), job.context().sequence(),
                unavailable.getMessage(), properties.syncRetry().interval());
        events.markFailed(job.eventId(), unavailable.getMessage(), clock.instant().plus(properties.syncRetry().interval()));
        metrics.recordSync(Outcome.FAILED);
        if (!job.retry()) {
            answer(job.context(), EventResult.Outcome.PENDING_SYNC, null);
        }
        return Outcome.FAILED;
    }

    /** La posesion puede haberse cerrado mientras el evento esperaba: entonces no hay a quien contestar, y el evento queda como quedo. */
    private void answer(EventContext context, EventResult.Outcome outcome, String reason) {
        try {
            commands.issue(context.possessionId(), CommandDraft.eventResult(context.shiftId(), context.sequence(), outcome, reason),
                    AuditActorResolver.SYSTEM_ACTOR);
        } catch (BusinessException noLongerOpen) {
            LOGGER.info("No EventResult for event {}/{}: possession {} is no longer open ({})", context.deviceId(), context.sequence(),
                    context.possessionId(), noLongerOpen.getReason());
        }
    }

    private static Optional<UUID> parseUuid(String value) {
        try {
            return Optional.of(UUID.fromString(value));
        } catch (IllegalArgumentException invalid) {
            return Optional.empty();
        }
    }
}
