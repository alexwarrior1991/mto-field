package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.CompleteTaskCommand;
import com.alejandro.mtofield.application.service.MaintenanceClient;
import com.alejandro.mtofield.domain.model.ShiftSnapshot;
import com.alejandro.mtofield.domain.model.TaskSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Cliente de mantenimiento apagado ({@code app.maintenance.enabled=false}): cualquier id de turno
 * existe y esta en curso, con un equipo sintetico determinista ({@code T-} y los cuatro primeros
 * caracteres del id), la fecha de hoy y un fin previsto a ocho horas. Es lo que usan los tests y
 * el simulador mientras no hay mto-maintenance delante.
 */
@Component
@ConditionalOnProperty(prefix = "app.maintenance", name = "enabled", havingValue = "false", matchIfMissing = false)
class NoOpMaintenanceClient implements MaintenanceClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NoOpMaintenanceClient.class);

    static final String SYNTHETIC_STATUS = "IN_PROGRESS";

    private final Clock clock;

    NoOpMaintenanceClient(Clock clock) {
        this.clock = clock;
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public Optional<ShiftSnapshot> findShift(UUID shiftId) {
        LOGGER.debug("Maintenance client disabled: synthetic shift for {}", shiftId);
        String teamCode = "T-" + shiftId.toString().substring(0, 4).toUpperCase();
        Instant now = clock.instant();
        return Optional.of(new ShiftSnapshot(
                shiftId,
                "SH-" + shiftId.toString().substring(0, 8),
                LocalDate.ofInstant(now, ZoneOffset.UTC),
                SYNTHETIC_STATUS,
                teamCode,
                "Team " + teamCode,
                now.truncatedTo(ChronoUnit.SECONDS),
                now.plus(8, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS),
                List.of()
        ));
    }

    @Override
    public Optional<TaskSnapshot> findTask(UUID orderId, UUID taskId) {
        LOGGER.debug("Maintenance client disabled: task lookup skipped ({}/{})", orderId, taskId);
        return Optional.empty();
    }

    @Override
    public TaskSnapshot startTask(UUID orderId, UUID taskId, UUID shiftId, String assignedUser) {
        throw new UnsupportedOperationException("maintenance client is disabled");
    }

    @Override
    public TaskSnapshot completeTask(UUID orderId, UUID taskId, CompleteTaskCommand command) {
        throw new UnsupportedOperationException("maintenance client is disabled");
    }
}
