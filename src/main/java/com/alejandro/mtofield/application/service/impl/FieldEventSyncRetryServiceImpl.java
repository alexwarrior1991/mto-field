package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.SyncJob;
import com.alejandro.mtofield.application.service.FieldEventService;
import com.alejandro.mtofield.application.service.FieldEventSyncRetryService;
import com.alejandro.mtofield.application.service.FieldEventSynchronizer;
import com.alejandro.mtofield.configuration.maintenance.MaintenanceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.List;

/**
 * Una pasada del reintento: los eventos de tarea vencidos, en orden de llegada y por lotes, por el
 * mismo sincronizador que la cola de trabajo. Se para en el primero que mantenimiento no contesta
 * (los que siguen esperarian lo mismo, y el orden entre el inicio y el fin de una tarea se conserva
 * para la pasada siguiente), como el reintento de stock de mto-maintenance.
 */
@Service
class FieldEventSyncRetryServiceImpl implements FieldEventSyncRetryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldEventSyncRetryServiceImpl.class);

    static final int BATCH_SIZE = 100;

    private final FieldEventService events;
    private final FieldEventSynchronizer synchronizer;
    private final MaintenanceProperties properties;
    private final Clock clock;

    FieldEventSyncRetryServiceImpl(FieldEventService events, FieldEventSynchronizer synchronizer, MaintenanceProperties properties, Clock clock) {
        this.events = events;
        this.synchronizer = synchronizer;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public int retryDue() {
        int resolved = 0;
        List<SyncJob> due;
        do {
            due = events.dueForRetry(BATCH_SIZE);
            for (SyncJob job : due) {
                FieldEventSynchronizer.Outcome outcome;
                try {
                    outcome = synchronizer.process(job);
                } catch (RuntimeException failure) {
                    LOGGER.warn("Retry of event {} of device {} failed: {}", job.eventId(), job.context().deviceId(), failure.toString());
                    events.markFailed(job.eventId(), failure.toString(), clock.instant().plus(properties.syncRetry().interval()));
                    return resolved;
                }
                switch (outcome) {
                    case SYNCED, REJECTED -> resolved++;
                    case FAILED, PENDING -> {
                        LOGGER.info("Retry stopped at event {} of device {} ({}); {} resolved in this pass", job.eventId(),
                                job.context().deviceId(), outcome, resolved);
                        return resolved;
                    }
                }
            }
        } while (due.size() == BATCH_SIZE);
        return resolved;
    }
}
