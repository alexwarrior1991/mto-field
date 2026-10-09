package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.application.dto.SyncJob;
import com.alejandro.mtofield.application.service.FieldEventService;
import com.alejandro.mtofield.application.service.FieldEventSynchronizer;
import com.alejandro.mtofield.configuration.grpc.FieldProperties;
import com.alejandro.mtofield.configuration.maintenance.MaintenanceProperties;
import com.alejandro.mtofield.infrastructure.grpc.metrics.FieldMetrics;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Una cola de trabajo acotada por dispositivo para lo que habla con mto-maintenance
 * ({@code TaskStarted} / {@code TaskCompleted}), con un hilo virtual consumidor que se retira
 * cuando lleva un rato sin nada que hacer.
 *
 * <p>El consumidor se arranca con {@code Thread.ofVirtual()}: hereda ni el {@code Context} de
 * gRPC ni el {@code SecurityContext} del callback, a proposito. La sincronizacion no debe morir
 * con la cancelacion del stream (el evento ya esta persistido y hay que contarlo), y quien hizo
 * que viaja en el propio evento ({@code reported_by}).</p>
 *
 * <p>La retirada es atomica frente a un {@code submit} concurrente: las dos cosas pasan dentro
 * de {@code compute} sobre la misma clave, asi que un trabajo nunca entra en la cola de un hilo
 * que ya decidio irse.</p>
 */
@Component
public class DeviceWorkQueues {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceWorkQueues.class);

    private final FieldEventSynchronizer synchronizer;
    private final FieldEventService events;
    private final Clock clock;
    private final int capacity;
    private final Duration idleTimeout;
    private final Duration retryDelay;
    private final Map<String, Worker> workers = new ConcurrentHashMap<>();

    @Autowired
    public DeviceWorkQueues(FieldEventSynchronizer synchronizer, FieldEventService events, FieldProperties properties,
                            MaintenanceProperties maintenance, Clock clock, FieldMetrics metrics) {
        this(synchronizer, events, properties.workQueueCapacity(), properties.workQueueIdleTimeout(), maintenance.syncRetry().interval(), clock, metrics);
    }

    DeviceWorkQueues(FieldEventSynchronizer synchronizer, FieldEventService events, int capacity, Duration idleTimeout, Duration retryDelay,
                     Clock clock, FieldMetrics metrics) {
        this.synchronizer = synchronizer;
        this.events = events;
        this.capacity = capacity;
        this.idleTimeout = idleTimeout;
        this.retryDelay = retryDelay;
        this.clock = clock;
        metrics.gauge(FieldMetrics.WORK_QUEUE_DEPTH, "Task events waiting to be synchronized, over every device",
                () -> workers.values().stream().mapToInt(worker -> worker.queue.size()).sum());
    }

    /**
     * Encola el trabajo del dispositivo, arrancando su consumidor si no lo tiene.
     *
     * @return {@code false} si la cola esta llena: el evento sigue persistido y quien llama cierra
     * el stream con RESOURCE_EXHAUSTED; la marca de agua evita que se reenvie
     */
    public boolean submit(String deviceId, SyncJob job) {
        boolean[] accepted = new boolean[1];
        workers.compute(deviceId, (id, current) -> {
            Worker worker = current != null ? current : start(id);
            accepted[0] = worker.queue.offer(job);
            return worker;
        });
        return accepted[0];
    }

    public int pending(String deviceId) {
        Worker worker = workers.get(deviceId);
        return worker == null ? 0 : worker.queue.size();
    }

    int activeWorkers() {
        return workers.size();
    }

    private Worker start(String deviceId) {
        Worker worker = new Worker(deviceId);
        worker.thread = Thread.ofVirtual().name("field-work-" + deviceId).start(worker);
        return worker;
    }

    private void process(SyncJob job) {
        try {
            synchronizer.process(job);
        } catch (RuntimeException failure) {
            LOGGER.warn("Synchronization of event {} of device {} failed: {}", job.eventId(), job.context().deviceId(), failure.toString());
            try {
                events.markFailed(job.eventId(), failure.toString(), clock.instant().plus(retryDelay));
            } catch (RuntimeException unrecorded) {
                LOGGER.error("Could not record the failure of event {}", job.eventId(), unrecorded);
            }
        }
    }

    @PreDestroy
    void shutdown() {
        for (Worker worker : workers.values()) {
            worker.thread.interrupt();
        }
    }

    private final class Worker implements Runnable {

        private final String deviceId;
        private final LinkedBlockingQueue<SyncJob> queue = new LinkedBlockingQueue<>(capacity);
        private volatile Thread thread;

        private Worker(String deviceId) {
            this.deviceId = deviceId;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    SyncJob job = queue.poll(idleTimeout.toMillis(), TimeUnit.MILLISECONDS);
                    if (job != null) {
                        process(job);
                    } else if (retire()) {
                        return;
                    }
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                workers.remove(deviceId, this);
                if (!queue.isEmpty()) {
                    LOGGER.info("Work queue of device {} interrupted with {} job(s) pending; they stay PENDING in the database", deviceId, queue.size());
                }
            }
        }

        /** Se va solo si, bajo el candado de la clave, sigue sin tener nada que hacer. */
        private boolean retire() {
            return workers.computeIfPresent(deviceId, (id, current) -> current == this && queue.isEmpty() ? null : current) == null;
        }
    }
}
