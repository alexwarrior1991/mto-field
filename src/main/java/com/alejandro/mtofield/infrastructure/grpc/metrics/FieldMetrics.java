package com.alejandro.mtofield.infrastructure.grpc.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Las metricas del canal de campo, con sus nombres en un solo sitio. Los gauges los registran
 * las piezas que tienen el dato (el registro de streams, las colas de trabajo, el tablero) al
 * construirse; los tiempos los apuntan quienes los miden.
 */
@Component
public class FieldMetrics {

    public static final String STREAMS_OPEN = "field.streams.open";
    public static final String OUTBOUND_DEPTH = "field.stream.outbound.depth";
    public static final String NOT_READY = "field.stream.not_ready";
    public static final String WORK_QUEUE_DEPTH = "field.work_queue.depth";
    public static final String TEAMS_CONNECTED = "field.teams.connected";
    public static final String COMMANDS_PENDING_ACK = "field.commands.pending_ack";
    public static final String ACK_TIME = "field.command.ack.time";

    private final MeterRegistry registry;
    private final Timer notReady;
    private final Timer ackTime;

    public FieldMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.notReady = Timer.builder(NOT_READY)
                .description("Time a device stream spent with commands waiting and the transport not ready")
                .register(registry);
        this.ackTime = Timer.builder(ACK_TIME)
                .description("Time from a command requiring acknowledgement being issued to a team acknowledging it")
                .register(registry);
    }

    public void recordNotReady(Duration duration) {
        notReady.record(duration);
    }

    public void recordAckTime(Duration duration) {
        if (!duration.isNegative()) {
            ackTime.record(duration);
        }
    }

    public void gauge(String name, String description, Supplier<Number> value) {
        Gauge.builder(name, value).description(description).register(registry);
    }

    public MeterRegistry registry() {
        return registry;
    }
}
