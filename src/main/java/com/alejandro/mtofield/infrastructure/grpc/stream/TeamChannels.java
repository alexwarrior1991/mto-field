package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.application.dto.DevicePrincipal;
import com.alejandro.mtofield.application.dto.EventContext;
import com.alejandro.mtofield.application.dto.ShiftMembership;
import com.alejandro.mtofield.application.dto.StoredCommand;
import com.alejandro.mtofield.application.dto.StoredEvent;
import com.alejandro.mtofield.application.dto.SyncJob;
import com.alejandro.mtofield.application.exception.BusinessException;
import com.alejandro.mtofield.application.exception.PossessionNotOpenException;
import com.alejandro.mtofield.application.mapper.ProtoTimestamps;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.application.service.FieldEventService;
import com.alejandro.mtofield.application.service.LivenessRegistry;
import com.alejandro.mtofield.application.service.PossessionService;
import com.alejandro.mtofield.configuration.grpc.FieldProperties;
import com.alejandro.mtofield.configuration.security.CurrentUserService;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.grpc.v1.Heartbeat;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.alejandro.mtofield.grpc.v1.Welcome;
import com.alejandro.mtofield.infrastructure.grpc.GrpcErrors;
import com.alejandro.mtofield.infrastructure.grpc.metrics.FieldMetrics;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * Abre un {@code TeamChannel}: devuelve el observador de lo que sube el dispositivo y, con el
 * primer {@code Join}, crea y registra su {@link DeviceStream}.
 *
 * <p>Antes de devolver el observador quedan puestos los manejadores del transporte
 * ({@code onReady}, {@code onCancel}, {@code onClose}) y el control de flujo manual: un
 * {@code request(1)} por mensaje procesado, y solo cuando la cola de trabajo lo acepto. El
 * principal y la caducidad del token se capturan aqui, en el unico hilo que los tiene.</p>
 *
 * <p>Un error de negocio en un mensaje se contesta en banda ({@code EventResult{REJECTED}}); lo
 * unico que cierra el stream con un {@code Status} es no empezar por un {@code Join} valido, que
 * la posesion se cierre, que el dispositivo no lea o que su cola de trabajo se desborde.</p>
 */
@Component
public class TeamChannels {

    private static final Logger LOGGER = LoggerFactory.getLogger(TeamChannels.class);

    public static final String REASON_JOIN_REQUIRED = "JOIN_REQUIRED";
    public static final String REASON_SHIFT_NOT_IN_OPEN_POSSESSION = "SHIFT_NOT_IN_OPEN_POSSESSION";
    public static final String REASON_POSSESSION_CLOSED = "POSSESSION_CLOSED";
    public static final String REASON_WORK_QUEUE_FULL = "WORK_QUEUE_FULL";
    public static final String REASON_ALREADY_JOINED = "already joined";
    public static final String REASON_EMPTY_MESSAGE = "empty message";

    private final PossessionService possessions;
    private final FieldCommandService commands;
    private final FieldEventService events;
    private final DeviceStreamRegistry registry;
    private final DeviceWorkQueues workQueues;
    private final LivenessRegistry liveness;
    private final CatchUpProbe probe;
    private final FieldProperties properties;
    private final FieldMetrics metrics;
    private final CurrentUserService currentUser;
    private final ObservationRegistry observations;
    private final ExecutorService executor;
    private final Clock clock;

    public TeamChannels(PossessionService possessions, FieldCommandService commands, FieldEventService events, DeviceStreamRegistry registry,
                        DeviceWorkQueues workQueues, LivenessRegistry liveness, CatchUpProbe probe, FieldProperties properties,
                        FieldMetrics metrics, CurrentUserService currentUser, ObservationRegistry observations,
                        @Qualifier("fieldStreamExecutor") ExecutorService executor, Clock clock) {
        this.possessions = possessions;
        this.commands = commands;
        this.events = events;
        this.registry = registry;
        this.workQueues = workQueues;
        this.liveness = liveness;
        this.probe = probe;
        this.properties = properties;
        this.metrics = metrics;
        this.currentUser = currentUser;
        this.observations = observations;
        this.executor = executor;
        this.clock = clock;
    }

    public StreamObserver<TeamMessage> open(StreamObserver<FieldCommand> responseObserver) {
        ServerCallStreamObserver<FieldCommand> out = (ServerCallStreamObserver<FieldCommand>) responseObserver;
        DevicePrincipal principal = new DevicePrincipal(currentUser.getUsername().orElse("unknown"), currentUser.getUserId().orElse(null));
        Instant tokenExpiresAt = currentUser.getTokenExpiresAt().orElse(null);
        Session session = new Session(out, principal, tokenExpiresAt);
        out.disableAutoRequest();
        out.setOnReadyHandler(session::onReady);
        out.setOnCancelHandler(session::onCancelled);
        out.request(1);
        return session;
    }

    private enum State { AWAITING_JOIN, ACTIVE, DONE }

    private final class Session implements StreamObserver<TeamMessage> {

        private final ServerCallStreamObserver<FieldCommand> out;
        private final DevicePrincipal principal;
        private final Instant tokenExpiresAt;
        private volatile State state = State.AWAITING_JOIN;
        private volatile DeviceStream stream;

        private Session(ServerCallStreamObserver<FieldCommand> out, DevicePrincipal principal, Instant tokenExpiresAt) {
            this.out = out;
            this.principal = principal;
            this.tokenExpiresAt = tokenExpiresAt;
        }

        @Override
        public void onNext(TeamMessage message) {
            switch (state) {
                case AWAITING_JOIN -> join(message);
                case ACTIVE -> handle(message);
                case DONE -> LOGGER.debug("Message after the end of the channel ignored ({})", message.getEventCase());
            }
        }

        private void join(TeamMessage message) {
            if (!message.hasJoin() || message.getDeviceId().isBlank()) {
                closeBeforeJoin(GrpcErrors.of(Status.Code.FAILED_PRECONDITION, REASON_JOIN_REQUIRED,
                        "the first message of a TeamChannel must be a Join with a device_id"));
                return;
            }
            UUID shiftId;
            try {
                shiftId = UUID.fromString(message.getJoin().getShiftId());
            } catch (IllegalArgumentException invalid) {
                closeBeforeJoin(GrpcErrors.of(Status.Code.INVALID_ARGUMENT, "INVALID_SHIFT_ID", "Join.shift_id is not a UUID"));
                return;
            }
            Optional<ShiftMembership> membership = possessions.membershipOfOpenPossession(shiftId);
            if (membership.isEmpty()) {
                closeBeforeJoin(GrpcErrors.of(Status.Code.FAILED_PRECONDITION, REASON_SHIFT_NOT_IN_OPEN_POSSESSION,
                        "shift " + shiftId + " is not in an open possession"));
                return;
            }
            ShiftMembership shift = membership.get();
            String deviceId = message.getDeviceId();
            DeviceStream created = new DeviceStream(deviceId, shift.shiftId(), shift.possessionId(), shift.teamCode(), principal, tokenExpiresAt, out,
                    properties.outboundQueueCapacity(), properties.heldCapacity(), properties.catchUpPageSize(), properties.catchUpPutTimeout(),
                    metrics, this::streamClosed);
            stream = created;
            state = State.ACTIVE;
            // Registrado ANTES de leer el atraso: lo que se confirme entre medias llega por el carril y se retiene.
            DeviceStream previous = registry.register(created);
            if (previous != null) {
                LOGGER.info("Device {} opened a second stream: the first one is superseded", deviceId);
                previous.supersede();
            }
            liveness.streamOpened(deviceId, shift.shiftId(), shift.possessionId());
            long watermark = events.contiguousWatermark(deviceId);
            FieldCommand welcome = FieldCommand.newBuilder()
                    .setCommandId(UUID.randomUUID().toString())
                    .setSequence(0)
                    .setIssuedAt(ProtoTimestamps.toProto(clock.instant()))
                    .setRequiresAck(false)
                    .setWelcome(Welcome.newBuilder()
                            .setPossessionId(shift.possessionId().toString())
                            .setServerTime(ProtoTimestamps.toProto(clock.instant()))
                            .setEndsAt(ProtoTimestamps.toProto(shift.endsAt()))
                            .setLastAppliedSequence(watermark))
                    .build();
            long after = message.getJoin().getLastCommandSequence();
            ReplaySource replay = (sequence, limit) -> commands.replayAfter(shift.possessionId(), shift.shiftId(), sequence, limit).stream()
                    .map(StoredCommand::command)
                    .toList();
            LOGGER.info("Device {} of team {} joined possession {} (last applied {}, resuming after #{})", deviceId, shift.teamCode(),
                    shift.possessionId(), watermark, after);
            try {
                executor.execute(() -> created.catchUp(after, welcome, replay, probe));
            } catch (RejectedExecutionException shuttingDown) {
                created.fail(GrpcErrors.of(Status.Code.UNAVAILABLE, "SHUTTING_DOWN", "server is shutting down"));
                return;
            }
            out.request(1);
        }

        private void handle(TeamMessage message) {
            DeviceStream current = stream;
            if (current == null || current.isClosed()) {
                return;
            }
            String kind = message.getEventCase().name().toLowerCase();
            Observation.createNotStarted("field.event", observations)
                    .lowCardinalityKeyValue("kind", kind)
                    .observe(() -> handleObserved(current, message));
        }

        private void handleObserved(DeviceStream current, TeamMessage message) {
            liveness.touch(current.deviceId());
            EventContext context = new EventContext(current.possessionId(), current.shiftId(), current.deviceId(), principal, message);
            try {
                switch (message.getEventCase()) {
                    case HEARTBEAT -> {
                        Heartbeat heartbeat = message.getHeartbeat();
                        liveness.heartbeat(current.deviceId(), heartbeat.getKp(), heartbeat.getBatteryPct(), heartbeat.getSignalDbm());
                    }
                    case COMMAND_ACK -> events.recordAck(context);
                    case CLEAR_OF_TRACK -> events.recordClearOfTrack(context);
                    case TASK_STARTED, TASK_COMPLETED -> {
                        StoredEvent stored = events.recordTaskEvent(context);
                        if (stored.inserted() && !workQueues.submit(current.deviceId(), new SyncJob(stored.id(), context))) {
                            // El evento ya esta persistido y la marca de agua evita que se reenvie.
                            current.fail(GrpcErrors.of(Status.Code.RESOURCE_EXHAUSTED, REASON_WORK_QUEUE_FULL,
                                    "device " + current.deviceId() + " has too many task events waiting to be synchronized"));
                            return;
                        }
                    }
                    case JOIN -> events.rejectInline(context, REASON_ALREADY_JOINED);
                    case EVENT_NOT_SET -> events.rejectInline(context, REASON_EMPTY_MESSAGE);
                }
            } catch (PossessionNotOpenException closed) {
                current.fail(GrpcErrors.of(Status.Code.FAILED_PRECONDITION, REASON_POSSESSION_CLOSED, "possession " + current.possessionId() + " is closed"));
                return;
            } catch (BusinessException rejected) {
                rejectQuietly(current, context, rejected.getReason());
            } catch (RuntimeException unexpected) {
                LOGGER.error("Message {} of device {} could not be processed", message.getEventCase(), current.deviceId(), unexpected);
                rejectQuietly(current, context, "internal error");
            }
            if (!current.isClosed()) {
                out.request(1);
            }
        }

        private void rejectQuietly(DeviceStream current, EventContext context, String reason) {
            try {
                events.rejectInline(context, reason);
            } catch (RuntimeException unanswerable) {
                LOGGER.warn("Device {} could not be answered for message #{}: {}", current.deviceId(), context.sequence(), unanswerable.toString());
            }
        }

        /** El unico sitio donde se escribe en {@code out} fuera del stream: aun no existe, asi que no hay escritor concurrente. */
        private void closeBeforeJoin(StatusRuntimeException error) {
            state = State.DONE;
            try {
                out.onError(error);
            } catch (RuntimeException gone) {
                LOGGER.debug("Call gone before the Join could be refused: {}", gone.toString());
            }
        }

        @Override
        public void onError(Throwable throwable) {
            // El cliente cancelo o la red cayo: la llamada ya no admite escrituras.
            LOGGER.info("TeamChannel{} ended from the client side: {}", stream == null ? "" : " of device " + stream.deviceId(),
                    Status.fromThrowable(throwable).getCode());
            abandon();
        }

        @Override
        public void onCompleted() {
            // El dispositivo se despide: fin normal.
            state = State.DONE;
            DeviceStream current = stream;
            if (current != null) {
                current.complete();
            } else {
                try {
                    out.onCompleted();
                } catch (RuntimeException gone) {
                    LOGGER.debug("Call gone before completing: {}", gone.toString());
                }
            }
        }

        private void onReady() {
            DeviceStream current = stream;
            if (current != null) {
                current.drain();
            }
        }

        private void onCancelled() {
            abandon();
        }

        private void abandon() {
            state = State.DONE;
            DeviceStream current = stream;
            if (current != null) {
                current.abandon();
            }
        }

        private void streamClosed(DeviceStream closed) {
            state = State.DONE;
            registry.unregister(closed);
            liveness.streamClosed(closed.deviceId());
        }
    }
}
