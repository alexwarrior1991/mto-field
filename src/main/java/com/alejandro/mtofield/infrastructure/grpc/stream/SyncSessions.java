package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.application.dto.DevicePrincipal;
import com.alejandro.mtofield.application.dto.EventContext;
import com.alejandro.mtofield.application.dto.ShiftMembership;
import com.alejandro.mtofield.application.dto.StoredEvent;
import com.alejandro.mtofield.application.dto.SyncJob;
import com.alejandro.mtofield.application.exception.BusinessException;
import com.alejandro.mtofield.application.exception.PossessionNotOpenException;
import com.alejandro.mtofield.application.service.FieldEventService;
import com.alejandro.mtofield.application.service.PossessionService;
import com.alejandro.mtofield.configuration.metrics.FieldMetrics;
import com.alejandro.mtofield.configuration.security.CurrentUserService;
import com.alejandro.mtofield.grpc.v1.SyncResult;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.alejandro.mtofield.infrastructure.grpc.GrpcErrors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;

/**
 * Abre un {@code SyncBufferedEvents}: el atraso que un dispositivo acumulo sin cobertura, subido
 * fuera del canal vivo y en orden. Es la otra mitad de la regla de protocolo que
 * {@link TeamChannels} hace cumplir: mientras hay atraso, los eventos de trabajo van solo por
 * aqui; por el {@code TeamChannel} siguen yendo latidos, acuses y salidas de via.
 *
 * <p>El primer mensaje es un {@code Join} con el turno (identifica la posesion abierta, como en
 * el canal vivo). Los siguientes se procesan uno a uno, con el mismo servicio que el canal vivo:
 * un acuse o una salida de via se aplican en linea, un evento de tarea se guarda y se encola a la
 * cola de trabajo del dispositivo. Cada uno recibe su {@code EventResult} por el registro de
 * ordenes (en el {@code TeamChannel} si esta abierto, o al reanudar), asi que el resultado final
 * de una tarea no viaja en el {@code SyncResult}: este solo cuenta cuantos se guardaron ahora,
 * cuantos ya estaban y cuantos no se guardaron, y devuelve la marca de agua contigua.</p>
 *
 * <p>El stream es un registro: las secuencias tienen que ir estrictamente crecientes. Una
 * repetida o menor que la anterior del mismo stream cierra la llamada con
 * {@code INVALID_ARGUMENT OUT_OF_ORDER}; un hueco no es un error aqui (es el canal del atraso).
 * Control de flujo manual, como en el canal vivo: {@code request(1)} por mensaje procesado.</p>
 */
@Component
public class SyncSessions {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyncSessions.class);

    public static final String REASON_OUT_OF_ORDER = "OUT_OF_ORDER";
    static final String OUTCOME_APPLIED = "applied";
    static final String OUTCOME_DUPLICATE = "duplicate";
    static final String OUTCOME_REJECTED = "rejected";

    private final PossessionService possessions;
    private final FieldEventService events;
    private final DeviceWorkQueues workQueues;
    private final CurrentUserService currentUser;
    private final TeamBinding teamBinding;
    private final ObservationRegistry observations;
    private final FieldMetrics metrics;

    public SyncSessions(PossessionService possessions, FieldEventService events, DeviceWorkQueues workQueues, CurrentUserService currentUser,
                        TeamBinding teamBinding, ObservationRegistry observations, FieldMetrics metrics) {
        this.possessions = possessions;
        this.events = events;
        this.workQueues = workQueues;
        this.currentUser = currentUser;
        this.teamBinding = teamBinding;
        this.observations = observations;
        this.metrics = metrics;
    }

    public StreamObserver<TeamMessage> open(StreamObserver<SyncResult> responseObserver) {
        ServerCallStreamObserver<SyncResult> out = (ServerCallStreamObserver<SyncResult>) responseObserver;
        DevicePrincipal principal = new DevicePrincipal(currentUser.getUsername().orElse("unknown"), currentUser.getUserId().orElse(null));
        Session session = new Session(out, principal, teamBinding.capture());
        out.disableAutoRequest();
        out.setOnCancelHandler(session::onCancelled);
        out.request(1);
        return session;
    }

    private enum State { AWAITING_JOIN, ACTIVE, DONE }

    /**
     * Solo el hilo de los callbacks escribe en {@code out} (gRPC los serializa); el manejador de
     * cancelacion no escribe. No hace falta el CAS del canal vivo.
     */
    private final class Session implements StreamObserver<TeamMessage> {

        private final ServerCallStreamObserver<SyncResult> out;
        private final DevicePrincipal principal;
        private final TeamBinding.Membership membership;
        private volatile State state = State.AWAITING_JOIN;
        private String deviceId;
        private ShiftMembership shift;
        private long lastSequence;
        private int applied;
        private int duplicates;
        private int rejected;

        private Session(ServerCallStreamObserver<SyncResult> out, DevicePrincipal principal, TeamBinding.Membership membership) {
            this.out = out;
            this.principal = principal;
            this.membership = membership;
        }

        @Override
        public void onNext(TeamMessage message) {
            switch (state) {
                case AWAITING_JOIN -> join(message);
                case ACTIVE -> handle(message);
                case DONE -> LOGGER.debug("Message after the end of the sync ignored ({})", message.getEventCase());
            }
        }

        private void join(TeamMessage message) {
            if (!message.hasJoin() || message.getDeviceId().isBlank()) {
                fail(GrpcErrors.of(Status.Code.FAILED_PRECONDITION, TeamChannels.REASON_JOIN_REQUIRED,
                        "the first message of a SyncBufferedEvents must be a Join with a device_id"));
                return;
            }
            UUID shiftId;
            try {
                shiftId = UUID.fromString(message.getJoin().getShiftId());
            } catch (IllegalArgumentException invalid) {
                fail(GrpcErrors.of(Status.Code.INVALID_ARGUMENT, "INVALID_SHIFT_ID", "Join.shift_id is not a UUID"));
                return;
            }
            Optional<ShiftMembership> openShift = possessions.membershipOfOpenPossession(shiftId);
            if (openShift.isEmpty()) {
                fail(GrpcErrors.of(Status.Code.FAILED_PRECONDITION, TeamChannels.REASON_SHIFT_NOT_IN_OPEN_POSSESSION,
                        "shift " + shiftId + " is not in an open possession"));
                return;
            }
            if (!teamBinding.allows(membership, openShift.get().teamCode())) {
                fail(teamBinding.refusal(membership, openShift.get().teamCode()));
                return;
            }
            shift = openShift.get();
            deviceId = message.getDeviceId();
            state = State.ACTIVE;
            LOGGER.info("Device {} of team {} uploads its backlog for possession {}", deviceId, shift.teamCode(), shift.possessionId());
            out.request(1);
        }

        private void handle(TeamMessage message) {
            String kind = message.getEventCase().name().toLowerCase();
            Observation.createNotStarted("field.event", observations)
                    .lowCardinalityKeyValue("kind", kind)
                    .lowCardinalityKeyValue("channel", "sync")
                    .observe(() -> handleObserved(message));
        }

        private void handleObserved(TeamMessage message) {
            if (message.hasHeartbeat()) {
                // Un latido guardado en el atraso no dice nada del presente.
                out.request(1);
                return;
            }
            if (message.getSequence() <= lastSequence) {
                fail(GrpcErrors.of(Status.Code.INVALID_ARGUMENT, REASON_OUT_OF_ORDER,
                        "upload #" + message.getSequence() + " after #" + lastSequence + ": the backlog must be uploaded in increasing order"));
                return;
            }
            lastSequence = message.getSequence();
            EventContext context = new EventContext(shift.possessionId(), shift.shiftId(), deviceId, principal, message);
            try {
                switch (message.getEventCase()) {
                    case COMMAND_ACK -> count(events.recordAck(context));
                    case CLEAR_OF_TRACK -> count(events.recordClearOfTrack(context));
                    case TASK_STARTED, TASK_COMPLETED -> {
                        StoredEvent stored = events.recordTaskEvent(context);
                        count(stored);
                        if (stored.inserted() && !workQueues.submit(deviceId, SyncJob.first(stored.id(), context))) {
                            // El evento ya esta persistido y la marca de agua evita que se reenvie.
                            fail(GrpcErrors.of(Status.Code.RESOURCE_EXHAUSTED, TeamChannels.REASON_WORK_QUEUE_FULL,
                                    "device " + deviceId + " has too many task events waiting to be synchronized"));
                            return;
                        }
                    }
                    case JOIN -> rejectQuietly(context, TeamChannels.REASON_ALREADY_JOINED);
                    case EVENT_NOT_SET -> rejectQuietly(context, TeamChannels.REASON_EMPTY_MESSAGE);
                    case HEARTBEAT -> { }
                }
            } catch (PossessionNotOpenException closed) {
                fail(GrpcErrors.of(Status.Code.FAILED_PRECONDITION, TeamChannels.REASON_POSSESSION_CLOSED,
                        "possession " + shift.possessionId() + " is closed"));
                return;
            } catch (BusinessException refused) {
                rejectQuietly(context, refused.getReason());
            } catch (RuntimeException unexpected) {
                LOGGER.error("Buffered message {} of device {} could not be processed", message.getEventCase(), deviceId, unexpected);
                rejectQuietly(context, "internal error");
            }
            if (state != State.DONE) {
                out.request(1);
            }
        }

        private void count(StoredEvent stored) {
            if (stored.id() == null) {
                rejected++;
            } else if (stored.inserted()) {
                applied++;
            } else {
                duplicates++;
            }
        }

        /** Un rechazo de negocio se cuenta y, como en el canal vivo, se contesta por el registro de ordenes. */
        private void rejectQuietly(EventContext context, String reason) {
            rejected++;
            try {
                events.rejectInline(context, reason);
            } catch (RuntimeException unanswerable) {
                LOGGER.warn("Device {} could not be answered for buffered message #{}: {}", deviceId, context.sequence(), unanswerable.toString());
            }
        }

        @Override
        public void onCompleted() {
            if (state == State.DONE) {
                return;
            }
            if (state == State.AWAITING_JOIN) {
                fail(GrpcErrors.of(Status.Code.FAILED_PRECONDITION, TeamChannels.REASON_JOIN_REQUIRED,
                        "the first message of a SyncBufferedEvents must be a Join with a device_id"));
                return;
            }
            state = State.DONE;
            long watermark = events.contiguousWatermark(deviceId);
            metrics.recordBufferedSync(OUTCOME_APPLIED, applied);
            metrics.recordBufferedSync(OUTCOME_DUPLICATE, duplicates);
            metrics.recordBufferedSync(OUTCOME_REJECTED, rejected);
            LOGGER.info("Device {} uploaded its backlog: {} applied, {} duplicate(s), {} rejected; contiguous watermark #{}", deviceId, applied,
                    duplicates, rejected, watermark);
            try {
                out.onNext(SyncResult.newBuilder()
                        .setLastAppliedSequence(watermark)
                        .setApplied(applied)
                        .setDuplicates(duplicates)
                        .setRejected(rejected)
                        .build());
                out.onCompleted();
            } catch (RuntimeException gone) {
                LOGGER.debug("Sync of device {} gone before its result could be sent: {}", deviceId, gone.toString());
            }
        }

        @Override
        public void onError(Throwable throwable) {
            // El cliente cancelo o la red cayo: lo ya guardado se queda, y la marca de agua lo dira al reanudar.
            LOGGER.info("SyncBufferedEvents{} ended from the client side after {} message(s): {}", deviceId == null ? "" : " of device " + deviceId,
                    applied + duplicates + rejected, Status.fromThrowable(throwable).getCode());
            state = State.DONE;
        }

        private void onCancelled() {
            state = State.DONE;
        }

        private void fail(StatusRuntimeException error) {
            state = State.DONE;
            try {
                out.onError(error);
            } catch (RuntimeException gone) {
                LOGGER.debug("Sync call gone before it could be refused: {}", gone.toString());
            }
        }
    }
}
