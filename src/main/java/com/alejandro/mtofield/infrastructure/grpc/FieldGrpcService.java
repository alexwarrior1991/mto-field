package com.alejandro.mtofield.infrastructure.grpc;

import com.alejandro.mtofield.application.dto.CommandDraft;
import com.alejandro.mtofield.application.dto.IssuedCommand;
import com.alejandro.mtofield.application.dto.PossessionView;
import com.alejandro.mtofield.application.exception.InvalidPossessionRequestException;
import com.alejandro.mtofield.application.mapper.ProtoTimestamps;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.application.service.PossessionService;
import com.alejandro.mtofield.configuration.security.CurrentUserService;
import com.alejandro.mtofield.configuration.security.SecurityRoles;
import com.alejandro.mtofield.grpc.v1.ClosePossessionRequest;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.grpc.v1.FieldServiceGrpc;
import com.alejandro.mtofield.grpc.v1.IssueCommandRequest;
import com.alejandro.mtofield.grpc.v1.IssueCommandResponse;
import com.alejandro.mtofield.grpc.v1.OpenPossessionRequest;
import com.alejandro.mtofield.grpc.v1.Possession;
import com.alejandro.mtofield.grpc.v1.PossessionBoard;
import com.alejandro.mtofield.grpc.v1.SyncResult;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.alejandro.mtofield.grpc.v1.WatchPossessionBoardRequest;
import com.alejandro.mtofield.infrastructure.grpc.mapper.FieldProtoMapper;
import com.alejandro.mtofield.infrastructure.grpc.stream.TeamChannels;
import io.grpc.stub.StreamObserver;
import org.springframework.grpc.server.service.GrpcService;
import org.springframework.security.access.prepost.PreAuthorize;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * La implementacion de {@code mto.field.v1.FieldService}.
 *
 * <p>El permiso de cada RPC va en su metodo, porque en gRPC no hay verbos ni rutas que una cadena
 * de filtros pueda mirar: el dispositivo ({@code FIELD_TEAM}) habla por {@code TeamChannel} y
 * {@code SyncBufferedEvents}; el responsable ({@code FIELD_SUPERVISE}) abre y cierra la posesion,
 * emite ordenes y mira el tablero. El interceptor de {@link
 * com.alejandro.mtofield.configuration.security.GrpcSecurityConfiguration} ya ha autenticado el
 * token y dejado el {@code SecurityContext} puesto antes de que el metodo se invoque, tanto en una
 * RPC unaria como al abrir un stream.</p>
 *
 * <p>Las unarias dejan salir las excepciones de negocio: {@code FieldGrpcExceptionAdvice} las
 * traduce a un estado con {@code ErrorInfo}. {@code SyncBufferedEvents} (fase 3) y, hasta su
 * commit, {@code WatchPossessionBoard} delegan en el {@code ImplBase} generado, que responde
 * {@code UNIMPLEMENTED}.</p>
 */
@GrpcService
public class FieldGrpcService extends FieldServiceGrpc.FieldServiceImplBase {

    private final PossessionService possessions;
    private final FieldCommandService commands;
    private final TeamChannels teamChannels;
    private final CurrentUserService currentUser;

    public FieldGrpcService(PossessionService possessions, FieldCommandService commands, TeamChannels teamChannels, CurrentUserService currentUser) {
        this.possessions = possessions;
        this.commands = commands;
        this.teamChannels = teamChannels;
        this.currentUser = currentUser;
    }

    @Override
    @PreAuthorize("hasRole('" + SecurityRoles.FIELD_SUPERVISE + "')")
    public void openPossession(OpenPossessionRequest request, StreamObserver<Possession> responseObserver) {
        List<UUID> shiftIds = new ArrayList<>(request.getShiftIdsCount());
        for (String shiftId : request.getShiftIdsList()) {
            try {
                shiftIds.add(UUID.fromString(shiftId));
            } catch (IllegalArgumentException invalid) {
                throw new InvalidPossessionRequestException("shift_ids contains a value that is not a UUID: " + shiftId);
            }
        }
        Instant endsAt = request.hasEndsAt() ? ProtoTimestamps.toInstant(request.getEndsAt()) : null;
        PossessionView view = possessions.open(shiftIds, endsAt, username());
        responseObserver.onNext(FieldProtoMapper.toProto(view));
        responseObserver.onCompleted();
    }

    @Override
    @PreAuthorize("hasRole('" + SecurityRoles.FIELD_SUPERVISE + "')")
    public void closePossession(ClosePossessionRequest request, StreamObserver<Possession> responseObserver) {
        UUID possessionId = FieldProtoMapper.uuid(request.getPossessionId(), "possession_id");
        PossessionView view = possessions.close(possessionId, request.getForce(), request.getReason(), username());
        responseObserver.onNext(FieldProtoMapper.toProto(view));
        responseObserver.onCompleted();
    }

    @Override
    @PreAuthorize("hasRole('" + SecurityRoles.FIELD_SUPERVISE + "')")
    public void issueCommand(IssueCommandRequest request, StreamObserver<IssueCommandResponse> responseObserver) {
        UUID possessionId = FieldProtoMapper.uuid(request.getPossessionId(), "possession_id");
        String key = request.getIdempotencyKey().isBlank() ? null : request.getIdempotencyKey();
        CommandDraft draft = switch (request.getCommandCase()) {
            case EVACUATE_NOW -> CommandDraft.evacuateNow(key, request.getEvacuateNow());
            case SUPERVISOR_MESSAGE -> CommandDraft.supervisorMessage(key, request.getSupervisorMessage());
            case WINDOW_CHANGED -> {
                if (!request.getWindowChanged().hasEndsAt()) {
                    throw new IllegalArgumentException("window_changed.ends_at is required");
                }
                possessions.changeEndsAt(possessionId, ProtoTimestamps.toInstant(request.getWindowChanged().getEndsAt()), username());
                yield CommandDraft.windowChanged(key, request.getWindowChanged());
            }
            case COMMAND_NOT_SET -> throw new IllegalArgumentException("command is required: evacuate_now, supervisor_message or window_changed");
        };
        IssuedCommand issued = commands.issue(possessionId, draft, username());
        responseObserver.onNext(IssueCommandResponse.newBuilder().setCommandId(issued.id().toString()).setSequence(issued.sequence()).build());
        responseObserver.onCompleted();
    }

    @Override
    @PreAuthorize("hasRole('" + SecurityRoles.FIELD_SUPERVISE + "')")
    public void watchPossessionBoard(WatchPossessionBoardRequest request, StreamObserver<PossessionBoard> responseObserver) {
        super.watchPossessionBoard(request, responseObserver);
    }

    @Override
    @PreAuthorize("hasRole('" + SecurityRoles.FIELD_TEAM + "')")
    public StreamObserver<TeamMessage> teamChannel(StreamObserver<FieldCommand> responseObserver) {
        return teamChannels.open(responseObserver);
    }

    @Override
    @PreAuthorize("hasRole('" + SecurityRoles.FIELD_TEAM + "')")
    public StreamObserver<TeamMessage> syncBufferedEvents(StreamObserver<SyncResult> responseObserver) {
        return super.syncBufferedEvents(responseObserver);
    }

    private String username() {
        return currentUser.getUsername().orElse("unknown");
    }
}
