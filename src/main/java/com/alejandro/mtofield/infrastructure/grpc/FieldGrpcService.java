package com.alejandro.mtofield.infrastructure.grpc;

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
import io.grpc.stub.StreamObserver;
import org.springframework.grpc.server.service.GrpcService;
import org.springframework.security.access.prepost.PreAuthorize;

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
 * <p>Mientras una RPC no se implemente, se delega en el {@code ImplBase} generado, que responde
 * {@code UNIMPLEMENTED}.</p>
 */
@GrpcService
public class FieldGrpcService extends FieldServiceGrpc.FieldServiceImplBase {

    @Override
    @PreAuthorize("hasRole('" + SecurityRoles.FIELD_SUPERVISE + "')")
    public void openPossession(OpenPossessionRequest request, StreamObserver<Possession> responseObserver) {
        super.openPossession(request, responseObserver);
    }

    @Override
    @PreAuthorize("hasRole('" + SecurityRoles.FIELD_SUPERVISE + "')")
    public void closePossession(ClosePossessionRequest request, StreamObserver<Possession> responseObserver) {
        super.closePossession(request, responseObserver);
    }

    @Override
    @PreAuthorize("hasRole('" + SecurityRoles.FIELD_SUPERVISE + "')")
    public void issueCommand(IssueCommandRequest request, StreamObserver<IssueCommandResponse> responseObserver) {
        super.issueCommand(request, responseObserver);
    }

    @Override
    @PreAuthorize("hasRole('" + SecurityRoles.FIELD_SUPERVISE + "')")
    public void watchPossessionBoard(WatchPossessionBoardRequest request, StreamObserver<PossessionBoard> responseObserver) {
        super.watchPossessionBoard(request, responseObserver);
    }

    @Override
    @PreAuthorize("hasRole('" + SecurityRoles.FIELD_TEAM + "')")
    public StreamObserver<TeamMessage> teamChannel(StreamObserver<FieldCommand> responseObserver) {
        return super.teamChannel(responseObserver);
    }

    @Override
    @PreAuthorize("hasRole('" + SecurityRoles.FIELD_TEAM + "')")
    public StreamObserver<TeamMessage> syncBufferedEvents(StreamObserver<SyncResult> responseObserver) {
        return super.syncBufferedEvents(responseObserver);
    }
}
