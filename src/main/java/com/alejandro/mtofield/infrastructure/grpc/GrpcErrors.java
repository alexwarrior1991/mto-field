package com.alejandro.mtofield.infrastructure.grpc;

import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import java.util.Map;

/**
 * Los errores que cierran una llamada, con un {@code google.rpc.ErrorInfo} en los detalles: el
 * {@code reason} es estable y legible por maquina (lo que un dispositivo o el simulador comparan),
 * el mensaje es para una persona y el {@code metadata} lleva lo que haga falta (los equipos que
 * faltan, por ejemplo). Viajan en el trailer {@code grpc-status-details-bin}, que grpcurl ya muestra.
 */
public final class GrpcErrors {

    public static final String DOMAIN = "mto-field";

    private GrpcErrors() {
    }

    public static StatusRuntimeException of(Status.Code code, String reason, String message) {
        return of(code, reason, message, Map.of());
    }

    public static StatusRuntimeException of(Status.Code code, String reason, String message, Map<String, String> metadata) {
        ErrorInfo info = ErrorInfo.newBuilder().setReason(reason).setDomain(DOMAIN).putAllMetadata(metadata).build();
        com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                .setCode(code.value())
                .setMessage(message == null ? reason : message)
                .addDetails(Any.pack(info))
                .build();
        return StatusProto.toStatusRuntimeException(status);
    }

    /** El {@code reason} del ErrorInfo de un error recibido, o vacio si no lleva ninguno. */
    public static String reasonOf(Throwable throwable) {
        com.google.rpc.Status status = StatusProto.fromThrowable(throwable);
        if (status == null) {
            return "";
        }
        for (Any detail : status.getDetailsList()) {
            if (detail.is(ErrorInfo.class)) {
                try {
                    return detail.unpack(ErrorInfo.class).getReason();
                } catch (com.google.protobuf.InvalidProtocolBufferException ignored) {
                    return "";
                }
            }
        }
        return "";
    }

    /** El {@code metadata} del ErrorInfo de un error recibido, o un mapa vacio. */
    public static Map<String, String> metadataOf(Throwable throwable) {
        com.google.rpc.Status status = StatusProto.fromThrowable(throwable);
        if (status == null) {
            return Map.of();
        }
        for (Any detail : status.getDetailsList()) {
            if (detail.is(ErrorInfo.class)) {
                try {
                    return detail.unpack(ErrorInfo.class).getMetadataMap();
                } catch (com.google.protobuf.InvalidProtocolBufferException ignored) {
                    return Map.of();
                }
            }
        }
        return Map.of();
    }

    static Code codeOf(Status.Code code) {
        return Code.forNumber(code.value());
    }
}
