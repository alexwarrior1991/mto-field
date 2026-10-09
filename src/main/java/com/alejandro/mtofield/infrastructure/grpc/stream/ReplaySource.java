package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.grpc.v1.FieldCommand;

import java.util.List;

/** De donde saca un stream su atraso: las ordenes de su turno despues de una secuencia, en orden y por paginas. */
@FunctionalInterface
public interface ReplaySource {

    List<FieldCommand> after(long sequence, int limit);
}
