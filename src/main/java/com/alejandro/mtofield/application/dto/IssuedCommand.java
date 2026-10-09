package com.alejandro.mtofield.application.dto;

import java.util.UUID;

/** Lo que devuelve emitir una orden: su id y el numero de secuencia que tomo. */
public record IssuedCommand(UUID id, long sequence) {
}
