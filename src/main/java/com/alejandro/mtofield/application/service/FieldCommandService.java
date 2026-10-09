package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.application.dto.CommandDraft;
import com.alejandro.mtofield.application.dto.IssuedCommand;
import com.alejandro.mtofield.application.dto.StoredCommand;

import java.util.List;
import java.util.UUID;

/** Las ordenes descendentes: emitirlas con su numero sin huecos y leerlas para reproducirlas. */
public interface FieldCommandService {

    /**
     * Persiste la orden con el siguiente numero de la posesion y la publica tras el commit. Con una
     * clave de idempotencia ya usada devuelve la orden que ya estaba, sin emitir nada.
     */
    IssuedCommand issue(UUID possessionId, CommandDraft draft, String issuedBy);

    /** Lo que un dispositivo de ese turno tiene que recibir despues de {@code after}, en orden. */
    List<StoredCommand> replayAfter(UUID possessionId, UUID shiftId, long after, int limit);

    /** El tramo [from, to] de una posesion, en orden. */
    List<StoredCommand> range(UUID possessionId, long from, long to);

    long maxSequence(UUID possessionId);
}
