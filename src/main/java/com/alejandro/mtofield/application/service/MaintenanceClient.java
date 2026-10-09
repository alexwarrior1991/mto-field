package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.domain.model.ShiftSnapshot;

import java.util.Optional;
import java.util.UUID;

/**
 * La unica puerta hacia mto-maintenance. En la fase 1 solo hay un cliente desconectado que
 * devuelve turnos sinteticos; la fase 2 trae el cliente REST con la cuenta de servicio y el
 * circuito, y anade el inicio y el fin de las tareas.
 */
public interface MaintenanceClient {

    boolean isEnabled();

    Optional<ShiftSnapshot> findShift(UUID shiftId);
}
