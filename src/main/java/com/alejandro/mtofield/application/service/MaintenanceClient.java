package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.application.dto.CompleteTaskCommand;
import com.alejandro.mtofield.application.exception.MaintenanceRejectedException;
import com.alejandro.mtofield.application.exception.MaintenanceUnavailableException;
import com.alejandro.mtofield.domain.model.ShiftSnapshot;
import com.alejandro.mtofield.domain.model.TaskSnapshot;

import java.util.Optional;
import java.util.UUID;

/**
 * La unica puerta hacia mto-maintenance. El cliente REST ({@code RestClientMaintenanceClient}) habla
 * con la cuenta de servicio {@code mto-field-svc} dentro del circuito {@code maintenance}; con
 * {@code app.maintenance.enabled=false} lo sustituye el {@code NoOpMaintenanceClient}, que
 * devuelve turnos sinteticos y no admite tareas.
 *
 * <p>Un fallo es una de dos excepciones: {@link MaintenanceRejectedException} si mto-maintenance
 * respondio que no (un 4xx de negocio, con su codigo), y {@link MaintenanceUnavailableException}
 * para todo lo demas (red, tiempo agotado, 5xx, circuito abierto, la cuenta de servicio rechazada).</p>
 */
public interface MaintenanceClient {

    boolean isEnabled();

    /** El turno, o vacio si mto-maintenance no lo conoce (404). */
    Optional<ShiftSnapshot> findShift(UUID shiftId);

    /** La tarea tal como esta ahora, o vacio si la orden o la tarea no existen: lo que reconcilia una respuesta perdida. */
    Optional<TaskSnapshot> findTask(UUID orderId, UUID taskId);

    /** {@code POST /orders/{orderId}/tasks/{taskId}/start} con el turno y la persona del dispositivo. */
    TaskSnapshot startTask(UUID orderId, UUID taskId, UUID shiftId, String assignedUser);

    /** {@code POST /orders/{orderId}/tasks/{taskId}/complete}. No es idempotente: un reintento tras una respuesta perdida es un 409. */
    TaskSnapshot completeTask(UUID orderId, UUID taskId, CompleteTaskCommand command);
}
