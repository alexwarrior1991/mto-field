package com.alejandro.mtofield.domain.model;

import java.util.UUID;

/**
 * Lo que mto-field lee de una tarea de mto-maintenance para saber si lo que un dispositivo subio ya
 * esta aplicado alli: su estado ({@code PENDING}, {@code IN_PROGRESS}, {@code COMPLETED},
 * {@code CANCELLED}) y el turno en el que se trabaja. Un subconjunto de su
 * {@code MaintenanceTaskResponse}.
 */
public record TaskSnapshot(UUID id, UUID orderId, String status, UUID shiftId, String assignedUser) {

    public boolean isPending() {
        return "PENDING".equals(status);
    }

    public boolean isInProgress() {
        return "IN_PROGRESS".equals(status);
    }

    public boolean isCompleted() {
        return "COMPLETED".equals(status);
    }
}
