package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.application.dto.EventContext;
import com.alejandro.mtofield.application.dto.StoredEvent;
import com.alejandro.mtofield.application.dto.SyncJob;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Lo que un dispositivo sube. Cada metodo es idempotente por {@code (device_id, sequence)} y
 * contesta en banda con un {@code EventResult} dirigido al turno; un rechazo nunca cierra el stream.
 */
public interface FieldEventService {

    /** Un acuse: se guarda, se aplica al equipo y se contesta APPLIED (o REJECTED si la orden no es de esta posesion). */
    void recordAck(EventContext context);

    /** La salida de via del equipo: se guarda, se marca el turno y se contesta APPLIED. */
    void recordClearOfTrack(EventContext context);

    /**
     * El inicio o el fin de una tarea: se guarda PENDING y se deja para la cola de trabajo del
     * dispositivo. Un reenvio recibe el resultado del estado guardado y no se vuelve a encolar.
     */
    StoredEvent recordTaskEvent(EventContext context);

    /** Un mensaje que no se puede aplicar (secuencia 0, un evento que no se admite aqui): solo el EventResult. */
    void rejectInline(EventContext context, String reason);

    void markSynced(UUID eventId);

    void markFailed(UUID eventId, String error, Instant nextAttemptAt);

    void markRejected(UUID eventId, String reason);

    /** La marca de agua contigua del dispositivo: todo lo <= N esta guardado. */
    long contiguousWatermark(String deviceId);

    /**
     * Los eventos de tarea que toca reintentar contra mto-maintenance (FAILED, o PENDING con su
     * siguiente intento vencido), en orden de llegada y con su contexto reconstruido del payload.
     */
    List<SyncJob> dueForRetry(int limit);
}
