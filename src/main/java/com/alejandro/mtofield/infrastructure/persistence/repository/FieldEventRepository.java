package com.alejandro.mtofield.infrastructure.persistence.repository;

import com.alejandro.mtofield.infrastructure.persistence.entity.FieldEventRecord;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldEventSyncStatus;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Los eventos subidos por los dispositivos: inserciones idempotentes y actualizaciones
 * condicionales de su estado de sincronizacion, todo nativo y decidido en la base.
 */
public interface FieldEventRepository extends JpaRepository<FieldEventRecord, UUID> {

    /**
     * Guarda el evento si es la primera vez que llega ese {@code (device_id, sequence)}; un reenvio
     * tras reconectar devuelve 0 y no aplica nada dos veces.
     */
    @Modifying
    @Query(value = """
            insert into field_event (
                id, device_id, sequence, possession_id, shift_id, kind, occurred_at, received_at, reported_by,
                payload, sync_status, sync_attempts, next_attempt_at, created_at, updated_at, created_by, updated_by
            ) values (
                :id, :deviceId, :sequence, :possessionId, :shiftId, cast(:kind as field_event_kind), :occurredAt, now(), :reportedBy,
                cast(:payload as json), cast(:syncStatus as field_event_sync_status), 0, :nextAttemptAt, now(), now(), :reportedBy, :reportedBy
            ) on conflict (device_id, sequence) do nothing
            """, nativeQuery = true)
    int insertIfMissing(@Param("id") UUID id,
                        @Param("deviceId") String deviceId,
                        @Param("sequence") long sequence,
                        @Param("possessionId") UUID possessionId,
                        @Param("shiftId") UUID shiftId,
                        @Param("kind") String kind,
                        @Param("occurredAt") Instant occurredAt,
                        @Param("reportedBy") String reportedBy,
                        @Param("payload") String payload,
                        @Param("syncStatus") String syncStatus,
                        @Param("nextAttemptAt") Instant nextAttemptAt);

    Optional<FieldEventRecord> findByDeviceIdAndSequence(String deviceId, long sequence);

    /**
     * La marca de agua CONTIGUA de un dispositivo: el mayor N tal que todo 1..N esta guardado. Con
     * dos streams en paralelo los eventos pueden llegar intercalados, y el maximo a secas diria que
     * un hueco ya esta aplicado.
     */
    @Query(value = """
            select coalesce(max(sequence), 0)
              from (select sequence, row_number() over (order by sequence) as rn
                      from field_event
                     where device_id = :deviceId) numbered
             where sequence = rn
            """, nativeQuery = true)
    long contiguousWatermark(@Param("deviceId") String deviceId);

    @Modifying
    @Query(value = """
            update field_event
               set sync_status = 'SYNCED',
                   synced_at = now(),
                   last_error = null,
                   next_attempt_at = null,
                   updated_at = now()
             where id = :id
               and sync_status in ('PENDING', 'FAILED')
            """, nativeQuery = true)
    int markSynced(@Param("id") UUID id);

    @Modifying
    @Query(value = """
            update field_event
               set sync_status = 'FAILED',
                   sync_attempts = sync_attempts + 1,
                   last_error = :error,
                   next_attempt_at = :nextAttemptAt,
                   updated_at = now()
             where id = :id
               and sync_status in ('PENDING', 'FAILED')
            """, nativeQuery = true)
    int markFailed(@Param("id") UUID id, @Param("error") String error, @Param("nextAttemptAt") Instant nextAttemptAt);

    @Modifying
    @Query(value = """
            update field_event
               set sync_status = 'REJECTED',
                   sync_attempts = sync_attempts + 1,
                   last_error = :reason,
                   next_attempt_at = null,
                   updated_at = now()
             where id = :id
               and sync_status in ('PENDING', 'FAILED')
            """, nativeQuery = true)
    int markRejected(@Param("id") UUID id, @Param("reason") String reason);

    /** Lo que toca reintentar contra mto-maintenance (Fase 2), en orden de llegada. */
    default List<FieldEventRecord> findDueForSync(Instant now, Limit limit) {
        return findDueForSync(List.of(FieldEventSyncStatus.PENDING, FieldEventSyncStatus.FAILED), now, limit);
    }

    /** Los estados van como parametro: un literal de enumerado en JPQL se traduce a un tipo que no existe. */
    @Query("""
            select e
              from FieldEventRecord e
             where e.syncStatus in :statuses
               and e.nextAttemptAt <= :now
             order by e.receivedAt asc
            """)
    List<FieldEventRecord> findDueForSync(@Param("statuses") Collection<FieldEventSyncStatus> statuses,
                                          @Param("now") Instant now,
                                          Limit limit);
}
