package com.alejandro.mtofield.infrastructure.persistence.repository;

import com.alejandro.mtofield.infrastructure.persistence.entity.FieldCommandRecord;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Las ordenes descendentes. Se insertan con SQL nativo (en la misma transaccion que toma su numero
 * de secuencia) y despues solo se leen.
 */
public interface FieldCommandRepository extends JpaRepository<FieldCommandRecord, UUID> {

    /**
     * Inserta la orden. Una clave de idempotencia repetida salta en
     * {@code uq_field_command_idempotency_key} y la transaccion entera (incluido el incremento del
     * contador) se deshace: sin hueco, y el reintento devuelve la orden que ya estaba.
     */
    @Modifying
    @Query(value = """
            insert into field_command (
                id, possession_id, sequence, kind, target_shift_id, idempotency_key, requires_ack,
                issued_at, issued_by, payload, created_at, updated_at, created_by, updated_by
            ) values (
                :id, :possessionId, :sequence, cast(:kind as field_command_kind), :targetShiftId, :idempotencyKey, :requiresAck,
                :issuedAt, :issuedBy, cast(:payload as json), now(), now(), :issuedBy, :issuedBy
            )
            """, nativeQuery = true)
    int insert(@Param("id") UUID id,
               @Param("possessionId") UUID possessionId,
               @Param("sequence") long sequence,
               @Param("kind") String kind,
               @Param("targetShiftId") UUID targetShiftId,
               @Param("idempotencyKey") String idempotencyKey,
               @Param("requiresAck") boolean requiresAck,
               @Param("issuedAt") Instant issuedAt,
               @Param("issuedBy") String issuedBy,
               @Param("payload") String payload);

    Optional<FieldCommandRecord> findByPossessionIdAndIdempotencyKey(UUID possessionId, String idempotencyKey);

    /**
     * Lo que un dispositivo de ese turno tiene que recibir despues de {@code after}: las ordenes de
     * difusion y las dirigidas a su turno, en orden de secuencia. Es la reproduccion de la
     * reanudacion, paginada con {@code limit}.
     */
    @Query("""
            select c
              from FieldCommandRecord c
             where c.possessionId = :possessionId
               and c.sequence > :after
               and (c.targetShiftId is null or c.targetShiftId = :shiftId)
             order by c.sequence asc
            """)
    List<FieldCommandRecord> findReplay(@Param("possessionId") UUID possessionId,
                                        @Param("shiftId") UUID shiftId,
                                        @Param("after") long after,
                                        Limit limit);

    /** El tramo [from, to] de una posesion, para que el despachador rellene lo que aun no abanico. */
    List<FieldCommandRecord> findByPossessionIdAndSequenceBetweenOrderBySequenceAsc(UUID possessionId, long from, long to);

    @Query("select coalesce(max(c.sequence), 0) from FieldCommandRecord c where c.possessionId = :possessionId")
    long maxSequence(@Param("possessionId") UUID possessionId);

    /** Las ordenes cuyo acuse sigue el tablero. */
    List<FieldCommandRecord> findByPossessionIdAndRequiresAckTrueOrderBySequenceAsc(UUID possessionId);
}
