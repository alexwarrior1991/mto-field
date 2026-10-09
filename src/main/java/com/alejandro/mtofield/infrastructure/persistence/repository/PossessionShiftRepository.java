package com.alejandro.mtofield.infrastructure.persistence.repository;

import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionShift;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface PossessionShiftRepository extends JpaRepository<PossessionShift, UUID> {

    List<PossessionShift> findByPossession_IdOrderByTeamCodeAsc(UUID possessionId);

    Optional<PossessionShift> findByPossession_IdAndShiftId(UUID possessionId, UUID shiftId);

    long countByPossession_IdAndClearOfTrackAtIsNull(UUID possessionId);

    boolean existsByShiftIdAndOpenTrue(UUID shiftId);

    /**
     * La posesion abierta en la que esta un turno, si la hay: lo que un {@code Join} necesita saber.
     * Como mucho una, por el indice parcial {@code uq_possession_shift_open}.
     */
    default Optional<OpenMembership> findOpenMembership(UUID shiftId) {
        return findMembership(shiftId, PossessionStatus.OPEN);
    }

    /**
     * El estado va como parametro y no como literal: Hibernate traduce un literal de enumerado a
     * un cast al tipo con el nombre de la clase Java ({@code possessionstatus}), que no existe.
     */
    @Query("""
            select s.possession.id as possessionId,
                   s.shiftId as shiftId,
                   s.teamCode as teamCode,
                   s.possession.endsAt as endsAt
              from PossessionShift s
             where s.shiftId = :shiftId
               and s.open = true
               and s.possession.status = :status
            """)
    Optional<OpenMembership> findMembership(@Param("shiftId") UUID shiftId, @Param("status") PossessionStatus status);

    /**
     * La salida de via de un equipo, escrita una sola vez: devuelve 0 si ya estaba fuera, y entonces
     * quien llama lo da igualmente por aplicado.
     */
    @Modifying
    @Query(value = """
            update possession_shift
               set clear_of_track_at = now(),
                   clear_of_track_by = :user,
                   clear_of_track_device = :device,
                   earthing_removed = :earthingRemoved,
                   updated_at = now(),
                   updated_by = :user
             where possession_id = :possessionId
               and shift_id = :shiftId
               and clear_of_track_at is null
            """, nativeQuery = true)
    int markClear(@Param("possessionId") UUID possessionId,
                  @Param("shiftId") UUID shiftId,
                  @Param("user") String user,
                  @Param("device") String device,
                  @Param("earthingRemoved") boolean earthingRemoved);

    /** Saca los turnos del indice parcial al cerrar la posesion. */
    @Modifying
    @Query(value = """
            update possession_shift
               set open = false,
                   updated_at = now(),
                   updated_by = :user
             where possession_id = :possessionId
               and open
            """, nativeQuery = true)
    int closeAll(@Param("possessionId") UUID possessionId, @Param("user") String user);

    /** Lo que un Join necesita de la posesion abierta de su turno. */
    interface OpenMembership {

        UUID getPossessionId();

        UUID getShiftId();

        String getTeamCode();

        Instant getEndsAt();
    }
}
