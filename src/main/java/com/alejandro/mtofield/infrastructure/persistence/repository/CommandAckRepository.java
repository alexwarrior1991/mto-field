package com.alejandro.mtofield.infrastructure.persistence.repository;

import com.alejandro.mtofield.infrastructure.persistence.entity.CommandAckRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface CommandAckRepository extends JpaRepository<CommandAckRecord, UUID> {

    /**
     * El acuse de un equipo a una orden, una sola vez: el segundo dispositivo del mismo equipo, o el
     * mismo tras reconectar, no hace nada (devuelve 0) y se da por acusado igualmente.
     */
    @Modifying
    @Query(value = """
            insert into command_ack (
                id, command_id, shift_id, device_id, acked_by, accepted, reason, acked_at,
                created_at, updated_at, created_by, updated_by
            ) values (
                gen_random_uuid(), :commandId, :shiftId, :deviceId, :ackedBy, :accepted, :reason, now(),
                now(), now(), :ackedBy, :ackedBy
            ) on conflict (command_id, shift_id) do nothing
            """, nativeQuery = true)
    int insertIfMissing(@Param("commandId") UUID commandId,
                        @Param("shiftId") UUID shiftId,
                        @Param("deviceId") String deviceId,
                        @Param("ackedBy") String ackedBy,
                        @Param("accepted") boolean accepted,
                        @Param("reason") String reason);

    List<CommandAckRecord> findByCommandIdIn(Collection<UUID> commandIds);

    List<CommandAckRecord> findByCommandId(UUID commandId);
}
