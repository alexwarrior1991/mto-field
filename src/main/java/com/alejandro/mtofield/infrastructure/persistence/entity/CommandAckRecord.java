package com.alejandro.mtofield.infrastructure.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.hibernate.annotations.Immutable;

import java.time.Instant;
import java.util.UUID;

/**
 * El acuse de una orden por un EQUIPO (uno por orden y turno; lo da cualquiera de sus dispositivos),
 * de solo lectura: lo escribe {@code CommandAckRepository.insertIfMissing} con SQL nativo.
 */
@Entity
@Immutable
@Table(
        name = "command_ack",
        uniqueConstraints = @UniqueConstraint(name = "uq_command_ack_command_shift", columnNames = {"command_id", "shift_id"})
)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@ToString(callSuper = true, onlyExplicitlyIncluded = true)
public class CommandAckRecord extends AuditableEntity {

    @Column(name = "command_id", nullable = false)
    @ToString.Include
    private UUID commandId;

    @Column(name = "shift_id", nullable = false)
    @ToString.Include
    private UUID shiftId;

    @Column(name = "device_id", nullable = false, length = 100)
    private String deviceId;

    /** El usuario del token que acuso. */
    @Column(name = "acked_by", nullable = false, length = 100)
    private String ackedBy;

    @Column(name = "accepted", nullable = false)
    private boolean accepted;

    @Column(name = "reason", columnDefinition = "text")
    private String reason;

    @Column(name = "acked_at", nullable = false)
    private Instant ackedAt;
}
