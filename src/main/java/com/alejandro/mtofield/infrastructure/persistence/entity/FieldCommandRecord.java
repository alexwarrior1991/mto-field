package com.alejandro.mtofield.infrastructure.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * Una orden descendente tal como se envio, de solo lectura: la escribe
 * {@code FieldCommandRepository.insert} con SQL nativo, en la misma transaccion que toma su numero
 * de secuencia, y nadie la modifica despues. Su {@code id} es el {@code FieldCommand.command_id}.
 *
 * <p>{@code payload} es el {@code FieldCommand} entero en JSON de protobuf: la reanudacion lo
 * reenvia tal cual; las columnas tipadas solo sirven para consultar.</p>
 */
@Entity
@Immutable
@Table(
        name = "field_command",
        uniqueConstraints = {
                @UniqueConstraint(name = "uq_field_command_possession_sequence", columnNames = {"possession_id", "sequence"}),
                @UniqueConstraint(name = "uq_field_command_idempotency_key", columnNames = {"possession_id", "idempotency_key"})
        },
        indexes = @Index(name = "idx_field_command_target", columnList = "possession_id, target_shift_id, sequence")
)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@ToString(callSuper = true, onlyExplicitlyIncluded = true)
public class FieldCommandRecord extends AuditableEntity {

    @Column(name = "possession_id", nullable = false)
    @ToString.Include
    private UUID possessionId;

    @Column(name = "sequence", nullable = false)
    @ToString.Include
    private long sequence;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.NAMED_ENUM)
    @Column(name = "kind", nullable = false, columnDefinition = "field_command_kind")
    @ToString.Include
    private FieldCommandKind kind;

    /** Turno destinatario; {@code null} es difusion a todos los turnos de la posesion. */
    @Column(name = "target_shift_id")
    private UUID targetShiftId;

    @Column(name = "idempotency_key", length = 100)
    private String idempotencyKey;

    @Column(name = "requires_ack", nullable = false)
    private boolean requiresAck;

    @Column(name = "issued_at", nullable = false)
    private Instant issuedAt;

    @Column(name = "issued_by", nullable = false, length = 100)
    private String issuedBy;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, columnDefinition = "json")
    private String payload;

    public boolean isBroadcast() {
        return targetShiftId == null;
    }

    /** Si la orden viaja a un dispositivo de ese turno: las de difusion y las dirigidas a el. */
    public boolean addressedTo(UUID shiftId) {
        return targetShiftId == null || targetShiftId.equals(shiftId);
    }
}
