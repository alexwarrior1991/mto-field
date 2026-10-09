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
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * Un evento subido por un dispositivo, unico por {@code (device_id, sequence)}. Lo inserta
 * {@code FieldEventRepository.insertIfMissing} con {@code on conflict do nothing}, y solo su estado
 * de sincronizacion con mto-maintenance cambia despues, con actualizaciones nativas condicionales.
 * Desde JPA se lee; no se escribe.
 */
@Entity
@Table(
        name = "field_event",
        uniqueConstraints = @UniqueConstraint(name = "uq_field_event_device_sequence", columnNames = {"device_id", "sequence"}),
        indexes = @Index(name = "idx_field_event_possession_received", columnList = "possession_id, received_at")
)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@ToString(callSuper = true, onlyExplicitlyIncluded = true)
public class FieldEventRecord extends AuditableEntity {

    @Column(name = "device_id", nullable = false, length = 100)
    @ToString.Include
    private String deviceId;

    @Column(name = "sequence", nullable = false)
    @ToString.Include
    private long sequence;

    @Column(name = "possession_id", nullable = false)
    private UUID possessionId;

    @Column(name = "shift_id", nullable = false)
    private UUID shiftId;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.NAMED_ENUM)
    @Column(name = "kind", nullable = false, columnDefinition = "field_event_kind")
    @ToString.Include
    private FieldEventKind kind;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "reported_by", nullable = false, length = 100)
    private String reportedBy;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, columnDefinition = "json")
    private String payload;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.NAMED_ENUM)
    @Column(name = "sync_status", nullable = false, columnDefinition = "field_event_sync_status")
    @ToString.Include
    private FieldEventSyncStatus syncStatus;

    @Column(name = "sync_attempts", nullable = false)
    private int syncAttempts;

    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    @Column(name = "synced_at")
    private Instant syncedAt;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;
}
