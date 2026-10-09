package com.alejandro.mtofield.infrastructure.persistence.entity;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Index;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * El bloqueo de una noche: agrupa varios turnos de mto-maintenance, tiene hora de fin y se cierra
 * una vez, cuando todos los equipos han salido de la via o de forma forzada por el responsable.
 */
@Entity
@Table(
        name = "possession",
        uniqueConstraints = @UniqueConstraint(name = "uq_possession_code", columnNames = "code"),
        indexes = @Index(name = "idx_possession_status_shift_date", columnList = "status, shift_date")
)
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@ToString(callSuper = true, onlyExplicitlyIncluded = true)
public class Possession extends AuditableEntity {

    @NotBlank
    @Size(max = 16)
    @Column(name = "code", nullable = false, length = 16)
    @ToString.Include
    private String code;

    @NotNull
    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.NAMED_ENUM)
    @Column(name = "status", nullable = false, columnDefinition = "possession_status")
    @Builder.Default
    @ToString.Include
    private PossessionStatus status = PossessionStatus.OPEN;

    @NotNull
    @Column(name = "shift_date", nullable = false)
    @ToString.Include
    private LocalDate shiftDate;

    @NotNull
    @Column(name = "ends_at", nullable = false)
    private Instant endsAt;

    @NotNull
    @Column(name = "opened_at", nullable = false)
    private Instant openedAt;

    @NotBlank
    @Size(max = 100)
    @Column(name = "opened_by", nullable = false, length = 100)
    private String openedBy;

    @Column(name = "closed_at")
    private Instant closedAt;

    @Size(max = 100)
    @Column(name = "closed_by", length = 100)
    private String closedBy;

    @Column(name = "forced", nullable = false)
    @Builder.Default
    private boolean forced = false;

    @Column(name = "close_reason", columnDefinition = "text")
    private String closeReason;

    /**
     * El contador de la secuencia descendente. Lo incrementa solo
     * {@code PossessionRepository.nextCommandSequence} con un {@code update ... returning} nativo,
     * nunca JPA: por eso no se inserta ni se actualiza desde aqui (la columna tiene su DEFAULT 0).
     */
    @Column(name = "next_command_seq", nullable = false, insertable = false, updatable = false)
    private long nextCommandSeq;

    @OneToMany(mappedBy = "possession", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    @Builder.Default
    private List<PossessionShift> shifts = new ArrayList<>();

    public void addShift(PossessionShift shift) {
        shift.setPossession(this);
        shifts.add(shift);
    }

    public boolean isOpen() {
        return status == PossessionStatus.OPEN;
    }
}
