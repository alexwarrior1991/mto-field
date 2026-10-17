package com.alejandro.mtofield.infrastructure.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.ForeignKey;
import jakarta.persistence.Index;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * Un turno de mto-maintenance dentro de una posesion, con la instantanea del equipo que el tablero
 * y los acuses nombran, y su salida de via.
 *
 * <p>La dotacion ({@code crewUsernames}: el usuario del jefe, del supervisor y de cada lineman) y los
 * vehiculos del turno se copian de mto-maintenance al abrir (V4): con dotacion, un {@code Join} se liga
 * por la persona; sin ella, por el grupo del equipo. Un {@code Join} que no encuentra a la persona
 * relee el turno una vez ({@code refreshCrew}), por si es un suplente anadido despues.</p>
 *
 * <p>{@code open} copia el estado de la posesion solo para el indice unico parcial
 * {@code uq_possession_shift_open (shift_id) WHERE open}: un turno no puede estar en dos posesiones
 * abiertas. Lo pone a {@code false} la transaccion que cierra.</p>
 */
@Entity
@Table(
        name = "possession_shift",
        uniqueConstraints = @UniqueConstraint(name = "uq_possession_shift_possession_shift", columnNames = {"possession_id", "shift_id"}),
        indexes = @Index(name = "idx_possession_shift_shift", columnList = "shift_id")
)
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@ToString(callSuper = true, onlyExplicitlyIncluded = true)
public class PossessionShift extends AuditableEntity {

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "possession_id", nullable = false, foreignKey = @ForeignKey(name = "fk_possession_shift_possession"))
    private Possession possession;

    @NotNull
    @Column(name = "shift_id", nullable = false)
    @ToString.Include
    private UUID shiftId;

    @Size(max = 32)
    @Column(name = "shift_code", length = 32)
    private String shiftCode;

    @NotBlank
    @Size(max = 16)
    @Column(name = "team_code", nullable = false, length = 16)
    @ToString.Include
    private String teamCode;

    @Size(max = 120)
    @Column(name = "team_name", length = 120)
    private String teamName;

    @Column(name = "planned_end")
    private Instant plannedEnd;

    /** El paquete de ejecucion del turno (id de mto-configuration), para la posicion de sus vehiculos. */
    @Column(name = "execution_package_id")
    private Long executionPackageId;

    /** Las vias del turno: la posicion de un vehiculo sin via en el latido es la unica, si solo hay una. */
    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "track_ids", nullable = false)
    @Builder.Default
    private List<Long> trackIds = new ArrayList<>();

    /** Los vehiculos del turno (ids de mto-resources): un dispositivo solo informa de la posicion de uno de ellos. */
    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "vehicle_ids", nullable = false)
    @Builder.Default
    private List<UUID> vehicleIds = new ArrayList<>();

    /** El usuario de Keycloak de cada persona de la dotacion, sin repetir; vacio si el turno no la tiene. */
    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "crew_usernames", nullable = false)
    @Builder.Default
    private List<String> crewUsernames = new ArrayList<>();

    @Column(name = "open", nullable = false)
    @Builder.Default
    private boolean open = true;

    @Column(name = "clear_of_track_at")
    private Instant clearOfTrackAt;

    @Size(max = 100)
    @Column(name = "clear_of_track_by", length = 100)
    private String clearOfTrackBy;

    @Size(max = 100)
    @Column(name = "clear_of_track_device", length = 100)
    private String clearOfTrackDevice;

    @Column(name = "earthing_removed")
    private Boolean earthingRemoved;

    public boolean isClearOfTrack() {
        return clearOfTrackAt != null;
    }

    /** La dotacion y los vehiculos de lo que dice ahora mto-maintenance del turno (al abrir o al releerlo). */
    public void takeCrewOf(List<String> usernames, List<UUID> vehicles, List<Long> tracks, Long executionPackage) {
        this.crewUsernames = new ArrayList<>(usernames.stream().map(username -> username.trim().toLowerCase(Locale.ROOT)).distinct().toList());
        this.vehicleIds = new ArrayList<>(vehicles);
        if (!tracks.isEmpty()) {
            this.trackIds = new ArrayList<>(tracks);
        }
        if (executionPackage != null) {
            this.executionPackageId = executionPackage;
        }
    }
}
