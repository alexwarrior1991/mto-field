package com.alejandro.mtofield.infrastructure.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.hibernate.annotations.Immutable;

import java.time.Instant;
import java.util.UUID;

/**
 * De quien es un dispositivo: la persona que se unio primero con ese {@code device_id}, mientras
 * siga abierta la posesion en la que lo hizo. De solo lectura: la escribe
 * {@code DeviceBindingRepository.bind} con SQL nativo, en una sentencia.
 */
@Entity
@Immutable
@Table(name = "device_binding")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@ToString
public class DeviceBindingRecord {

    @Id
    @Column(name = "device_id", nullable = false, length = 100)
    private String deviceId;

    /** El {@code preferred_username} del token que lo ligo. */
    @Column(name = "username", nullable = false, length = 100)
    private String username;

    @Column(name = "possession_id", nullable = false)
    private UUID possessionId;

    @Column(name = "bound_at", nullable = false)
    private Instant boundAt;
}
