package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.application.dto.ShiftMembership;

import java.util.Optional;

/**
 * La posicion del vehiculo de cada dispositivo, hacia {@code mto-resources}: el dispositivo dice al
 * unirse en que vehiculo de su turno va ({@code Join.vehicle_id}) y en cada latido su kp (y su via, o la
 * unica del turno); lo que cambio se publica por el exchange de telemetria, como mucho cada
 * {@code app.field.positions.min-interval}, y nunca despues de que su equipo salga de la via.
 *
 * <p>Lo que se sabe de cada dispositivo vive en memoria de esta replica, ligado al stream que lo abrio
 * ({@code owner}): un stream que sustituye a otro del mismo dispositivo no pierde su vehiculo cuando el
 * anterior se cierra. Una replica que muere lo pierde con sus streams, y el dispositivo lo vuelve a
 * decir al reconectar.</p>
 */
public interface VehiclePositionReporter {

    /** El motivo del rechazo, o vacio si el vehiculo es del turno y queda ligado al dispositivo. */
    Optional<String> bind(Object owner, String deviceId, ShiftMembership shift, String vehicleId);

    /** Un latido: publica la posicion si cambio, si toca y si el turno sigue en la via. Nunca falla hacia el stream. */
    void report(String deviceId, String kp, long trackId, String reportedBy);

    /** El stream {@code owner} se cerro: su dispositivo deja de informar (si no lo ha sustituido otro). */
    void unbind(Object owner, String deviceId);
}
