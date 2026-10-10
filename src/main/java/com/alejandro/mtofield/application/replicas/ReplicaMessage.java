package com.alejandro.mtofield.application.replicas;

import java.time.Instant;
import java.util.UUID;

/**
 * Lo que una replica les cuenta a las demas: el estado que no esta en la base. La base es la
 * verdad (las ordenes con su secuencia, los acuses, las salidas de via); esto es solo quien esta
 * conectado a que replica, su ultimo latido y hasta donde se le ha escrito, mas los avisos que
 * ahorran esperar al tic de puesta al dia. Un mensaje que se pierda no pierde nada.
 */
public sealed interface ReplicaMessage permits ReplicaMessage.Command, ReplicaMessage.Device, ReplicaMessage.Closed, ReplicaMessage.Stopped {

    /** Una orden confirmada en la base: solo su numero, cada replica la lee de alli. */
    record Command(UUID possessionId, long sequence) implements ReplicaMessage {
    }

    /**
     * Lo ultimo que esta replica sabe de un dispositivo que tiene (o tenia) conectado: lo que el
     * tablero de cualquier replica necesita para la vida del equipo y para {@code sent_to}.
     *
     * @param openedAt         cuando se abrio el stream en la replica que lo cuenta; decide quien sustituye a quien
     * @param streamOpen       {@code false} es el ultimo estado conocido tras cerrarse el stream
     * @param lastSentSequence la mayor secuencia escrita en ese stream
     */
    record Device(String deviceId, UUID shiftId, UUID possessionId, String teamCode, Instant openedAt, Instant lastSeen, String kp,
                  int batteryPct, int signalDbm, boolean streamOpen, long lastSentSequence) implements ReplicaMessage {
    }

    /** La posesion ha quedado cerrada en la base: las demas replicas despiden sus streams y sus observadores. */
    record Closed(UUID possessionId) implements ReplicaMessage {
    }

    /** La replica se para: lo que contaba de sus dispositivos deja de valer. */
    record Stopped() implements ReplicaMessage {
    }
}
