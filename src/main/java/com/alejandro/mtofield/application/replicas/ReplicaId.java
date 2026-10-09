package com.alejandro.mtofield.application.replicas;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.HexFormat;

/**
 * El nombre de esta replica en el bus: el configurado ({@code app.field.replicas.id}) o, en blanco,
 * el nombre de la maquina con un sufijo aleatorio, para que dos replicas en el mismo host (o un
 * contenedor reiniciado antes de que el broker borre su cola) no se confundan.
 */
public record ReplicaId(String value) {

    public ReplicaId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("a replica id cannot be blank");
        }
    }

    public static ReplicaId of(String configured) {
        if (configured != null && !configured.isBlank()) {
            return new ReplicaId(configured.trim());
        }
        byte[] suffix = new byte[3];
        new SecureRandom().nextBytes(suffix);
        return new ReplicaId(hostname() + "-" + HexFormat.of().formatHex(suffix));
    }

    private static String hostname() {
        try {
            String name = InetAddress.getLocalHost().getHostName();
            return name == null || name.isBlank() ? "replica" : name;
        } catch (UnknownHostException unknown) {
            return "replica";
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
