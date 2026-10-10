package com.alejandro.mtofield.simulator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Las opciones de la linea de ordenes del simulador.
 *
 * @param mode          {@code demo} (responsable y equipos en una JVM), {@code supervisor} o {@code device}
 * @param targets       host:puerto de cada replica del servidor gRPC (varios, separados por comas: los dispositivos se
 *                      reparten entre ellos y el responsable usa el primero)
 * @param token         un token ya acunado, o nada: entonces {@code user}/{@code password} contra {@code tokenUrl}
 * @param localIssuer   sin Keycloak: el simulador sirve el JWK Set de la clave de test y acuna sus tokens
 * @param teams         equipos (turnos) que se simulan o se abren
 * @param shifts        ids de turno dados, en vez de inventados
 * @param teamCodes     con {@code --local-issuer} y turnos dados, el codigo del equipo de cada turno (va en el claim de grupos del token);
 *                      con turnos inventados es el sintetico del cliente NoOp
 * @param devicesPerTeam dispositivos por equipo
 * @param neverAckTeam  el equipo (1..N) que nunca acusa el desalojo; 0 = ninguno
 * @param cutEvery      cada cuanto un dispositivo pierde la cobertura (cancela el stream y reanuda); 0 = nunca
 * @param blocking      los dispositivos usan el stub bloqueante v2 (si no, el observador con onReady)
 * @param mixed         alternar los dos estilos de cliente
 * @param evacuateAfter cuando el responsable ordena el desalojo
 * @param duration      cuanto dura la simulacion antes de cerrar
 * @param heartbeat     el intervalo del latido
 * @param tokenTtl      con {@code --local-issuer}, cuanto dura cada token acunado (para ver el cierre por caducidad y la renovacion)
 */
record SimulatorOptions(
        String mode,
        List<String> targets,
        String token,
        String user,
        String password,
        String tokenUrl,
        String clientId,
        boolean localIssuer,
        int issuerPort,
        int teams,
        List<UUID> shifts,
        List<String> teamCodes,
        int devicesPerTeam,
        int neverAckTeam,
        Duration cutEvery,
        boolean blocking,
        boolean mixed,
        Duration evacuateAfter,
        Duration duration,
        Duration heartbeat,
        Duration tokenTtl
) {

    static final String USAGE = """
            Usage: FieldSimulator [--mode demo|supervisor|device] [--target host:port[,host:port...]]
                   [--token <jwt> | --user <u> --password <p> [--token-url <url>] [--client-id mto-frontend] | --local-issuer [--issuer-port 8082]]
                   [--teams N] [--shifts id,id,...] [--team-codes code,code,...] [--devices-per-team N] [--never-ack-team N]
                   [--cut-every 30s] [--blocking] [--mixed] [--evacuate-after 20s] [--duration 90s] [--heartbeat 10s] [--token-ttl 60m]
            """;

    static SimulatorOptions parse(String[] args) {
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < args.length; index++) {
            String arg = args[index];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + arg + "\n" + USAGE);
            }
            String key = arg.substring(2);
            boolean flag = key.equals("blocking") || key.equals("mixed") || key.equals("local-issuer") || key.equals("help");
            if (flag) {
                values.put(key, "true");
            } else if (index + 1 < args.length) {
                values.put(key, args[++index]);
            } else {
                throw new IllegalArgumentException("Missing value for --" + key + "\n" + USAGE);
            }
        }
        if (values.containsKey("help")) {
            throw new IllegalArgumentException(USAGE);
        }
        List<UUID> shifts = new ArrayList<>();
        if (values.containsKey("shifts")) {
            for (String shift : values.get("shifts").split(",")) {
                shifts.add(UUID.fromString(shift.trim()));
            }
        }
        return new SimulatorOptions(
                values.getOrDefault("mode", "demo"),
                List.of(values.getOrDefault("target", "localhost:9094").split(",")),
                values.get("token"),
                values.get("user"),
                values.get("password"),
                values.getOrDefault("token-url", "http://auth.mto.local:8082/realms/mto/protocol/openid-connect/token"),
                values.getOrDefault("client-id", "mto-frontend"),
                values.containsKey("local-issuer"),
                Integer.parseInt(values.getOrDefault("issuer-port", "8082")),
                Integer.parseInt(values.getOrDefault("teams", shifts.isEmpty() ? "3" : String.valueOf(shifts.size()))),
                shifts,
                values.containsKey("team-codes") ? List.of(values.get("team-codes").split(",")) : List.of(),
                Integer.parseInt(values.getOrDefault("devices-per-team", "1")),
                Integer.parseInt(values.getOrDefault("never-ack-team", "0")),
                duration(values.getOrDefault("cut-every", "0s")),
                values.containsKey("blocking"),
                values.containsKey("mixed"),
                duration(values.getOrDefault("evacuate-after", "20s")),
                duration(values.getOrDefault("duration", "90s")),
                duration(values.getOrDefault("heartbeat", "10s")),
                duration(values.getOrDefault("token-ttl", "60m"))
        );
    }

    /** "30s", "2m", "500ms" o un ISO-8601 ("PT30S"). */
    static Duration duration(String value) {
        String text = value.trim().toLowerCase();
        if (text.startsWith("pt") || text.startsWith("p")) {
            return Duration.parse(value.toUpperCase());
        }
        if (text.endsWith("ms")) {
            return Duration.ofMillis(Long.parseLong(text.substring(0, text.length() - 2)));
        }
        if (text.endsWith("s")) {
            return Duration.ofSeconds(Long.parseLong(text.substring(0, text.length() - 1)));
        }
        if (text.endsWith("m")) {
            return Duration.ofMinutes(Long.parseLong(text.substring(0, text.length() - 1)));
        }
        return Duration.ofSeconds(Long.parseLong(text));
    }

    /**
     * El codigo del equipo del turno N (1..): el dado con --team-codes o, con un turno inventado, el
     * sintetico que le da el cliente NoOp de mto-field (T- y los cuatro primeros hex del id).
     */
    String teamCodeOf(int team, UUID shiftId) {
        if (team - 1 < teamCodes.size()) {
            return teamCodes.get(team - 1).trim();
        }
        return "T-" + shiftId.toString().substring(0, 4).toUpperCase();
    }

    /** Los turnos de la simulacion: los dados, o uno inventado por equipo. */
    List<UUID> shiftIds() {
        if (!shifts.isEmpty()) {
            return shifts;
        }
        List<UUID> invented = new ArrayList<>(teams);
        for (int index = 0; index < teams; index++) {
            invented.add(UUID.randomUUID());
        }
        return invented;
    }
}
