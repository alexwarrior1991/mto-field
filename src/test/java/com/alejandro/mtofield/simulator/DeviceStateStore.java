package com.alejandro.mtofield.simulator;

import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.google.protobuf.util.JsonFormat;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Lo que un dispositivo guarda en su propio disco para sobrevivir a un reinicio: su contador de
 * subidas, la ultima orden que aplico y las subidas que el servidor no le ha confirmado (fase 5).
 *
 * <p>Hasta aqui el simulador seguia, en cada arranque, por detras de la marca de agua que el servidor
 * le daba en el {@code Welcome}. Con el fichero, al volver a arrancar sigue por su propio contador y
 * reenvia lo que quedo sin confirmar, que es lo que haria una tableta de verdad: lo que el servidor
 * ya tiene lo descarta como duplicado por la marca contigua. Un fichero por dispositivo,
 * {@code <state-dir>/<deviceId>.json}, escrito entero y de forma atomica (temporal y {@code move}):
 * un corte a mitad de escritura deja el anterior, nunca uno a medias. Un fichero ausente o corrupto
 * cuenta como ninguno y se dice.</p>
 */
final class DeviceStateStore {

    /** Lo que se guarda. */
    record State(long sequence, long lastCommandSequence, List<TeamMessage> unconfirmed) {
        State {
            unconfirmed = List.copyOf(unconfirmed);
        }
    }

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final Path file;

    DeviceStateStore(Path directory, String deviceId) {
        this.file = directory.resolve(deviceId + ".json");
    }

    Path file() {
        return file;
    }

    /** El estado guardado, o nada si no hay fichero o no se entiende (y entonces se dice en el log). */
    Optional<State> load() {
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try {
            JsonNode root = JSON.readTree(Files.readString(file, StandardCharsets.UTF_8));
            List<TeamMessage> unconfirmed = new ArrayList<>();
            for (JsonNode node : root.path("unconfirmed")) {
                TeamMessage.Builder builder = TeamMessage.newBuilder();
                JsonFormat.parser().merge(node.toString(), builder);
                unconfirmed.add(builder.build());
            }
            return Optional.of(new State(root.path("sequence").asLong(0), root.path("lastCommandSequence").asLong(0), unconfirmed));
        } catch (IOException | RuntimeException unreadable) {
            Log.error("state", file + " is unreadable and is ignored: " + unreadable);
            return Optional.empty();
        }
    }

    /** Guarda el estado entero, de forma atomica. Un fallo se dice y no para al dispositivo. */
    void save(State state) {
        try {
            Files.createDirectories(file.getParent());
            StringBuilder json = new StringBuilder();
            json.append("{\n  \"sequence\": ").append(state.sequence())
                    .append(",\n  \"lastCommandSequence\": ").append(state.lastCommandSequence())
                    .append(",\n  \"unconfirmed\": [");
            for (int index = 0; index < state.unconfirmed().size(); index++) {
                json.append(index == 0 ? "\n    " : ",\n    ").append(JsonFormat.printer().omittingInsignificantWhitespace().print(state.unconfirmed().get(index)));
            }
            json.append(state.unconfirmed().isEmpty() ? "]\n}\n" : "\n  ]\n}\n");
            Path temporary = file.resolveSibling(file.getFileName() + ".tmp");
            Files.writeString(temporary, json, StandardCharsets.UTF_8);
            try {
                Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException | RuntimeException failure) {
            Log.error("state", "could not save " + file + ": " + failure);
        }
    }
}
