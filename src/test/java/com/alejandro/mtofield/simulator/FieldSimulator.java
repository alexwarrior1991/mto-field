package com.alejandro.mtofield.simulator;

import com.alejandro.mtofield.grpc.v1.Possession;
import com.alejandro.mtofield.grpc.v1.PossessionBoard;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * El simulador de equipos y responsable, sin Spring: una herramienta para ver el canal en marcha.
 *
 * <pre>
 * ./mvnw -q test-compile exec:java -Dexec.classpathScope=test \
 *     -Dexec.args="--mode demo --target localhost:9094 --user campo.responsable --password local --teams 3 --never-ack-team 3 --cut-every 15s"
 * </pre>
 *
 * <p>{@code demo} abre una posesion, levanta los equipos (un estilo de cliente u otro, o
 * alternados con {@code --mixed}), ordena el desalojo a los X segundos y cierra. {@code supervisor}
 * y {@code device} hacen cada mitad en JVMs distintas ({@code --shifts} con los ids que imprime el
 * responsable). Sin Keycloak, {@code --local-issuer} sirve el JWK Set de la clave de test y acuna
 * los tokens; el servidor se arranca con {@code KEYCLOAK_ISSUER_URI=http://localhost:8082/realms/mto}.</p>
 *
 * <p>Sale con codigo 1 si algun dispositivo recibio una orden duplicada o fuera de orden.</p>
 */
public final class FieldSimulator {

    private FieldSimulator() {
    }

    public static void main(String[] args) throws Exception {
        SimulatorOptions options;
        try {
            options = SimulatorOptions.parse(args);
        } catch (IllegalArgumentException invalid) {
            System.err.println(invalid.getMessage());
            System.exit(2);
            return;
        }
        // Un canal por replica: el responsable habla con la primera y los dispositivos se reparten
        // entre todas (fase 4: una posesion repartida entre replicas, el tablero desde cualquiera).
        List<ManagedChannel> channels = options.targets().stream()
                .map(target -> ManagedChannelBuilder.forTarget(target.trim()).usePlaintext()
                        .keepAliveTime(20, TimeUnit.SECONDS).keepAliveTimeout(10, TimeUnit.SECONDS).keepAliveWithoutCalls(true)
                        .build())
                .toList();
        if (channels.size() > 1) {
            Log.info("simulator", channels.size() + " replicas: the supervisor on " + options.targets().getFirst() + ", the devices spread over all of them");
        }
        int exitCode;
        try (TokenClient tokens = new TokenClient(options)) {
            exitCode = switch (options.mode()) {
                case "demo" -> demo(channels, tokens, options);
                case "supervisor" -> supervisor(channels.getFirst(), tokens, options);
                case "device" -> devices(channels, tokens, options, options.shiftIds(), options.duration());
                default -> {
                    System.err.println("Unknown mode " + options.mode() + "\n" + SimulatorOptions.USAGE);
                    yield 2;
                }
            };
        } finally {
            for (ManagedChannel channel : channels) {
                channel.shutdownNow();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
        }
        System.exit(exitCode);
    }

    private static int demo(List<ManagedChannel> channels, TokenClient tokens, SimulatorOptions options) throws Exception {
        SupervisorConsole supervisor = new SupervisorConsole(channels.getFirst(), tokens);
        List<UUID> shiftIds = options.shiftIds();
        Possession possession = supervisor.open(shiftIds);
        supervisor.watch(possession.getId());
        Thread runner = Thread.ofVirtual().name("sim-devices").start(() -> {
            try {
                devices(channels, tokens, options, shiftIds, options.duration());
            } catch (Exception failed) {
                Log.error("devices", failed.toString());
            }
        });
        Thread.sleep(options.evacuateAfter().toMillis());
        supervisor.say(possession.getId(), "Cerramos en breve: recoged herramienta");
        supervisor.evacuate(possession.getId(), "Fin de la ventana de trabajos");
        int errors = waitAndClose(supervisor, possession, options.duration().minus(options.evacuateAfter()));
        runner.join(TimeUnit.SECONDS.toMillis(15));
        supervisor.stopWatching();
        return errors;
    }

    private static int supervisor(ManagedChannel channel, TokenClient tokens, SimulatorOptions options) throws Exception {
        SupervisorConsole supervisor = new SupervisorConsole(channel, tokens);
        Possession possession = supervisor.open(options.shiftIds());
        supervisor.watch(possession.getId());
        Log.info("supervisor", "start the devices with: --mode device --shifts " + String.join(",", possession.getShiftIdsList()));
        Thread.sleep(options.evacuateAfter().toMillis());
        supervisor.evacuate(possession.getId(), "Fin de la ventana de trabajos");
        int result = waitAndClose(supervisor, possession, options.duration().minus(options.evacuateAfter()));
        supervisor.stopWatching();
        return result;
    }

    /** Espera a que todos salgan de la via (o a que se acabe el tiempo) y cierra, forzado si hace falta. */
    private static int waitAndClose(SupervisorConsole supervisor, Possession possession, Duration patience) throws InterruptedException {
        Instant deadline = Instant.now().plus(patience.isNegative() ? Duration.ofSeconds(30) : patience);
        while (Instant.now().isBefore(deadline)) {
            PossessionBoard board = supervisor.latestBoard();
            if (board != null && board.getAllClear()) {
                supervisor.close(possession.getId(), false, "");
                return 0;
            }
            Thread.sleep(1000);
        }
        PossessionBoard board = supervisor.latestBoard();
        String pending = board == null || board.getCommandsCount() == 0 ? "?" : String.join(",", board.getCommands(board.getCommandsCount() - 1).getPendingList());
        supervisor.close(possession.getId(), true, "Simulation over; teams still on the track: " + pending);
        return 0;
    }

    private static int devices(List<ManagedChannel> channels, TokenClient tokens, SimulatorOptions options, List<UUID> shiftIds, Duration duration) throws Exception {
        List<DeviceRunner> runners = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        int next = 0;
        for (int team = 1; team <= shiftIds.size(); team++) {
            for (int device = 1; device <= options.devicesPerTeam(); device++) {
                boolean blocking = options.blocking() || (options.mixed() && (team + device) % 2 == 0);
                String deviceId = "sim-t" + team + "-d" + device;
                ManagedChannel channel = channels.get(next++ % channels.size());
                String replica = channels.size() > 1 ? " on " + options.targets().get((next - 1) % channels.size()).trim() : "";
                DeviceRunner runner = new DeviceRunner(channel, tokens, team, device, options.teamCodeOf(team, shiftIds.get(team - 1)), deviceId, shiftIds.get(team - 1),
                        "team " + team + (blocking ? " blocking" : " observer") + replica, team == options.neverAckTeam(), blocking, options.cutEvery(),
                        options.heartbeat(), new BigDecimal("30.000").add(new BigDecimal(team)), options.stateDir());
                runners.add(runner);
                threads.add(Thread.ofVirtual().name("sim-runner-" + deviceId).start(runner::run));
            }
        }
        for (Thread thread : threads) {
            thread.join(duration.plusSeconds(20).toMillis());
        }
        runners.forEach(DeviceRunner::stop);
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(5));
        }
        int duplicates = runners.stream().mapToInt(runner -> runner.script().duplicates()).sum();
        int rejected = runners.stream().mapToInt(runner -> runner.script().rejected()).sum();
        int synced = runners.stream().mapToInt(runner -> runner.script().synced()).sum();
        long clear = runners.stream().filter(runner -> runner.script().isClear()).count();
        Log.info("devices", runners.size() + " device(s) done: " + clear + " clear of track, " + synced + " work event(s) uploaded as backlog, "
                + rejected + " upload(s) rejected, " + duplicates + " duplicate/out-of-order command(s)");
        return duplicates == 0 ? 0 : 1;
    }
}
