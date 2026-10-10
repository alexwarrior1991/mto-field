package com.alejandro.mtofield.simulator;

import com.alejandro.mtofield.grpc.v1.ClearOfTrack;
import com.alejandro.mtofield.grpc.v1.CommandAck;
import com.alejandro.mtofield.grpc.v1.EventResult;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.grpc.v1.Heartbeat;
import com.alejandro.mtofield.grpc.v1.Join;
import com.alejandro.mtofield.grpc.v1.SyncResult;
import com.alejandro.mtofield.grpc.v1.TaskCompleted;
import com.alejandro.mtofield.grpc.v1.TaskStarted;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.google.protobuf.Timestamp;
import io.grpc.Status;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lo que hace un dispositivo, sea cual sea el estilo de cliente: se une con su ultima secuencia
 * aplicada, late cada N segundos avanzando su kp, empieza y acaba una tarea, acusa el desalojo
 * con un retraso aleatorio (salvo el equipo que nunca acusa) y sale de la via despues.
 *
 * <p>Comprueba la bajada: una secuencia menor o igual que la ultima aplicada es un duplicado y se
 * cuenta como error. Un hueco no lo es: el dispositivo ve un subconjunto de la secuencia de la
 * posesion (los resultados de los eventos de otros turnos tambien toman numero).</p>
 *
 * <p>Lo que sube se recuerda hasta que llega su {@code EventResult}; al reanudar, el
 * {@code Welcome} dice hasta donde guardo el servidor ({@code last_applied_sequence}, contigua) y
 * se reenvia solo lo que queda por encima.</p>
 */
final class DeviceScript {

    /** El estilo de cliente que lleva los mensajes: observador con onReady, o bloqueante. */
    interface Transport {
        void send(TeamMessage message);

        void cancel();

        void halfClose();

        /** Sube el atraso por SyncBufferedEvents, en otra llamada; contesta con el resultado o con el fallo. */
        void uploadBacklog(List<TeamMessage> backlog, java.util.function.Consumer<SyncResult> onResult, java.util.function.Consumer<Status> onFailure);
    }

    /** Un canal abierto: su transporte y como acabo. */
    record Session(Transport transport, java.util.concurrent.CompletableFuture<Status> ended) {
    }

    private final String deviceId;
    private final UUID shiftId;
    private final String teamLabel;
    private final boolean neverAcks;
    private final Duration heartbeatEvery;
    private final ScheduledExecutorService scheduler;
    private final Random random = new Random();
    private final Map<Long, TeamMessage> unconfirmed = new LinkedHashMap<>();
    /** Los eventos de trabajo que esperan el canal del atraso, en orden (la regla de protocolo). */
    private final List<TeamMessage> backlog = new ArrayList<>();
    private final AtomicInteger duplicates = new AtomicInteger();
    private final AtomicInteger rejected = new AtomicInteger();
    private final AtomicInteger synced = new AtomicInteger();

    private long sequence;
    private long lastCommandSequence;
    private BigDecimal kp;
    private int battery = 95;
    private boolean taskStarted;
    private boolean taskCompleted;
    private boolean evacuating;
    private boolean clear;
    private boolean syncing;
    private Transport transport;
    private String possessionId;
    private final DeviceStateStore store;

    DeviceScript(String deviceId, UUID shiftId, String teamLabel, boolean neverAcks, Duration heartbeatEvery, BigDecimal startKp,
                 ScheduledExecutorService scheduler) {
        this(deviceId, shiftId, teamLabel, neverAcks, heartbeatEvery, startKp, scheduler, null);
    }

    /**
     * @param store donde este dispositivo guarda su contador, su ultima orden y sus subidas sin confirmar (fase 5), o nulo:
     *              entonces cada arranque sigue por detras de la marca que el servidor da en el Welcome
     */
    DeviceScript(String deviceId, UUID shiftId, String teamLabel, boolean neverAcks, Duration heartbeatEvery, BigDecimal startKp,
                 ScheduledExecutorService scheduler, DeviceStateStore store) {
        this.deviceId = deviceId;
        this.shiftId = shiftId;
        this.teamLabel = teamLabel;
        this.neverAcks = neverAcks;
        this.heartbeatEvery = heartbeatEvery;
        this.kp = startKp;
        this.scheduler = scheduler;
        this.store = store;
        if (store != null) {
            store.load().ifPresent(saved -> {
                sequence = saved.sequence();
                lastCommandSequence = saved.lastCommandSequence();
                saved.unconfirmed().forEach(message -> unconfirmed.put(message.getSequence(), message));
                taskStarted = saved.unconfirmed().stream().anyMatch(TeamMessage::hasTaskStarted) || sequence > 0;
                taskCompleted = saved.unconfirmed().stream().anyMatch(TeamMessage::hasTaskCompleted) || sequence > 0;
                log("resuming from " + store.file().getFileName() + ": my uploads up to #" + sequence + ", last command #" + lastCommandSequence
                        + ", " + unconfirmed.size() + " upload(s) unconfirmed");
            });
        }
    }

    String deviceId() {
        return deviceId;
    }

    int duplicates() {
        return duplicates.get();
    }

    int rejected() {
        return rejected.get();
    }

    /** Eventos de trabajo que subieron por SyncBufferedEvents. */
    int synced() {
        return synced.get();
    }

    UUID shiftId() {
        return shiftId;
    }

    synchronized boolean isClear() {
        return clear;
    }

    synchronized long lastCommandSequence() {
        return lastCommandSequence;
    }

    /** Arranca lo periodico: el latido y la tarea del turno. */
    void start() {
        scheduler.scheduleAtFixedRate(this::heartbeat, 2, heartbeatEvery.toSeconds(), TimeUnit.SECONDS);
        scheduler.schedule(() -> upload(builder -> builder.setTaskStarted(TaskStarted.newBuilder().setOrderId("MO-SIM").setTaskId("task-" + deviceId))), 5,
                TimeUnit.SECONDS);
        scheduler.schedule(() -> upload(builder -> builder.setTaskCompleted(TaskCompleted.newBuilder().setOrderId("MO-SIM").setTaskId("task-" + deviceId)
                .addTaskTypeCodes("RG-01").setWorkComplete(true).setNotes("simulated"))), 25, TimeUnit.SECONDS);
    }

    /** El transporte esta abierto: lo primero es el Join con la ultima orden aplicada. */
    synchronized void onConnected(Transport opened) {
        transport = opened;
        log("joining shift " + shiftId + " after #" + lastCommandSequence + (unconfirmed.isEmpty() ? "" : ", " + unconfirmed.size() + " upload(s) unconfirmed"));
        opened.send(message(0).setJoin(Join.newBuilder().setShiftId(shiftId.toString()).setLastCommandSequence(lastCommandSequence)).build());
    }

    synchronized void onDisconnected(Status status) {
        transport = null;
        log("stream ended: " + status.getCode() + (status.getDescription() == null ? "" : " " + status.getDescription()));
    }

    synchronized void onCommand(FieldCommand command) {
        if (command.hasWelcome()) {
            possessionId = command.getWelcome().getPossessionId();
            long watermark = command.getWelcome().getLastAppliedSequence();
            log("welcome: possession " + possessionId + ", server has my uploads up to #" + watermark);
            if (watermark > sequence) {
                // Un dispositivo sin contador propio (sin --state-dir, o con el fichero perdido) sigue
                // despues de lo que el servidor ya guarda con su id: si empezara en #1 repetiria los
                // numeros de una noche anterior y cada subida seria un duplicado.
                log((sequence == 0 ? "no counter of my own" : "the server is ahead of my counter (#" + sequence + ")") + ": continuing after #" + watermark);
                sequence = watermark;
            }
            List<Long> stored = new ArrayList<>();
            for (Map.Entry<Long, TeamMessage> entry : unconfirmed.entrySet()) {
                if (entry.getKey() <= watermark) {
                    stored.add(entry.getKey());
                } else if (isWork(entry.getValue())) {
                    // Un evento de trabajo sin confirmar es atraso: va por SyncBufferedEvents, no por aqui.
                    if (backlog.stream().noneMatch(buffered -> buffered.getSequence() == entry.getKey())) {
                        backlog.add(entry.getValue());
                    }
                } else if (transport != null) {
                    log("resending upload #" + entry.getKey());
                    transport.send(entry.getValue());
                }
            }
            stored.forEach(unconfirmed::remove);
            backlog.removeIf(buffered -> buffered.getSequence() <= watermark);
            backlog.sort(java.util.Comparator.comparingLong(TeamMessage::getSequence));
            persist();
            uploadBacklogIfAny();
            return;
        }
        if (command.getSequence() <= lastCommandSequence) {
            duplicates.incrementAndGet();
            Log.error(deviceId, "DUPLICATE or out-of-order command #" + command.getSequence() + " after #" + lastCommandSequence + " (" + command.getCommandCase() + ")");
            return;
        }
        lastCommandSequence = command.getSequence();
        persist();
        switch (command.getCommandCase()) {
            case EVACUATE_NOW -> {
                evacuating = true;
                if (neverAcks) {
                    log("EVACUATE_NOW #" + command.getSequence() + " received; this team never acknowledges (simulated)");
                    return;
                }
                long ackDelay = 1 + random.nextInt(4);
                long clearDelay = ackDelay + 3 + random.nextInt(6);
                log("EVACUATE_NOW #" + command.getSequence() + " (" + command.getEvacuateNow().getReason() + "): ack in " + ackDelay + "s, clear in " + clearDelay + "s");
                scheduler.schedule(() -> upload(builder -> builder.setCommandAck(CommandAck.newBuilder().setCommandId(command.getCommandId()).setAccepted(true))),
                        ackDelay, TimeUnit.SECONDS);
                scheduler.schedule(() -> {
                    synchronized (this) {
                        clear = true;
                    }
                    upload(builder -> builder.setClearOfTrack(ClearOfTrack.newBuilder().setEarthingRemoved(true)));
                }, clearDelay, TimeUnit.SECONDS);
            }
            case SUPERVISOR_MESSAGE -> log("message #" + command.getSequence() + " from " + command.getSupervisorMessage().getAuthor() + ": "
                    + command.getSupervisorMessage().getText());
            case WINDOW_CHANGED -> log("window changed #" + command.getSequence() + ": ends at " + command.getWindowChanged().getEndsAt().getSeconds());
            case EVENT_RESULT -> {
                EventResult result = command.getEventResult();
                TeamMessage answered = unconfirmed.remove(result.getSequence());
                if (result.getOutcome() == EventResult.Outcome.REJECTED && result.getReason().startsWith("BACKLOG_PENDING")) {
                    // El servidor dice que hay atraso por debajo: ese evento vuelve a la cola del atraso.
                    if (answered != null) {
                        unconfirmed.put(result.getSequence(), answered);
                        backlog.add(answered);
                        backlog.sort(java.util.Comparator.comparingLong(TeamMessage::getSequence));
                        uploadBacklogIfAny();
                    }
                } else if (result.getOutcome() == EventResult.Outcome.REJECTED) {
                    rejected.incrementAndGet();
                }
                persist();
                log("upload #" + result.getSequence() + " -> " + result.getOutcome() + (result.getReason().isBlank() ? "" : " (" + result.getReason() + ")")
                        + " [command #" + command.getSequence() + "]");
            }
            case WELCOME, COMMAND_NOT_SET -> log("unexpected command " + command.getCommandCase());
        }
    }

    private synchronized void heartbeat() {
        if (transport == null || clear) {
            return;
        }
        kp = kp.add(new BigDecimal("0.050")).setScale(3, RoundingMode.HALF_UP);
        battery = Math.max(5, battery - 1);
        transport.send(message(0).setHeartbeat(Heartbeat.newBuilder().setKp(kp.toPlainString()).setBatteryPct(battery).setSignalDbm(-80 - random.nextInt(30))).build());
    }

    private synchronized void upload(java.util.function.UnaryOperator<TeamMessage.Builder> body) {
        if (taskStartedTwice(body)) {
            return;
        }
        long next = ++sequence;
        TeamMessage message = body.apply(message(next)).build();
        unconfirmed.put(next, message);
        // Antes de enviar: si el proceso muere con el mensaje en vuelo, el contador ya cuenta con el.
        persist();
        if (transport == null) {
            if (isWork(message)) {
                backlog.add(message);
            }
            log("upload #" + next + " " + message.getEventCase() + " buffered: no coverage");
        } else if (isWork(message) && (!backlog.isEmpty() || syncing)) {
            // La regla de protocolo: mientras hay atraso, el trabajo va solo por SyncBufferedEvents y en orden.
            backlog.add(message);
            log("upload #" + next + " " + message.getEventCase() + " buffered: backlog pending");
        } else {
            transport.send(message);
            log("upload #" + next + " " + message.getEventCase());
        }
    }

    /** Lo que sobrevive a un reinicio, entero y cada vez que cambia; sin --state-dir no hace nada. */
    private void persist() {
        if (store != null) {
            store.save(new DeviceStateStore.State(sequence, lastCommandSequence, List.copyOf(unconfirmed.values())));
        }
    }

    private static boolean isWork(TeamMessage message) {
        return message.hasTaskStarted() || message.hasTaskCompleted();
    }

    /** Con transporte y atraso, una subida por SyncBufferedEvents; mientras dura, el trabajo nuevo se acumula detras. */
    private synchronized void uploadBacklogIfAny() {
        if (transport == null || syncing || backlog.isEmpty()) {
            return;
        }
        syncing = true;
        List<TeamMessage> batch = List.copyOf(backlog);
        log("uploading backlog of " + batch.size() + " work event(s) through SyncBufferedEvents (#" + batch.getFirst().getSequence() + "..#"
                + batch.getLast().getSequence() + ")");
        transport.uploadBacklog(batch, result -> onBacklogUploaded(batch, result), status -> onBacklogFailed(batch, status));
    }

    private synchronized void onBacklogUploaded(List<TeamMessage> batch, SyncResult result) {
        syncing = false;
        log("backlog uploaded: " + result.getApplied() + " applied, " + result.getDuplicates() + " duplicate(s), " + result.getRejected()
                + " rejected; server has my uploads up to #" + result.getLastAppliedSequence());
        synced.addAndGet(result.getApplied());
        if (result.getRejected() == 0) {
            backlog.removeAll(batch);
        } else {
            backlog.removeIf(buffered -> buffered.getSequence() <= result.getLastAppliedSequence());
        }
        uploadBacklogIfAny();
    }

    private synchronized void onBacklogFailed(List<TeamMessage> batch, Status status) {
        syncing = false;
        log("backlog upload of " + batch.size() + " event(s) failed: " + status.getCode() + (status.getDescription() == null ? "" : " " + status.getDescription())
                + "; it stays buffered");
        scheduler.schedule(this::uploadBacklogIfAny, 5, TimeUnit.SECONDS);
    }

    /** La tarea empieza y acaba una vez aunque el planificador repita. */
    private boolean taskStartedTwice(java.util.function.UnaryOperator<TeamMessage.Builder> body) {
        TeamMessage probe = body.apply(TeamMessage.newBuilder()).build();
        if (probe.hasTaskStarted()) {
            if (taskStarted) {
                return true;
            }
            taskStarted = true;
        }
        if (probe.hasTaskCompleted()) {
            if (taskCompleted) {
                return true;
            }
            taskCompleted = true;
        }
        return false;
    }

    private TeamMessage.Builder message(long withSequence) {
        Instant now = Instant.now();
        return TeamMessage.newBuilder().setDeviceId(deviceId).setSequence(withSequence)
                .setOccurredAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()));
    }

    private void log(String message) {
        Log.info(deviceId, "[" + teamLabel + "] " + message);
    }
}
