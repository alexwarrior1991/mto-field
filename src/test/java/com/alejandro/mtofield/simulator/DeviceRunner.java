package com.alejandro.mtofield.simulator;

import io.grpc.ManagedChannel;
import io.grpc.Status;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Un dispositivo durante la noche: abre el canal, lo pierde (si se simula la falta de cobertura)
 * y lo reanuda con la ultima orden aplicada, hasta que la posesion se cierra o la simulacion
 * acaba. El estilo de cliente se elige por dispositivo.
 */
final class DeviceRunner {

    private final ManagedChannel channel;
    private final TokenClient tokens;
    private final int team;
    private final int device;
    private final String teamCode;
    private final DeviceScript script;
    private final boolean blocking;
    private final Duration cutEvery;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private volatile DeviceScript.Transport current;

    DeviceRunner(ManagedChannel channel, TokenClient tokens, int team, int device, String teamCode, String deviceId, UUID shiftId, String teamLabel,
                 boolean neverAcks, boolean blocking, Duration cutEvery, Duration heartbeat, BigDecimal startKp, Path stateDir) {
        this.channel = channel;
        this.tokens = tokens;
        this.team = team;
        this.device = device;
        this.teamCode = teamCode;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().name("sim-" + deviceId + "-", 0).factory());
        this.script = new DeviceScript(deviceId, shiftId, teamLabel, neverAcks, heartbeat, startKp, scheduler,
                stateDir == null ? null : new DeviceStateStore(stateDir, deviceId));
        this.blocking = blocking;
        this.cutEvery = cutEvery;
    }

    DeviceScript script() {
        return script;
    }

    /** Corre hasta que se pare o la posesion se cierre (fin normal del stream). */
    void run() {
        script.start();
        ScheduledFuture<?> cutter = null;
        if (!cutEvery.isZero()) {
            cutter = scheduler.scheduleAtFixedRate(() -> {
                DeviceScript.Transport transport = current;
                if (transport != null && !stopped.get()) {
                    Log.info(script.deviceId(), "losing coverage (simulated cut)");
                    transport.cancel();
                }
            }, cutEvery.toMillis(), cutEvery.toMillis(), TimeUnit.MILLISECONDS);
        }
        try {
            while (!stopped.get()) {
                String token = tokens.deviceToken(team, device, teamCode);
                DeviceScript.Session session = blocking ? BlockingDevice.open(channel, token, script) : ObserverDevice.open(channel, token, script);
                current = session.transport();
                Status status = session.ended().get();
                current = null;
                if (status.getCode() == Status.Code.OK) {
                    Log.info(script.deviceId(), "possession closed: the device goes home");
                    return;
                }
                if (stopped.get()) {
                    return;
                }
                if (status.getCode() == Status.Code.UNAUTHENTICATED) {
                    // El token caduco (o fue rechazado): el siguiente intento va con uno nuevo.
                    tokens.invalidate();
                    Log.info(script.deviceId(), "token expired or rejected: renewing it before resuming");
                }
                Thread.sleep(2000);
                Log.info(script.deviceId(), "resuming after #" + script.lastCommandSequence());
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException unexpected) {
            Log.error(script.deviceId(), "device failed: " + unexpected.getCause());
        } catch (IOException noToken) {
            Log.error(script.deviceId(), "device could not get a token: " + noToken.getMessage());
        } finally {
            if (cutter != null) {
                cutter.cancel(false);
            }
            scheduler.shutdownNow();
        }
    }

    void stop() {
        stopped.set(true);
        DeviceScript.Transport transport = current;
        if (transport != null) {
            transport.halfClose();
        }
    }
}
