package com.alejandro.mtofield.infrastructure.messaging.outbox;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class OutboxDispatchTriggerTest {

    /** Ejecuta en el mismo hilo, para poder razonar sobre el orden. */
    private static final java.util.concurrent.Executor DIRECTO = Runnable::run;

    @Test
    void unaPeticionProvocaUnaPasadaDelRelay() {
        AtomicInteger pasadas = new AtomicInteger();

        new OutboxDispatchTrigger(DIRECTO, pasadas::incrementAndGet).requestDispatch();

        assertThat(pasadas).hasValue(1);
    }

    @Test
    void milPeticionesSeAgrupanEnUnaSolaTareaEncolada() {
        // Un bulkCreate de mil entidades escribe mil mensajes y pide mil despertares:
        // encolarlos todos seria mil pasadas del relay para el mismo trabajo.
        //
        // El ejecutor guarda las tareas sin ejecutarlas, en vez de usar un hilo real:
        // asi se mide la agrupacion sin depender de si ese hilo llega a arrancar antes
        // o despues de que termine el bucle, que es una carrera y no un invariante.
        List<Runnable> encoladas = new ArrayList<>();
        AtomicInteger pasadas = new AtomicInteger();

        OutboxDispatchTrigger trigger = new OutboxDispatchTrigger(encoladas::add, pasadas::incrementAndGet);

        for (int i = 0; i < 1000; i++) {
            trigger.requestDispatch();
        }

        assertThat(encoladas)
                .as("mil peticiones antes de que arranque la pasada son una sola tarea")
                .hasSize(1);

        encoladas.forEach(Runnable::run);
        assertThat(pasadas).hasValue(1);
    }

    @Test
    void conUnHiloRealMilPeticionesNoPuedenPasarDeDosPasadas() throws Exception {
        // Con un ejecutor de verdad el numero exacto NO es determinista, y afirmar que
        // es uno seria un test que falla segun la maquina. El invariante real es la
        // COTA: el indicador es uno solo, asi que como mucho hay una pasada corriendo
        // y otra encolada. Nunca mil, que es lo que se quiere evitar.
        AtomicInteger pasadas = new AtomicInteger();
        CountDownLatch arranca = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        OutboxDispatchTrigger trigger = new OutboxDispatchTrigger(executor, () -> {
            try {
                arranca.await();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
            pasadas.incrementAndGet();
        });

        for (int i = 0; i < 1000; i++) {
            trigger.requestDispatch();
        }
        arranca.countDown();

        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        assertThat(pasadas.get())
                .as("mil peticiones no pueden convertirse en mil pasadas del relay")
                .isBetween(1, 2);
    }

    @Test
    void loQueSeGuardaMientrasElRelayTrabajaProvocaOtraPasada() {
        // El indicador se libera ANTES de ejecutar justamente por esto: si se liberara
        // al terminar, un mensaje escrito durante la pasada se quedaria esperando al
        // siguiente sondeo, que es la latencia que se intenta quitar.
        AtomicInteger pasadas = new AtomicInteger();
        OutboxDispatchTrigger[] referencia = new OutboxDispatchTrigger[1];

        referencia[0] = new OutboxDispatchTrigger(DIRECTO, () -> {
            if (pasadas.incrementAndGet() == 1) {
                referencia[0].requestDispatch();
            }
        });

        referencia[0].requestDispatch();

        assertThat(pasadas).hasValue(2);
    }

    @Test
    void unFalloPublicandoNoEscapaHaciaElHiloQueLoPidio() {
        // Quien pide el despertar es la transaccion de negocio que acaba de confirmar:
        // un error del relay no puede llegarle de vuelta.
        OutboxDispatchTrigger trigger = new OutboxDispatchTrigger(DIRECTO, () -> {
            throw new IllegalStateException("broker caido");
        });

        assertThatCode(trigger::requestDispatch).doesNotThrowAnyException();
    }

    @Test
    void siElEjecutorRechazaLaTareaElPlanificadorSigueSiendoLaRed() {
        OutboxDispatchTrigger trigger = new OutboxDispatchTrigger(
                runnable -> {
                    throw new RejectedExecutionException("saturado");
                },
                () -> { });

        assertThatCode(trigger::requestDispatch).doesNotThrowAnyException();
    }

    @Test
    void trasUnRechazoSeVuelveAAdmitirLaSiguientePeticion() {
        // Si el indicador se quedara puesto, un rechazo puntual dejaria el despertar
        // inmediato inutilizado para siempre.
        AtomicInteger pasadas = new AtomicInteger();
        boolean[] rechaza = {true};

        OutboxDispatchTrigger trigger = new OutboxDispatchTrigger(
                runnable -> {
                    if (rechaza[0]) {
                        throw new RejectedExecutionException("saturado");
                    }
                    runnable.run();
                },
                pasadas::incrementAndGet);

        trigger.requestDispatch();
        rechaza[0] = false;
        trigger.requestDispatch();

        assertThat(pasadas).hasValue(1);
    }
}
