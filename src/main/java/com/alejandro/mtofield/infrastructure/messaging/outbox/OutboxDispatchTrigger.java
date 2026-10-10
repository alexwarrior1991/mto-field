package com.alejandro.mtofield.infrastructure.messaging.outbox;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Despierta al relay en cuanto hay algo que publicar, sin esperar a la siguiente
 * pasada del planificador.
 * <p>
 * El sondeo cada 5 segundos es la red de seguridad, pero tambien es el suelo de
 * latencia de TODOS los eventos: un cambio de dato maestro tardaba entre 0 y 5
 * segundos en salir por motivos que no tienen nada que ver con el negocio.
 * <p>
 * Las peticiones se agrupan: un bulkCreate de mil entidades escribe mil mensajes y
 * pide mil despertares, pero basta con uno. El indicador se libera ANTES de ejecutar,
 * de modo que lo que se guarde mientras el relay trabaja provoca otra pasada en lugar
 * de perderse hasta el siguiente sondeo.
 */
@Slf4j
public class OutboxDispatchTrigger {

    private final Executor executor;
    private final Runnable dispatch;
    private final AtomicBoolean pending = new AtomicBoolean();

    public OutboxDispatchTrigger(Executor executor, Runnable dispatch) {
        this.executor = executor;
        this.dispatch = dispatch;
    }

    public void requestDispatch() {
        if (!pending.compareAndSet(false, true)) {
            // Ya hay una pasada encargada que todavia no ha empezado: se suma a ella.
            return;
        }

        try {
            executor.execute(this::runDispatch);
        } catch (RejectedExecutionException exception) {
            pending.set(false);
            log.debug("Despertar del outbox descartado; lo recogera el planificador", exception);
        }
    }

    private void runDispatch() {
        pending.set(false);

        try {
            dispatch.run();
        } catch (Exception exception) {
            // El planificador vuelve a pasar de todas formas: un fallo aqui no puede
            // escalar a ningun sitio, y menos al hilo de la peticion que lo origino.
            log.warn("Error en la publicacion inmediata del outbox", exception);
        }
    }
}
