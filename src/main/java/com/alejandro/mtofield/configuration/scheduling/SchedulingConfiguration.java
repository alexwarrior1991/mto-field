package com.alejandro.mtofield.configuration.scheduling;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.task.ThreadPoolTaskSchedulerBuilder;
import org.springframework.boot.thread.Threading;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

/**
 * El planificador de los {@code @Scheduled}: un pool de {@code spring.task.scheduling.pool.size} (4)
 * hilos, virtuales con {@code spring.threads.virtual.enabled}, para que ninguna pasada espere a otra.
 * Es el de mto-notification.
 *
 * <p>Aqui corren el tic del tablero, el tic de puesta al dia entre replicas (cada 2 s, la garantia
 * de correccion cuando el bus falla), el barrido de tokens caducados, el reintento de los eventos de
 * tarea, el vigilante de acuses y el relay, las metricas y la purga del outbox. Con los hilos
 * virtuales, Boot planifica con un {@code SimpleAsyncTaskScheduler}, que corre todas las tareas
 * {@code fixedDelay} en su unico hilo de planificacion: un reintento esperando a un mto-maintenance
 * lento (hasta el tiempo limite del circuito por llamada) o un relay esperando la confirmacion del
 * broker paraban la puesta al dia entre replicas y el cierre de los streams con el token caducado.
 * Sin ellos, Boot usa un {@code ThreadPoolTaskScheduler} de un hilo si nadie fija el tamano. Aqui el
 * pool es el mismo en los dos casos: cada tarea sigue sin solaparse consigo misma (el siguiente
 * {@code fixedDelay} se cuenta desde que termina la pasada anterior), pero las demas no la esperan.</p>
 *
 * <p>El tic de puesta al dia no tiene un planificador aparte: lo que puede esperar a la red son dos
 * tareas (el reintento y el relay), y las demas solo esperan a la base, sin la cual el tic tampoco
 * podria releer nada. Con cuatro hilos le queda sitio; si se anaden tareas que esperen fuera, se
 * sube {@code SPRING_TASK_SCHEDULING_POOL_SIZE}.</p>
 *
 * <p>El planificador no es un bean a proposito: un {@code ThreadPoolTaskScheduler} es tambien un
 * {@code Executor}, y otro {@code Executor} en el contexto cambiaria lo que Boot autoconfigura y lo
 * que se inyecta por tipo. Boot sigue creando su {@code taskScheduler}, en el que ya no se planifica
 * nada. Este se cierra con el contexto. El hilo {@code outbox-dispatch} del despacho inmediato del
 * outbox no pasa por aqui: sigue siendo un hilo de plataforma propio.</p>
 */
@Configuration(proxyBeanMethods = false)
public class SchedulingConfiguration implements SchedulingConfigurer, DisposableBean {

    private final ThreadPoolTaskScheduler scheduler;

    public SchedulingConfiguration(ThreadPoolTaskSchedulerBuilder builder, Environment environment) {
        ThreadPoolTaskScheduler built = builder.build();
        if (Threading.VIRTUAL.isActive(environment)) {
            built.setThreadFactory(Thread.ofVirtual().name(built.getThreadNamePrefix(), 0).factory());
        }
        built.initialize();
        this.scheduler = built;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        registrar.setTaskScheduler(scheduler);
    }

    @Override
    public void destroy() {
        scheduler.destroy();
    }
}
