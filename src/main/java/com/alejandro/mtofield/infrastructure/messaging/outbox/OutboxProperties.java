package com.alejandro.mtofield.infrastructure.messaging.outbox;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import org.springframework.beans.factory.InitializingBean;

import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.outbox")
public class OutboxProperties implements InitializingBean {

    private boolean enabled = true;

    /**
     * Intentos antes de dar un mensaje por perdido (FAILED).
     * <p>
     * El intento se cuenta al RECLAMAR el mensaje, no al fallar: uno que tumba al
     * proceso, o cuyo fallo no se llega a apuntar, tambien los agota.
     * <p>
     * Con backoff exponencial acotado a {@link #maxRetryDelay}, 20 intentos cubren
     * mas de una hora de broker caido. Con el backoff lineal anterior y 10 intentos
     * la ventana era de menos de cuatro minutos: un reinicio de RabbitMQ bastaba
     * para mandar los eventos a FAILED.
     */
    private int maxAttempts = 20;

    /** Retardo del primer reintento. Se duplica en cada intento hasta el tope. */
    private Duration initialRetryDelay = Duration.ofSeconds(5);

    /** Tope del backoff exponencial. */
    private Duration maxRetryDelay = Duration.ofMinutes(5);

    /**
     * Desviacion aleatoria aplicada al retardo, entre 0 y 1.
     * <p>
     * Con 0.2 el retardo real cae en [0.8*d, 1.2*d]. Evita que todas las replicas
     * reintenten a la vez contra un broker que acaba de levantarse.
     */
    private double retryJitter = 0.2;

    private Duration publisherFixedDelay = Duration.ofSeconds(5);

    /** Mensajes que reclama cada lote; una pasada reclama lotes mientras haya trabajo. */
    private int batchSize = 50;

    /**
     * Tiempo que un mensaje reclamado (IN_PROGRESS) permanece invisible para el
     * resto de replicas.
     * <p>
     * Es la red de seguridad ante una caida del proceso entre reclamar y publicar:
     * pasado este plazo otra replica vuelve a cogerlo.
     * <p>
     * Ya NO tiene que cubrir el lote entero ({@code batchSize * confirmTimeout}, que
     * con los valores por defecto eran 500 s contra 300 s): el relay renueva el reclamo
     * de cada mensaje justo antes de publicarlo, y no publica el que ya no es suyo. Lo
     * que si tiene que cubrir es UNA publicacion con holgura y la pasada entera, y eso
     * se comprueba al arrancar ({@link #validate()}).
     */
    private Duration claimVisibilityTimeout = Duration.ofMinutes(5);

    /** Espera maxima por el publisher confirm del broker antes de dar el envio por fallido. */
    private Duration confirmTimeout = Duration.ofSeconds(10);

    /**
     * Tiempo que una pasada del relay sigue reclamando lotes mientras haya trabajo.
     * <p>
     * Con el orden por agregado un lote lleva como mucho UN mensaje de cada agregado,
     * asi que N eventos de un agregado necesitan N lotes: con un lote por pasada salian
     * uno cada {@link #publisherFixedDelay}. La pasada vacia lo que haya y se corta al
     * cumplir este plazo; lo que tuviera reclamado sin empezar lo suelta, para que lo
     * coja la siguiente pasada o cualquier otra replica.
     */
    private Duration passTimeBudget = Duration.ofSeconds(30);

    /** Cada cuanto se refresca la foto del outbox que publican las metricas. */
    private Duration metricsRefreshDelay = Duration.ofSeconds(30);

    /**
     * Publica los mensajes de un mismo agregado en el orden en que se generaron.
     * <p>
     * Un mensaje que falla y se reprograma dejaria pasar por delante al siguiente del
     * mismo agregado, y el consumidor aplicaria el cambio viejo encima del nuevo. Con
     * esto activo, el reclamo retiene a los posteriores mientras el anterior siga sin
     * publicarse.
     * <p>
     * El coste es que un mensaje atascado frena a los de SU agregado (solo a esos).
     * Se puede desactivar si en algun momento interesa vaciar un atasco a costa del
     * orden, pero por defecto pesa mas no corromper el dato.
     */
    private boolean strictOrderingPerAggregate = true;

    /**
     * Publica en cuanto la transaccion de negocio confirma, sin esperar al sondeo.
     * <p>
     * El sondeo sigue existiendo como red de seguridad; esto solo le quita el suelo de
     * latencia, que era de hasta {@code publisher-fixed-delay} para todos los eventos.
     */
    private boolean immediateDispatch = true;

    private Purge purge = new Purge();

    @Override
    public void afterPropertiesSet() {
        validate();
    }

    /**
     * Comprueba, al arrancar, que los plazos no se contradicen. Mejor no arrancar que
     * publicar duplicados y desordenados el dia que el broker deje de confirmar.
     *
     * @throws IllegalStateException con la propiedad que hay que corregir
     */
    public void validate() {
        requirePositive("max-attempts", maxAttempts);
        requirePositive("batch-size", batchSize);
        requirePositive("confirm-timeout", confirmTimeout);
        requirePositive("claim-visibility-timeout", claimVisibilityTimeout);
        requirePositive("pass-time-budget", passTimeBudget);

        if (claimVisibilityTimeout.compareTo(confirmTimeout.multipliedBy(2)) < 0) {
            throw new IllegalStateException(("""
                    app.outbox.claim-visibility-timeout (%s) debe ser al menos el doble de \
                    app.outbox.confirm-timeout (%s): un mensaje se reclama, se espera su \
                    confirmacion y se cierra dentro de su visibilidad; si no cabe con holgura, \
                    otra replica lo reclama mientras tanto y sale duplicado.""")
                    .formatted(claimVisibilityTimeout, confirmTimeout));
        }

        if (passTimeBudget.plus(confirmTimeout).compareTo(claimVisibilityTimeout) > 0) {
            throw new IllegalStateException(("""
                    app.outbox.pass-time-budget (%s) mas app.outbox.confirm-timeout (%s) no puede \
                    superar app.outbox.claim-visibility-timeout (%s): el ultimo mensaje que una \
                    pasada empieza tiene que terminar con su reclamo todavia vigente.""")
                    .formatted(passTimeBudget, confirmTimeout, claimVisibilityTimeout));
        }
    }

    private static void requirePositive(String property, int value) {
        if (value < 1) {
            throw new IllegalStateException(
                    "app.outbox.%s debe ser al menos 1 y vale %d".formatted(property, value));
        }
    }

    private static void requirePositive(String property, Duration value) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalStateException(
                    "app.outbox.%s debe ser una duracion positiva y vale %s".formatted(property, value));
        }
    }

    /**
     * Borrado de los mensajes ya publicados.
     * <p>
     * Sin purga, outbox_message solo crece: cada alta, modificacion y baja de dato
     * maestro deja una fila con su JSON, y las operaciones masivas publican un evento
     * POR ENTIDAD. Son millones de filas de payload dentro de la base transaccional
     * de negocio, no en un almacen historico aparte.
     */
    @Getter
    @Setter
    public static class Purge {

        private boolean enabled = true;

        /**
         * Antiguedad a partir de la cual se borra un mensaje PUBLISHED.
         * <p>
         * Una semana da margen de sobra para investigar un incidente de mensajeria
         * con la tabla todavia delante.
         */
        private Duration retention = Duration.ofDays(7);

        /**
         * Filas por sentencia DELETE.
         * <p>
         * Se borra por lotes y no de una sentencia: un DELETE de millones de filas
         * mantiene una transaccion larguisima, hincha el WAL y bloquea el vacuum.
         */
        private int batchSize = 500;

        /**
         * Lotes por pasada, como tope de trabajo.
         * <p>
         * Si hay mas atrasado, se termina en las siguientes pasadas. Asi la primera
         * ejecucion tras activar la purga sobre una tabla enorme no se convierte en
         * un borrado de horas.
         */
        private int maxBatchesPerRun = 20;

        private Duration fixedDelay = Duration.ofHours(1);
    }
}
