package com.alejandro.mtofield.infrastructure.messaging.outbox;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.outbox")
public class OutboxProperties {

    private boolean enabled = true;

    /**
     * Intentos antes de dar un mensaje por perdido (FAILED).
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

    /** Mensajes que reclama cada pasada del relay. */
    private int batchSize = 50;

    /**
     * Tiempo que un mensaje reclamado (IN_PROGRESS) permanece invisible para el
     * resto de replicas.
     * <p>
     * Es la red de seguridad ante una caida del proceso entre reclamar y publicar:
     * pasado este plazo otra replica vuelve a cogerlo. Debe ser holgadamente mayor
     * que {@link #confirmTimeout} por el tamano del lote, o dos replicas publicarian
     * el mismo mensaje.
     */
    private Duration claimVisibilityTimeout = Duration.ofMinutes(5);

    /** Espera maxima por el publisher confirm del broker antes de dar el envio por fallido. */
    private Duration confirmTimeout = Duration.ofSeconds(10);

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
