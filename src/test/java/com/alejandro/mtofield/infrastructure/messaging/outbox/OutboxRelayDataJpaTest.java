package com.alejandro.mtofield.infrastructure.messaging.outbox;

import com.alejandro.mtofield.support.PostgreSQLTestContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

/**
 * Tests del relay contra PostgreSQL real (los de {@code mto-configuration}, sobre la misma copia
 * del outbox): lo que se prueba aqui (FOR UPDATE SKIP LOCKED, bloqueos entre transacciones,
 * limites de columna, la secuencia que asigna la base) no lo reproduce ni un mock ni H2, y de paso
 * {@code ddl-auto: validate} comprueba la entidad contra lo que deja {@code V13}.
 * <p>
 * La clase corre SIN transaccion de test: {@code @DataJpaTest} envuelve cada test en
 * una transaccion que hace rollback, y con eso los tests de concurrencia no verian
 * nada y el propio SKIP LOCKED seria indistinguible de una serializacion.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ImportAutoConfiguration(FlywayAutoConfiguration.class)
@Import(OutboxRelayDataJpaTest.OutboxTestConfiguration.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class OutboxRelayDataJpaTest extends PostgreSQLTestContainer {

    @Autowired
    private OutboxMessageRepository outboxMessageRepository;

    @Autowired
    private OutboxRelayService outboxRelayService;

    @Autowired
    private OutboxAdminService outboxAdminService;

    @Autowired
    private OutboxProperties outboxProperties;

    @Autowired
    private OutboxPublisherScheduler outboxPublisherScheduler;

    /** El broker no entra en estos tests: lo que se comprueba es lo que queda en la tabla. */
    @Autowired
    private OutboxRabbitPublisher outboxRabbitPublisher;

    @Autowired
    private OutboxPurgeScheduler outboxPurgeScheduler;

    @Autowired
    private OutboxService outboxService;

    @Autowired
    private io.micrometer.tracing.Tracer tracer;

    @Autowired
    private org.springframework.transaction.PlatformTransactionManager transactionManager;

    @Autowired
    private DataSource dataSource;

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registerPostgreSQLProperties(registry);
    }

    @BeforeEach
    void resetState() {
        reset(outboxRabbitPublisher);
        outboxMessageRepository.deleteAll();

        outboxProperties.setBatchSize(50);
        outboxProperties.setMaxAttempts(20);
        outboxProperties.setInitialRetryDelay(Duration.ofSeconds(5));
        outboxProperties.setMaxRetryDelay(Duration.ofMinutes(5));
        outboxProperties.setRetryJitter(0d);
        outboxProperties.setClaimVisibilityTimeout(Duration.ofMinutes(5));
        outboxProperties.setStrictOrderingPerAggregate(true);
        outboxProperties.getPurge().setRetention(Duration.ofDays(7));
        outboxProperties.getPurge().setBatchSize(500);
        outboxProperties.getPurge().setMaxBatchesPerRun(20);
    }

    // ------------------------------------------------------------------ reclamo

    @Test
    void reclamarDejaElLoteEnProgresoEInvisibleParaElRestoDeReplicas() {
        List<UUID> ids = persistPending(3);

        List<OutboxRecord> primerLote = outboxRelayService.claimBatch();

        assertThat(primerLote).extracting(OutboxRecord::id).containsExactlyInAnyOrderElementsOf(ids);
        assertThat(outboxRelayService.claimBatch())
                .as("un mensaje ya reclamado no puede volver a entregarse mientras dure su visibilidad")
                .isEmpty();

        assertThat(outboxMessageRepository.findAll())
                .allSatisfy(message -> {
                    assertThat(message.getStatus()).isEqualTo(OutboxStatus.IN_PROGRESS);
                    assertThat(message.getNextAttemptAt()).isAfter(Instant.now());
                });
    }

    @Test
    void reclamarRespetaElTamanoDeLote() {
        persistPending(10);
        outboxProperties.setBatchSize(4);

        assertThat(outboxRelayService.claimBatch()).hasSize(4);
        assertThat(outboxRelayService.claimBatch()).hasSize(4);
        assertThat(outboxRelayService.claimBatch()).hasSize(2);
    }

    @Test
    void noSeReclamaLoQueTodaviaNoTocaReintentar() {
        persistPending(1);
        persist(message -> {
            message.setStatus(OutboxStatus.PENDING);
            message.setNextAttemptAt(Instant.now().plus(Duration.ofHours(1)));
        });

        assertThat(outboxRelayService.claimBatch()).hasSize(1);
    }

    @Test
    void unMensajeSinFechaDeReintentoNoSeQuedaAtascado() {
        // next_attempt_at admite nulos; con una comparacion directa esa fila jamas
        // entraria en el lote y se quedaria ahi para siempre.
        UUID id = persist(message -> {
            message.setStatus(OutboxStatus.PENDING);
            message.setNextAttemptAt(null);
        });

        assertThat(outboxRelayService.claimBatch()).extracting(OutboxRecord::id).containsExactly(id);
    }

    // --------------------------------------------------- exclusion entre replicas

    @Test
    void noSeReclamanFilasBloqueadasPorOtraTransaccionNiSeEsperaPorEllas() throws Exception {
        List<UUID> ids = persistPending(3);
        UUID bloqueado = ids.getFirst();

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            try (PreparedStatement statement = connection.prepareStatement(
                    "select id from outbox_message where id = ? for update")) {
                statement.setObject(1, bloqueado);
                statement.execute();
            }

            // Sin SKIP LOCKED esta llamada se quedaria esperando al lock de arriba.
            List<OutboxRecord> lote = assertTimeoutPreemptively(
                    Duration.ofSeconds(10),
                    () -> outboxRelayService.claimBatch(),
                    "claimBatch se ha bloqueado esperando una fila tomada por otra transaccion");

            assertThat(lote).extracting(OutboxRecord::id)
                    .containsExactlyInAnyOrderElementsOf(ids.subList(1, ids.size()))
                    .doesNotContain(bloqueado);

            connection.rollback();
        }
    }

    @Test
    void variasReplicasConcurrentesNoSeLlevanElMismoMensaje() throws Exception {
        int mensajes = 60;
        int replicas = 6;
        persistPending(mensajes);
        outboxProperties.setBatchSize(10);

        List<UUID> entregados = new CopyOnWriteArrayList<>();
        CountDownLatch salida = new CountDownLatch(1);
        CountDownLatch fin = new CountDownLatch(replicas);
        List<Thread> hilos = new ArrayList<>();

        for (int i = 0; i < replicas; i++) {
            Thread hilo = new Thread(() -> {
                try {
                    salida.await();
                    outboxRelayService.claimBatch().forEach(record -> entregados.add(record.id()));
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                } finally {
                    fin.countDown();
                }
            });
            hilo.start();
            hilos.add(hilo);
        }

        salida.countDown();
        assertThat(fin.await(30, TimeUnit.SECONDS)).isTrue();
        for (Thread hilo : hilos) {
            hilo.join();
        }

        assertThat(entregados)
                .as("con varias replicas leyendo sin bloqueo, el mismo evento se publicaba N veces")
                .doesNotHaveDuplicates()
                .hasSize(replicas * 10);

        assertThat(Set.copyOf(entregados)).hasSize(replicas * 10);
    }

    @Test
    void unMensajeReclamadoSeRecuperaCuandoExpiraSuVisibilidad() throws Exception {
        outboxProperties.setClaimVisibilityTimeout(Duration.ofMillis(200));
        UUID id = persistPending(1).getFirst();

        List<OutboxRecord> primerReclamo = outboxRelayService.claimBatch();
        assertThat(primerReclamo).hasSize(1);

        // La replica que lo reclamo muere aqui, sin marcarlo ni publicado ni fallido.
        Thread.sleep(400);

        List<OutboxRecord> segundoReclamo = outboxRelayService.claimBatch();

        assertThat(segundoReclamo).extracting(OutboxRecord::id).containsExactly(id);
        assertThat(segundoReclamo.getFirst().attempts())
                .as("la recuperacion cuenta como intento para que no gire indefinidamente")
                .isEqualTo(1);
    }

    // ------------------------------------------------------------------ cierre

    @Test
    void marcarPublicadoDejaElMensajeCerrado() {
        UUID id = persistPending(1).getFirst();
        outboxRelayService.claimBatch();

        outboxRelayService.markPublished(id);

        OutboxMessage message = outboxMessageRepository.findById(id).orElseThrow();
        assertThat(message.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(message.getPublishedAt()).isNotNull();
        assertThat(message.getNextAttemptAt()).isNull();
        assertThat(outboxRelayService.claimBatch()).isEmpty();
    }

    @Test
    void unErrorMasLargoQueLaColumnaNoRompeElRelay() {
        UUID id = persistPending(1).getFirst();
        outboxRelayService.claimBatch();
        String errorGigante = "detalle del fallo AMQP ".repeat(500);

        // Con el mensaje sin recortar, el flush reventaba y se perdia la tanda ENTERA,
        // incluidos los mensajes ya publicados; ademas el fallo se repetia cada pasada.
        assertThatCode(() -> outboxRelayService.markFailed(id, new IllegalStateException(errorGigante)))
                .doesNotThrowAnyException();

        OutboxMessage message = outboxMessageRepository.findById(id).orElseThrow();
        assertThat(message.getLastError()).hasSizeLessThanOrEqualTo(OutboxErrors.MAX_LAST_ERROR_LENGTH);
        assertThat(message.getStatus()).isEqualTo(OutboxStatus.PENDING);
    }

    @Test
    void cadaFalloAplazaElSiguienteIntentoConBackoffExponencial() {
        UUID id = persistPending(1).getFirst();

        Instant antes = Instant.now();
        outboxRelayService.claimBatch();
        outboxRelayService.markFailed(id, new IllegalStateException("broker caido"));

        OutboxMessage primerFallo = outboxMessageRepository.findById(id).orElseThrow();
        assertThat(primerFallo.getAttempts()).isEqualTo(1);
        assertThat(primerFallo.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(primerFallo.getNextAttemptAt()).isBetween(
                antes.plus(Duration.ofSeconds(4)), antes.plus(Duration.ofSeconds(7)));

        // Segundo fallo: el retardo se duplica.
        primerFallo.setNextAttemptAt(Instant.now().minusSeconds(1));
        outboxMessageRepository.save(primerFallo);

        Instant antesDelSegundo = Instant.now();
        outboxRelayService.claimBatch();
        outboxRelayService.markFailed(id, new IllegalStateException("broker caido"));

        OutboxMessage segundoFallo = outboxMessageRepository.findById(id).orElseThrow();
        assertThat(segundoFallo.getAttempts()).isEqualTo(2);
        assertThat(segundoFallo.getNextAttemptAt()).isBetween(
                antesDelSegundo.plus(Duration.ofSeconds(9)), antesDelSegundo.plus(Duration.ofSeconds(12)));
    }

    @Test
    void alAgotarLosIntentosElMensajeQuedaEnFailed() {
        UUID id = persist(message -> {
            message.setStatus(OutboxStatus.PENDING);
            message.setNextAttemptAt(Instant.now().minusSeconds(1));
            message.setMaxAttempts(1);
        });
        outboxRelayService.claimBatch();

        outboxRelayService.markFailed(id, new IllegalStateException("nack"));

        OutboxMessage message = outboxMessageRepository.findById(id).orElseThrow();
        assertThat(message.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(message.getNextAttemptAt()).isNull();
        assertThat(outboxRelayService.claimBatch()).isEmpty();
    }

    // ------------------------------------------------------------------ redrive

    @Test
    void elRedriveDevuelveLosFailedAlCircuitoDePublicacion() {
        UUID id = persist(message -> {
            message.setStatus(OutboxStatus.FAILED);
            message.setAttempts(20);
            message.setLastError("agotado");
            message.setNextAttemptAt(null);
        });

        assertThat(outboxAdminService.redriveFailed(10)).isEqualTo(1);

        OutboxMessage message = outboxMessageRepository.findById(id).orElseThrow();
        assertThat(message.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(message.getAttempts()).isZero();
        assertThat(message.getLastError()).isNull();

        assertThat(outboxRelayService.claimBatch())
                .as("tras el redrive el mensaje debe volver a ser publicable")
                .extracting(OutboxRecord::id)
                .containsExactly(id);
    }

    @Test
    void elRedriveRespetaSuLimiteYNoTocaLoQueNoEstaEnFailed() {
        IntStream.range(0, 5).forEach(i -> persist(message -> message.setStatus(OutboxStatus.FAILED)));
        persistPending(2);

        assertThat(outboxAdminService.redriveFailed(3)).isEqualTo(3);
        assertThat(outboxMessageRepository.countByStatus(OutboxStatus.FAILED)).isEqualTo(2);
        assertThat(outboxMessageRepository.countByStatus(OutboxStatus.PENDING)).isEqualTo(5);
        assertThat(outboxAdminService.redriveFailed(0)).isZero();
    }

    @Test
    void lasEstadisticasReflejanElEstadoRealDelOutbox() {
        persistPending(2);
        persist(message -> message.setStatus(OutboxStatus.FAILED));
        persist(message -> message.setStatus(OutboxStatus.PUBLISHED));

        OutboxStats stats = outboxAdminService.stats();

        assertThat(stats.pending()).isEqualTo(2);
        assertThat(stats.failed()).isEqualTo(1);
        assertThat(stats.published()).isEqualTo(1);
        assertThat(stats.inProgress()).isZero();
        assertThat(stats.oldestPendingCreatedAt())
                .as("la edad del PENDING mas antiguo es la senal de que el relay esta roto")
                .isNotNull();
    }

    // ------------------------------------------------------- relay de punta a punta

    @Test
    void elRelayCompletoDejaElMensajePublicadoCuandoElBrokerConfirma() {
        UUID id = persistPending(1).getFirst();

        outboxPublisherScheduler.publishPendingMessages();

        OutboxMessage message = outboxMessageRepository.findById(id).orElseThrow();
        assertThat(message.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(message.getPublishedAt()).isNotNull();
    }

    @Test
    void elRelayCompletoAplazaElMensajeCuandoElBrokerNoConfirma() {
        UUID id = persistPending(1).getFirst();
        doThrow(new OutboxPublishException("nack del broker"))
                .when(outboxRabbitPublisher).publish(any());

        Instant antes = Instant.now();
        outboxPublisherScheduler.publishPendingMessages();

        OutboxMessage message = outboxMessageRepository.findById(id).orElseThrow();
        assertThat(message.getStatus())
                .as("sin ack del broker el mensaje NO puede quedar como publicado")
                .isEqualTo(OutboxStatus.PENDING);
        assertThat(message.getAttempts()).isEqualTo(1);
        assertThat(message.getLastError()).contains("nack del broker");
        assertThat(message.getNextAttemptAt()).isAfter(antes);

        // Y en la siguiente ventana vuelve a intentarse, no se pierde.
        message.setNextAttemptAt(Instant.now().minusSeconds(1));
        outboxMessageRepository.save(message);
        reset(outboxRabbitPublisher);

        outboxPublisherScheduler.publishPendingMessages();

        assertThat(outboxMessageRepository.findById(id).orElseThrow().getStatus())
                .isEqualTo(OutboxStatus.PUBLISHED);
    }

    // ----------------------------------------------------- publicacion inmediata

    @Test
    void elMensajeSePublicaAlConfirmarSinEsperarAlSondeo() {
        // El sondeo cada 5 segundos es la red de seguridad, pero tambien era el suelo
        // de latencia de TODOS los eventos. Aqui no se llama al planificador en ningun
        // momento: si el mensaje acaba publicado, es porque el commit lo desperto.
        new org.springframework.transaction.support.TransactionTemplate(transactionManager)
                .executeWithoutResult(status -> outboxService.save(
                        "order", "42", "FIELD_POSSESSION_EVACUATION_ISSUED",
                        "mto.field.exchange", "mto.field.possession.evacuation-issued",
                        java.util.Map.of("id", 42)));

        org.awaitility.Awaitility.await()
                .atMost(java.time.Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(outboxMessageRepository.findAll())
                        .singleElement()
                        .extracting(OutboxMessage::getStatus)
                        .isEqualTo(OutboxStatus.PUBLISHED));
    }

    @Test
    void unaTransaccionQueHaceRollbackNoDespiertaAlRelay() {
        // Si se publicara antes del commit, se estaria anunciando un cambio que nunca
        // ocurrio. Es exactamente la razon por la que existe el outbox.
        org.springframework.transaction.support.TransactionTemplate plantilla =
                new org.springframework.transaction.support.TransactionTemplate(transactionManager);

        assertThatCode(() -> plantilla.executeWithoutResult(status -> {
            outboxService.save("order", "43", "FIELD_POSSESSION_EVACUATION_ISSUED",
                    "mto.field.exchange", "mto.field.possession.evacuation-issued",
                    java.util.Map.of("id", 43));
            status.setRollbackOnly();
        })).doesNotThrowAnyException();

        assertThat(outboxMessageRepository.findAll())
                .as("sin commit no hay ni fila ni evento que publicar")
                .isEmpty();
    }

    // --------------------------------------------------------- orden por agregado

    @Test
    void unMensajeAtascadoRetieneALosSiguientesDeSuMismoAgregado() {
        UUID primero = persistFor("42");
        UUID segundo = persistFor("42");

        // El primero se reclama y falla: vuelve a PENDING, aplazado por el backoff.
        outboxRelayService.claimBatch();
        outboxRelayService.markFailed(primero, new IllegalStateException("broker caido"));

        List<OutboxRecord> lote = outboxRelayService.claimBatch();

        // Sin esta retencion, el segundo saldria ahora y el primero despues: el
        // consumidor aplicaria el cambio VIEJO encima del nuevo y el dato maestro
        // quedaria mal sin que nadie se entere.
        assertThat(lote)
                .as("el segundo del agregado no puede adelantar al primero, que aun va a llegar")
                .isEmpty();
        assertThat(outboxMessageRepository.findById(segundo).orElseThrow().getStatus())
                .isEqualTo(OutboxStatus.PENDING);
    }

    @Test
    void laRetencionEsSoloDeSuAgregadoYNoFrenaAlResto() {
        UUID bloqueante = persistFor("42");
        persistFor("42");
        UUID otroAgregado = persistFor("99");

        // Lote de uno para que el primer reclamo se lleve solo al bloqueante y no
        // arrastre ya de paso el mensaje del otro agregado.
        outboxProperties.setBatchSize(1);
        outboxRelayService.claimBatch();
        outboxRelayService.markFailed(bloqueante, new IllegalStateException("broker caido"));
        outboxProperties.setBatchSize(50);

        assertThat(outboxRelayService.claimBatch())
                .as("un agregado atascado no puede parar el circuito entero")
                .extracting(OutboxRecord::id)
                .containsExactly(otroAgregado);
    }

    @Test
    void alPublicarseElPrimeroSeLiberaElSiguiente() {
        UUID primero = persistFor("42");
        UUID segundo = persistFor("42");

        outboxRelayService.claimBatch();
        outboxRelayService.markPublished(primero);

        assertThat(outboxRelayService.claimBatch())
                .extracting(OutboxRecord::id)
                .containsExactly(segundo);
    }

    @Test
    void unMensajeAgotadoNoRetieneAlAgregadoParaSiempre() {
        UUID agotado = persist(message -> {
            message.setAggregateId("42");
            message.setStatus(OutboxStatus.FAILED);
            message.setNextAttemptAt(null);
        });
        UUID siguiente = persistFor("42");

        // Un FAILED ya no se va a publicar por si mismo: si retuviera, dejaria el
        // agregado parado indefinidamente y eso es peor que el desorden.
        assertThat(outboxRelayService.claimBatch())
                .extracting(OutboxRecord::id)
                .containsExactly(siguiente);
        assertThat(outboxMessageRepository.findById(agotado).orElseThrow().getStatus())
                .isEqualTo(OutboxStatus.FAILED);
    }

    @Test
    void elOrdenLoMandaLaSecuenciaYNoLaFechaDeCreacion() {
        // created_at no sirve como clave de orden: un bulkCreate genera N mensajes con
        // el mismo instante, y ademas la fecha la pone la aplicacion, de modo que un
        // reloj desajustado entre replicas puede darla del reves.
        //
        // Aqui se crean con created_at DECRECIENTE segun avanza la secuencia: si la
        // consulta ordenara por fecha, saldrian justo al reves.
        Instant base = Instant.now().minusSeconds(1);
        List<UUID> porSecuencia = IntStream.range(0, 5)
                .mapToObj(i -> {
                    Instant haciaAtras = base.minusSeconds(i);
                    return persist(message -> {
                        message.setAggregateId(String.valueOf(i));
                        message.setStatus(OutboxStatus.PENDING);
                        message.setCreatedAt(haciaAtras);
                        message.setNextAttemptAt(haciaAtras);
                    });
                })
                .toList();

        assertThat(outboxRelayService.claimBatch())
                .extracting(OutboxRecord::id)
                .containsExactlyElementsOf(porSecuencia);
    }

    @Test
    void variosMensajesDelMismoInstanteConservanSuOrdenDeCreacion() {
        // El caso real del bulkCreate: mismo created_at para todos.
        Instant mismoInstante = Instant.now().minusSeconds(1);
        List<UUID> creados = IntStream.range(0, 10)
                .mapToObj(i -> persist(message -> {
                    message.setAggregateId(String.valueOf(i));
                    message.setStatus(OutboxStatus.PENDING);
                    message.setCreatedAt(mismoInstante);
                    message.setNextAttemptAt(mismoInstante);
                }))
                .toList();

        assertThat(outboxRelayService.claimBatch())
                .extracting(OutboxRecord::id)
                .containsExactlyElementsOf(creados);
    }

    @Test
    void laBaseDeDatosAsignaUnaSecuenciaCrecienteACadaMensaje() {
        List<UUID> creados = IntStream.range(0, 5).mapToObj(i -> persistFor("42")).toList();

        List<Long> secuencias = creados.stream()
                .map(id -> outboxMessageRepository.findById(id).orElseThrow().getSequenceNumber())
                .toList();

        assertThat(secuencias).doesNotContainNull().isSorted();
        assertThat(secuencias).doesNotHaveDuplicates();
    }

    @Test
    void desactivandoElOrdenEstrictoElSegundoAdelantaAlPrimero() {
        outboxProperties.setStrictOrderingPerAggregate(false);
        UUID primero = persistFor("42");
        UUID segundo = persistFor("42");

        // Sin orden estricto nada retiene al segundo, asi que el lote de uno es lo que
        // deja ver que adelanta al primero en cuanto este se aplaza.
        outboxProperties.setBatchSize(1);
        outboxRelayService.claimBatch();
        outboxRelayService.markFailed(primero, new IllegalStateException("broker caido"));
        outboxProperties.setBatchSize(50);

        // Es la palanca para vaciar un atasco a costa del orden. Que exista no la hace
        // recomendable: por defecto pesa mas no corromper el dato.
        assertThat(outboxRelayService.claimBatch())
                .extracting(OutboxRecord::id)
                .containsExactly(segundo);
    }

    // ------------------------------------------------------------------ trazabilidad

    @Test
    void elContextoDeTrazaDeLaOperacionSobreviveAlSaltoDelOutbox() {
        io.micrometer.tracing.Span operacion = tracer.nextSpan().name("PUT /stations/42").start();

        // Fase 1: la operacion de negocio guarda el evento, con su traza viva.
        try (io.micrometer.tracing.Tracer.SpanInScope ignored = tracer.withSpan(operacion)) {
            outboxService.save("order", "42", "FIELD_POSSESSION_EVACUATION_ISSUED",
                    "mto.field.exchange", "mto.field.possession.evacuation-issued",
                    java.util.Map.of("id", 42));
        } finally {
            operacion.end();
        }

        OutboxMessage guardado = outboxMessageRepository.findAll().getFirst();
        assertThat(guardado.getTraceParent())
                .as("si no se guarda aqui, no hay forma de recuperarlo despues: el contexto"
                        + " de la peticion ya no existe cuando el relay publica")
                .contains(operacion.context().traceId());

        // Fase 2: el relay, minutos despues y sin ninguna traza activa.
        assertThat(tracer.currentSpan()).isNull();

        List<OutboxRecord> lote = outboxRelayService.claimBatch();

        assertThat(lote).hasSize(1);
        assertThat(lote.getFirst().traceParent()).isEqualTo(guardado.getTraceParent());
    }

    @Test
    void unEventoGeneradoSinTrazaSeGuardaIgualmente() {
        // Una tarea programada no tiene peticion detras, y aun asi tiene que publicar.
        assertThat(tracer.currentSpan()).isNull();

        outboxService.save("order", "43", "FIELD_POSSESSION_OPENED",
                "mto.field.exchange", "mto.field.possession.opened",
                java.util.Map.of("id", 43));

        OutboxMessage guardado = outboxMessageRepository.findAll().getFirst();
        assertThat(guardado.getTraceParent()).isNull();
        assertThat(outboxRelayService.claimBatch()).hasSize(1);
    }

    // ------------------------------------------------------------------ purga

    @Test
    void laPurgaBorraLosPublicadosAntiguosYRespetaElResto() {
        Instant viejo = Instant.now().minus(Duration.ofDays(30));

        UUID publicadoAntiguo = persist(message -> {
            message.setStatus(OutboxStatus.PUBLISHED);
            message.setPublishedAt(viejo);
        });
        UUID publicadoReciente = persist(message -> {
            message.setStatus(OutboxStatus.PUBLISHED);
            message.setPublishedAt(Instant.now());
        });
        UUID fallidoAntiguo = persist(message -> {
            message.setStatus(OutboxStatus.FAILED);
            message.setCreatedAt(viejo);
        });
        UUID pendienteAntiguo = persist(message -> {
            message.setStatus(OutboxStatus.PENDING);
            message.setCreatedAt(viejo);
        });

        outboxPurgeScheduler.purgePublishedMessages();

        assertThat(outboxMessageRepository.findById(publicadoAntiguo)).isEmpty();
        assertThat(outboxMessageRepository.findById(publicadoReciente))
                .as("dentro del periodo de retencion no se toca")
                .isPresent();
        assertThat(outboxMessageRepository.findById(fallidoAntiguo))
                .as("un FAILED es justo lo que hay que mirar: borrarlo seria tapar el problema")
                .isPresent();
        assertThat(outboxMessageRepository.findById(pendienteAntiguo))
                .as("un PENDING todavia no se ha publicado; borrarlo seria perder el evento")
                .isPresent();
    }

    @Test
    void laPurgaSeGuiaPorElEstadoYNoSoloPorLaFecha() {
        Instant viejo = Instant.now().minus(Duration.ofDays(30));

        // Fila artificial pero posible: un mensaje que NO esta publicado y aun asi
        // lleva published_at informado. Si la purga se fiara solo de la fecha, se
        // llevaria por delante un evento que nadie ha llegado a enviar.
        UUID pendienteConFecha = persist(message -> {
            message.setStatus(OutboxStatus.PENDING);
            message.setPublishedAt(viejo);
            message.setCreatedAt(viejo);
        });
        UUID fallidoConFecha = persist(message -> {
            message.setStatus(OutboxStatus.FAILED);
            message.setPublishedAt(viejo);
            message.setCreatedAt(viejo);
        });

        outboxPurgeScheduler.purgePublishedMessages();

        assertThat(outboxMessageRepository.findById(pendienteConFecha)).isPresent();
        assertThat(outboxMessageRepository.findById(fallidoConFecha)).isPresent();
    }

    @Test
    void laPurgaBorraPorLotesHastaVaciarLoQueToca() {
        Instant viejo = Instant.now().minus(Duration.ofDays(30));
        IntStream.range(0, 25).forEach(i -> persist(message -> {
            message.setStatus(OutboxStatus.PUBLISHED);
            message.setPublishedAt(viejo);
        }));

        outboxProperties.getPurge().setBatchSize(10);

        outboxPurgeScheduler.purgePublishedMessages();

        assertThat(outboxMessageRepository.count()).isZero();
    }

    @Test
    void laPurgaNoHaceMasTrabajoDelPermitidoEnUnaPasada() {
        Instant viejo = Instant.now().minus(Duration.ofDays(30));
        IntStream.range(0, 25).forEach(i -> persist(message -> {
            message.setStatus(OutboxStatus.PUBLISHED);
            message.setPublishedAt(viejo);
        }));

        // Sobre una tabla enorme, la primera pasada no puede ser un borrado de horas.
        outboxProperties.getPurge().setBatchSize(5);
        outboxProperties.getPurge().setMaxBatchesPerRun(2);

        outboxPurgeScheduler.purgePublishedMessages();

        assertThat(outboxMessageRepository.count()).isEqualTo(15);
    }

    @Test
    void laPurgaSobreUnOutboxLimpioNoHaceNada() {
        persistPending(3);

        assertThatCode(() -> outboxPurgeScheduler.purgePublishedMessages()).doesNotThrowAnyException();
        assertThat(outboxMessageRepository.count()).isEqualTo(3);
    }

    // ------------------------------------------------------------------ utilidades

    private List<UUID> persistPending(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> persist(message -> {
                    message.setStatus(OutboxStatus.PENDING);
                    message.setNextAttemptAt(Instant.now().minusSeconds(1));
                    message.setAggregateId(String.valueOf(i));
                }))
                .collect(Collectors.toList());
    }

    /** Mensaje listo para publicar de un agregado concreto. */
    private UUID persistFor(String aggregateId) {
        return persist(message -> {
            message.setAggregateType("order");
            message.setAggregateId(aggregateId);
            message.setStatus(OutboxStatus.PENDING);
            message.setNextAttemptAt(Instant.now().minusSeconds(1));
        });
    }

    private UUID persist(java.util.function.Consumer<OutboxMessage> customizer) {
        Instant now = Instant.now();

        OutboxMessage message = new OutboxMessage();
        message.setId(UUID.randomUUID());
        message.setAggregateType("order");
        message.setAggregateId("1");
        message.setEventType("FIELD_POSSESSION_OPENED");
        message.setExchangeName("mto.field.exchange");
        message.setRoutingKey("mto.field.possession.opened");
        message.setPayload("{\"id\":1}");
        message.setStatus(OutboxStatus.PENDING);
        message.setAttempts(0);
        message.setMaxAttempts(20);
        message.setCreatedAt(now);
        message.setNextAttemptAt(now.minusSeconds(1));

        customizer.accept(message);

        return outboxMessageRepository.save(message).getId();
    }

    @TestConfiguration
    @EnableConfigurationProperties(OutboxProperties.class)
    static class OutboxTestConfiguration {

        @Bean
        OutboxRetryPolicy outboxRetryPolicy(OutboxProperties outboxProperties) {
            return new OutboxRetryPolicy(outboxProperties);
        }

        @Bean
        OutboxRelayService outboxRelayService(OutboxMessageRepository repository,
                                              OutboxProperties properties,
                                              OutboxRetryPolicy retryPolicy) {
            return new OutboxRelayService(repository, properties, retryPolicy);
        }

        @Bean
        OutboxAdminService outboxAdminService(OutboxMessageRepository repository) {
            return new OutboxAdminService(repository);
        }

        @Bean
        OutboxRabbitPublisher outboxRabbitPublisher() {
            return org.mockito.Mockito.mock(OutboxRabbitPublisher.class);
        }

        @Bean
        OutboxMetrics outboxMetrics() {
            return new OutboxMetrics(new SimpleMeterRegistry());
        }

        @Bean
        io.opentelemetry.sdk.trace.SdkTracerProvider sdkTracerProvider() {
            return io.opentelemetry.sdk.trace.SdkTracerProvider.builder().build();
        }

        @Bean
        io.micrometer.tracing.Tracer tracer(io.opentelemetry.sdk.trace.SdkTracerProvider tracerProvider) {
            io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext currentTraceContext =
                    new io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext();
            return new io.micrometer.tracing.otel.bridge.OtelTracer(
                    tracerProvider.get("it"), currentTraceContext, event -> { },
                    new io.micrometer.tracing.otel.bridge.OtelBaggageManager(
                            currentTraceContext, List.of(), List.of()));
        }

        @Bean
        OutboxTracing outboxTracing(io.micrometer.tracing.Tracer tracer,
                                    io.opentelemetry.sdk.trace.SdkTracerProvider tracerProvider) {
            return new MicrometerOutboxTracing(tracer, new io.micrometer.tracing.otel.bridge.OtelPropagator(
                    io.opentelemetry.context.propagation.ContextPropagators.create(
                            io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator.getInstance()),
                    tracerProvider.get("it")));
        }

        @Bean
        OutboxService outboxService(OutboxMessageRepository repository,
                                    OutboxProperties properties,
                                    OutboxTracing tracing,
                                    org.springframework.context.ApplicationEventPublisher eventPublisher) {
            return new OutboxService(repository, properties,
                    new tools.jackson.databind.ObjectMapper(), tracing, eventPublisher);
        }

        @Bean
        OutboxDispatchTrigger outboxDispatchTrigger(OutboxPublisherScheduler scheduler) {
            return new OutboxDispatchTrigger(
                    java.util.concurrent.Executors.newSingleThreadExecutor(),
                    scheduler::publishPendingMessages);
        }

        @Bean
        OutboxImmediateDispatchListener outboxImmediateDispatchListener(OutboxDispatchTrigger trigger) {
            return new OutboxImmediateDispatchListener(trigger);
        }

        @Bean
        OutboxPublisherScheduler outboxPublisherScheduler(OutboxRelayService relayService,
                                                          OutboxRabbitPublisher publisher,
                                                          OutboxMetrics metrics) {
            return new OutboxPublisherScheduler(relayService, publisher, metrics);
        }

        @Bean
        OutboxPurgeService outboxPurgeService(OutboxMessageRepository repository) {
            return new OutboxPurgeService(repository);
        }

        @Bean
        OutboxPurgeScheduler outboxPurgeScheduler(OutboxPurgeService purgeService,
                                                  OutboxProperties properties) {
            return new OutboxPurgeScheduler(purgeService, properties);
        }
    }
}
