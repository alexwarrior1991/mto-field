package com.alejandro.mtofield.infrastructure.persistence;

import com.alejandro.mtofield.configuration.JpaAuditingConfiguration;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldCommandRecord;
import com.alejandro.mtofield.infrastructure.persistence.entity.Possession;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionShift;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionStatus;
import com.alejandro.mtofield.infrastructure.persistence.repository.CommandAckRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.FieldCommandRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.FieldEventRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.PossessionRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.PossessionShiftRepository;
import com.alejandro.mtofield.support.PostgreSQLTestContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Limit;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lo que solo decide el SQL de verdad: las inserciones idempotentes, la secuencia sin huecos bajo
 * contencion, la clave de idempotencia, la marca de agua contigua y el indice parcial de turno
 * abierto. Contra un PostgreSQL real con las migraciones aplicadas.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ImportAutoConfiguration(FlywayAutoConfiguration.class)
@Import(JpaAuditingConfiguration.class)
class FieldRepositoryDataJpaTest extends PostgreSQLTestContainer {

    @DynamicPropertySource
    static void postgreSQLProperties(DynamicPropertyRegistry registry) {
        registerPostgreSQLProperties(registry);
    }

    @Autowired
    private PossessionRepository possessions;

    @Autowired
    private PossessionShiftRepository possessionShifts;

    @Autowired
    private FieldCommandRepository commands;

    @Autowired
    private CommandAckRepository acks;

    @Autowired
    private FieldEventRepository events;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Nested
    @DisplayName("Eventos subidos")
    class UploadedEvents {

        @Test
        void aRepeatedDeviceSequenceIsInsertedOnlyOnce() {
            Possession possession = possessions.saveAndFlush(possession("PO-000101"));
            UUID shiftId = UUID.randomUUID();

            assertThat(insertEvent(possession, shiftId, "dev-1", 1)).isEqualTo(1);
            assertThat(insertEvent(possession, shiftId, "dev-1", 1)).isEqualTo(0);
            assertThat(insertEvent(possession, shiftId, "dev-1", 2)).isEqualTo(1);
            assertThat(insertEvent(possession, shiftId, "dev-2", 1)).isEqualTo(1);
        }

        @Test
        void theDatabaseRefusesASequenceOfZero() {
            Possession possession = possessions.saveAndFlush(possession("PO-000102"));

            assertThatThrownBy(() -> insertEvent(possession, UUID.randomUUID(), "dev-1", 0))
                    .isInstanceOf(DataIntegrityViolationException.class);
        }

        @Test
        void theWatermarkIsTheContiguousMaximumNotTheMaximum() {
            Possession possession = possessions.saveAndFlush(possession("PO-000103"));
            UUID shiftId = UUID.randomUUID();

            assertThat(events.contiguousWatermark("empty")).isZero();

            for (long sequence : new long[] {1, 2, 3}) {
                insertEvent(possession, shiftId, "contiguous", sequence);
            }
            assertThat(events.contiguousWatermark("contiguous")).isEqualTo(3);

            for (long sequence : new long[] {1, 2, 4, 5}) {
                insertEvent(possession, shiftId, "gap", sequence);
            }
            assertThat(events.contiguousWatermark("gap")).isEqualTo(2);

            for (long sequence : new long[] {2, 3}) {
                insertEvent(possession, shiftId, "late-start", sequence);
            }
            assertThat(events.contiguousWatermark("late-start")).isZero();
        }

        @Test
        void aSyncStatusOnlyMovesForwardFromPendingOrFailed() {
            Possession possession = possessions.saveAndFlush(possession("PO-000104"));
            UUID eventId = UUID.randomUUID();
            events.insertIfMissing(eventId, "dev-sync", 1, possession.getId(), UUID.randomUUID(), "TASK_STARTED",
                    Instant.now(), "campo.tecnico1", "{}", "PENDING", Instant.now());

            assertThat(events.markFailed(eventId, "timeout", Instant.now())).isEqualTo(1);
            assertThat(events.findDueForSync(Instant.now().plusSeconds(1), Limit.of(10)))
                    .extracting(record -> record.getId()).contains(eventId);
            assertThat(events.markSynced(eventId)).isEqualTo(1);
            assertThat(events.findDueForSync(Instant.now().plusSeconds(1), Limit.of(10)))
                    .extracting(record -> record.getId()).doesNotContain(eventId);
            assertThat(events.markFailed(eventId, "again", Instant.now())).isZero();
            assertThat(events.markRejected(eventId, "no")).isZero();
        }
    }

    @Nested
    @DisplayName("Ordenes y acuses")
    class CommandsAndAcks {

        @Test
        void aRepeatedIdempotencyKeyIsRefusedButNullKeysCoexist() {
            Possession possession = possessions.saveAndFlush(possession("PO-000105"));

            insertCommand(possession, 1, "evac-1");
            insertCommand(possession, 2, null);
            insertCommand(possession, 3, null);
            assertThatThrownBy(() -> insertCommand(possession, 4, "evac-1"))
                    .isInstanceOf(DataIntegrityViolationException.class);
        }

        @Test
        void theReplayReturnsBroadcastsAndTheShiftsOwnCommandsAfterTheSequenceInOrder() {
            Possession possession = possessions.saveAndFlush(possession("PO-000106"));
            UUID mine = UUID.randomUUID();
            UUID other = UUID.randomUUID();
            insertCommand(possession, 1, null, null);
            insertCommand(possession, 2, null, mine);
            insertCommand(possession, 3, null, other);
            insertCommand(possession, 4, null, null);
            insertCommand(possession, 5, null, mine);

            List<FieldCommandRecord> replay = commands.findReplay(possession.getId(), mine, 1, Limit.of(10));

            assertThat(replay).extracting(FieldCommandRecord::getSequence).containsExactly(2L, 4L, 5L);
            assertThat(commands.findReplay(possession.getId(), mine, 1, Limit.of(2)))
                    .extracting(FieldCommandRecord::getSequence).containsExactly(2L, 4L);
            assertThat(commands.maxSequence(possession.getId())).isEqualTo(5);
            assertThat(commands.maxSequence(UUID.randomUUID())).isZero();
        }

        @Test
        void aTeamAcknowledgesACommandOnce() {
            Possession possession = possessions.saveAndFlush(possession("PO-000107"));
            UUID commandId = insertCommand(possession, 1, "evac-1");
            UUID shiftId = UUID.randomUUID();

            assertThat(acks.insertIfMissing(commandId, shiftId, "dev-1", "campo.tecnico1", true, null)).isEqualTo(1);
            assertThat(acks.insertIfMissing(commandId, shiftId, "dev-2", "campo.tecnico2", true, null)).isZero();
            assertThat(acks.findByCommandId(commandId)).singleElement()
                    .satisfies(ack -> assertThat(ack.getDeviceId()).isEqualTo("dev-1"));
        }
    }

    @Nested
    @DisplayName("Posesion y turnos")
    class PossessionAndShifts {

        @Test
        void aShiftCannotBeInTwoOpenPossessionsUntilTheFirstCloses() {
            UUID shiftId = UUID.randomUUID();
            Possession first = possession("PO-000108");
            first.addShift(shift(shiftId, "T-1"));
            possessions.saveAndFlush(first);

            Possession second = possession("PO-000109");
            second.addShift(shift(shiftId, "T-1"));
            assertThatThrownBy(() -> possessions.saveAndFlush(second))
                    .isInstanceOf(DataIntegrityViolationException.class);
        }

        @Test
        void closingTheShiftsFreesTheShiftAndEndsItsMembership() {
            UUID shiftId = UUID.randomUUID();
            Possession first = possession("PO-000110");
            first.addShift(shift(shiftId, "T-1"));
            possessions.saveAndFlush(first);
            assertThat(possessionShifts.findOpenMembership(shiftId)).isPresent()
                    .get().satisfies(membership -> {
                        assertThat(membership.getPossessionId()).isEqualTo(first.getId());
                        assertThat(membership.getTeamCode()).isEqualTo("T-1");
                    });

            assertThat(possessionShifts.closeAll(first.getId(), "campo.responsable")).isEqualTo(1);
            assertThat(possessionShifts.existsByShiftIdAndOpenTrue(shiftId)).isFalse();

            Possession second = possession("PO-000111");
            second.addShift(shift(shiftId, "T-1"));
            possessions.saveAndFlush(second);
            assertThat(possessionShifts.findOpenMembership(shiftId)).isPresent()
                    .get().satisfies(membership -> assertThat(membership.getPossessionId()).isEqualTo(second.getId()));
        }

        @Test
        void clearOfTrackIsWrittenOnce() {
            UUID shiftId = UUID.randomUUID();
            Possession possession = possession("PO-000112");
            possession.addShift(shift(shiftId, "T-1"));
            possessions.saveAndFlush(possession);

            assertThat(possessionShifts.markClear(possession.getId(), shiftId, "campo.tecnico1", "dev-1", true)).isEqualTo(1);
            assertThat(possessionShifts.markClear(possession.getId(), shiftId, "campo.tecnico2", "dev-2", false)).isZero();
            assertThat(possessionShifts.countByPossession_IdAndClearOfTrackAtIsNull(possession.getId())).isZero();
        }

        @Test
        void theSequenceIsOnlyHandedOutByAnOpenPossession() {
            Possession open = possessions.saveAndFlush(possession("PO-000113"));
            assertThat(possessions.nextCommandSequence(open.getId())).contains(1L);
            assertThat(possessions.nextCommandSequence(open.getId())).contains(2L);
            assertThat(possessions.nextCommandSequence(UUID.randomUUID())).isEmpty();
        }
    }

    /**
     * La propiedad que lo sostiene todo: dos emisores a la vez, cada uno en su transaccion, y las
     * secuencias salen exactamente 1..N sin duplicados ni huecos; un tercero que toma un numero y
     * deshace no deja hueco. Sin transaccion de test a proposito: cada hilo confirma la suya.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void twoConcurrentEmittersGetAGaplessSequenceInCommitOrder() throws Exception {
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        UUID possessionId = transaction.execute(status -> possessions.save(possession("PO-000199")).getId());
        try {
            int perThread = 100;
            CountDownLatch start = new CountDownLatch(1);
            ConcurrentLinkedQueue<Long> issued = new ConcurrentLinkedQueue<>();
            List<Thread> threads = new ArrayList<>();
            for (int emitter = 0; emitter < 2; emitter++) {
                threads.add(Thread.ofVirtual().start(() -> {
                    await(start);
                    for (int i = 0; i < perThread; i++) {
                        issued.add(transaction.execute(status -> {
                            long sequence = possessions.nextCommandSequence(possessionId).orElseThrow();
                            commands.insert(UUID.randomUUID(), possessionId, sequence, "SUPERVISOR_MESSAGE", null, null, false,
                                    Instant.now(), "campo.responsable", "{}");
                            return sequence;
                        }));
                    }
                }));
            }
            threads.add(Thread.ofVirtual().start(() -> {
                await(start);
                for (int i = 0; i < 20; i++) {
                    transaction.execute(status -> {
                        possessions.nextCommandSequence(possessionId).orElseThrow();
                        status.setRollbackOnly();
                        return null;
                    });
                }
            }));
            start.countDown();
            for (Thread thread : threads) {
                assertThat(thread.join(java.time.Duration.ofMinutes(2))).as("emitter finished").isTrue();
            }

            assertThat(issued).hasSize(2 * perThread);
            assertThat(issued).containsExactlyInAnyOrderElementsOf(LongStream.rangeClosed(1, 2L * perThread).boxed().toList());
            assertThat(commands.maxSequence(possessionId)).isEqualTo(2L * perThread);
        } finally {
            transaction.executeWithoutResult(status -> possessions.deleteById(possessionId));
        }
    }

    @AfterEach
    void nothingLeaks() {
        // Los tests transaccionales se deshacen solos; el de concurrencia limpia lo suyo.
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }

    private static Possession possession(String code) {
        return Possession.builder()
                .code(code)
                .status(PossessionStatus.OPEN)
                .shiftDate(LocalDate.of(2026, 10, 9))
                .endsAt(Instant.parse("2026-10-10T05:00:00Z"))
                .openedAt(Instant.now())
                .openedBy("campo.responsable")
                .build();
    }

    private static PossessionShift shift(UUID shiftId, String teamCode) {
        return PossessionShift.builder()
                .shiftId(shiftId)
                .teamCode(teamCode)
                .teamName("Equipo " + teamCode)
                .build();
    }

    private int insertEvent(Possession possession, UUID shiftId, String deviceId, long sequence) {
        return events.insertIfMissing(UUID.randomUUID(), deviceId, sequence, possession.getId(), shiftId, "COMMAND_ACK",
                Instant.now(), "campo.tecnico1", "{\"deviceId\":\"" + deviceId + "\"}", "NOT_REQUIRED", null);
    }

    private UUID insertCommand(Possession possession, long sequence, String idempotencyKey) {
        return insertCommand(possession, sequence, idempotencyKey, null);
    }

    private UUID insertCommand(Possession possession, long sequence, String idempotencyKey, UUID targetShiftId) {
        UUID id = UUID.randomUUID();
        commands.insert(id, possession.getId(), sequence, "EVACUATE_NOW", targetShiftId, idempotencyKey, true,
                Instant.now(), "campo.responsable", "{\"commandId\":\"" + id + "\"}");
        return id;
    }
}
