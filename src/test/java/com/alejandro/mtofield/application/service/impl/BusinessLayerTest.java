package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.dto.BoardSnapshot;
import com.alejandro.mtofield.application.dto.CommandDraft;
import com.alejandro.mtofield.application.dto.DevicePrincipal;
import com.alejandro.mtofield.application.dto.EventContext;
import com.alejandro.mtofield.application.dto.IssuedCommand;
import com.alejandro.mtofield.application.dto.PossessionView;
import com.alejandro.mtofield.application.dto.StoredCommand;
import com.alejandro.mtofield.application.dto.StoredEvent;
import com.alejandro.mtofield.application.dto.SyncJob;
import com.alejandro.mtofield.application.event.CommandCommitted;
import com.alejandro.mtofield.application.event.PossessionClosed;
import com.alejandro.mtofield.application.exception.InvalidPossessionRequestException;
import com.alejandro.mtofield.application.exception.PossessionNotAllClearException;
import com.alejandro.mtofield.application.exception.PossessionNotFoundException;
import com.alejandro.mtofield.application.exception.PossessionNotOpenException;
import com.alejandro.mtofield.application.exception.ShiftAlreadyInOpenPossessionException;
import com.alejandro.mtofield.application.mapper.ProtoJson;
import com.alejandro.mtofield.application.service.BoardPublisher;
import com.alejandro.mtofield.application.service.DeviceStreamPresence;
import com.alejandro.mtofield.application.service.FieldCodeGenerator;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.application.service.LivenessRegistry;
import com.alejandro.mtofield.application.service.MaintenanceClient;
import com.alejandro.mtofield.configuration.grpc.FieldProperties;
import com.alejandro.mtofield.configuration.maintenance.MaintenanceClientConfiguration;
import com.alejandro.mtofield.configuration.metrics.FieldMetrics;
import com.alejandro.mtofield.domain.model.CommandAckSummary;
import com.alejandro.mtofield.domain.model.ShiftNotWorkableException;
import com.alejandro.mtofield.domain.model.ShiftSnapshot;
import com.alejandro.mtofield.domain.model.TeamLiveness;
import com.alejandro.mtofield.grpc.v1.ClearOfTrack;
import com.alejandro.mtofield.grpc.v1.CommandAck;
import com.alejandro.mtofield.grpc.v1.EvacuateNow;
import com.alejandro.mtofield.grpc.v1.EventResult;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.grpc.v1.TaskStarted;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.alejandro.mtofield.infrastructure.persistence.entity.CommandAckRecord;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldCommandKind;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldCommandRecord;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldEventRecord;
import com.alejandro.mtofield.infrastructure.persistence.entity.FieldEventSyncStatus;
import com.alejandro.mtofield.infrastructure.persistence.entity.Possession;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionShift;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionStatus;
import com.alejandro.mtofield.infrastructure.persistence.repository.CommandAckRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.FieldCommandRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.FieldEventRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.PossessionRepository;
import com.alejandro.mtofield.infrastructure.persistence.repository.PossessionShiftRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Los servicios de aplicacion con los repositorios sustituidos por dobles; lo que decide la base esta en FieldRepositoryDataJpaTest. */
class BusinessLayerTest {

    static final Instant NOW = Instant.parse("2026-10-09T22:00:00Z");
    static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    static final DevicePrincipal TECHNICIAN = new DevicePrincipal("campo.tecnico1", "4c2d6d2a-0000-0000-0000-000000000001");

    static ShiftSnapshot shift(UUID id, String teamCode, LocalDate date, String status) {
        return new ShiftSnapshot(id, "SH-" + teamCode, date, status, teamCode, "Team " + teamCode,
                NOW.minus(Duration.ofHours(1)), NOW.plus(Duration.ofHours(7)), List.of(1L));
    }

    static EventContext context(UUID possessionId, UUID shiftId, String deviceId, TeamMessage message) {
        return new EventContext(possessionId, shiftId, deviceId, TECHNICIAN, message);
    }

    static TeamMessage.Builder message(String deviceId, long sequence) {
        return TeamMessage.newBuilder().setDeviceId(deviceId).setSequence(sequence).setOccurredAt(CommandDraft.timestamp(NOW));
    }

    @Nested
    class Possessions {

        private final PossessionRepository possessions = mock(PossessionRepository.class);
        private final PossessionShiftRepository shifts = mock(PossessionShiftRepository.class);
        private final MaintenanceClient maintenance = mock(MaintenanceClient.class);
        private final FieldCodeGenerator codes = mock(FieldCodeGenerator.class);
        private final ApplicationEventPublisher events = mock(ApplicationEventPublisher.class);
        private final PossessionServiceImpl service = new PossessionServiceImpl(possessions, shifts, maintenance, codes, events, CLOCK);

        private final UUID shiftA = UUID.randomUUID();
        private final UUID shiftB = UUID.randomUUID();

        @BeforeEach
        void stubs() {
            when(codes.nextPossessionCode()).thenReturn("PO-000001");
            when(possessions.saveAndFlush(any(Possession.class))).thenAnswer(invocation -> invocation.getArgument(0));
            when(possessions.save(any(Possession.class))).thenAnswer(invocation -> invocation.getArgument(0));
            when(maintenance.findShift(shiftA)).thenReturn(Optional.of(shift(shiftA, "T-A", LocalDate.of(2026, 10, 9), "IN_PROGRESS")));
            when(maintenance.findShift(shiftB)).thenReturn(Optional.of(shift(shiftB, "T-B", LocalDate.of(2026, 10, 9), "PLANNED")));
        }

        @Test
        void openReadsEveryShiftGeneratesTheCodeAndKeepsTheTeamCodes() {
            PossessionView view = service.open(List.of(shiftA, shiftB), null, "campo.responsable");

            assertThat(view.code()).isEqualTo("PO-000001");
            assertThat(view.shiftIds()).containsExactly(shiftA, shiftB);
            assertThat(view.status()).isEqualTo(PossessionStatus.OPEN);
            assertThat(view.endsAt()).as("sin ends_at, el primer turno que acaba").isEqualTo(NOW.plus(Duration.ofHours(7)));
            ArgumentCaptor<Possession> saved = ArgumentCaptor.forClass(Possession.class);
            verify(possessions).saveAndFlush(saved.capture());
            assertThat(saved.getValue().getShiftDate()).isEqualTo(LocalDate.of(2026, 10, 9));
            assertThat(saved.getValue().getOpenedAt()).isEqualTo(NOW);
            assertThat(saved.getValue().getOpenedBy()).isEqualTo("campo.responsable");
            assertThat(saved.getValue().getShifts()).extracting(PossessionShift::getTeamCode).containsExactly("T-A", "T-B");
            assertThat(saved.getValue().getShifts()).allMatch(PossessionShift::isOpen);
            assertThat(saved.getValue().getShifts()).allMatch(shift -> shift.getPossession() == saved.getValue());
        }

        @Test
        void openKeepsAnExplicitEndsAt() {
            Instant endsAt = NOW.plus(Duration.ofHours(5));

            assertThat(service.open(List.of(shiftA), endsAt, "campo.responsable").endsAt()).isEqualTo(endsAt);
        }

        @Test
        void openRejectsAShiftThatMaintenanceDoesNotKnow() {
            UUID unknown = UUID.randomUUID();
            when(maintenance.findShift(unknown)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> service.open(List.of(shiftA, unknown), null, "campo.responsable"))
                    .isInstanceOf(InvalidPossessionRequestException.class)
                    .hasMessageContaining(unknown.toString());
            verify(possessions, never()).saveAndFlush(any());
        }

        @Test
        void openRejectsNoShiftsARepeatedShiftAndShiftsOnDifferentDates() {
            assertThatThrownBy(() -> service.open(List.of(), null, "campo.responsable")).isInstanceOf(InvalidPossessionRequestException.class);
            assertThatThrownBy(() -> service.open(List.of(shiftA, shiftA), null, "campo.responsable"))
                    .isInstanceOf(InvalidPossessionRequestException.class).hasMessageContaining("repeated");
            UUID tomorrow = UUID.randomUUID();
            when(maintenance.findShift(tomorrow)).thenReturn(Optional.of(shift(tomorrow, "T-C", LocalDate.of(2026, 10, 10), "PLANNED")));
            assertThatThrownBy(() -> service.open(List.of(shiftA, tomorrow), null, "campo.responsable"))
                    .isInstanceOf(InvalidPossessionRequestException.class).hasMessageContaining("same date");
        }

        @Test
        void openRejectsAShiftMaintenanceAlreadyFinished() {
            UUID closed = UUID.randomUUID();
            when(maintenance.findShift(closed)).thenReturn(Optional.of(shift(closed, "T-C", LocalDate.of(2026, 10, 9), "CLOSED")));

            assertThatThrownBy(() -> service.open(List.of(closed), null, "campo.responsable"))
                    .isInstanceOf(ShiftNotWorkableException.class)
                    .satisfies(exception -> assertThat(((ShiftNotWorkableException) exception).getStatus()).isEqualTo("CLOSED"));
        }

        @Test
        void openRejectsAShiftAlreadyInAnOpenPossessionBeforeWritingAndWhenTheIndexSaysSo() {
            when(shifts.existsByShiftIdAndOpenTrue(shiftB)).thenReturn(true);
            assertThatThrownBy(() -> service.open(List.of(shiftA, shiftB), null, "campo.responsable"))
                    .isInstanceOf(ShiftAlreadyInOpenPossessionException.class).hasMessageContaining(shiftB.toString());
            verify(possessions, never()).saveAndFlush(any());

            when(shifts.existsByShiftIdAndOpenTrue(shiftB)).thenReturn(false);
            when(possessions.saveAndFlush(any(Possession.class))).thenThrow(new DataIntegrityViolationException("uq_possession_shift_open"));
            assertThatThrownBy(() -> service.open(List.of(shiftA, shiftB), null, "campo.responsable"))
                    .isInstanceOf(ShiftAlreadyInOpenPossessionException.class);
        }

        @Test
        void closeWithoutForceRefusesWhileTeamsAreOnTheTrackAndNamesThem() {
            UUID possessionId = UUID.randomUUID();
            Possession possession = openPossession();
            when(possessions.findWithLockById(possessionId)).thenReturn(Optional.of(possession));
            when(shifts.findByPossession_IdOrderByTeamCodeAsc(possessionId)).thenReturn(List.of(
                    PossessionShift.builder().shiftId(shiftA).teamCode("T-A").clearOfTrackAt(NOW).build(),
                    PossessionShift.builder().shiftId(shiftB).teamCode("T-B").build()));

            assertThatThrownBy(() -> service.close(possessionId, false, null, "campo.responsable"))
                    .isInstanceOf(PossessionNotAllClearException.class)
                    .satisfies(exception -> assertThat(((PossessionNotAllClearException) exception).getPendingTeams()).containsExactly("T-B"));
            assertThat(possession.getStatus()).isEqualTo(PossessionStatus.OPEN);
            verify(events, never()).publishEvent(any());
        }

        @Test
        void closeWithEveryoneClearClosesAndTellsTheStreams() {
            UUID possessionId = UUID.randomUUID();
            Possession possession = openPossession();
            when(possessions.findWithLockById(possessionId)).thenReturn(Optional.of(possession));
            when(shifts.findByPossession_IdOrderByTeamCodeAsc(possessionId)).thenReturn(List.of(
                    PossessionShift.builder().shiftId(shiftA).teamCode("T-A").clearOfTrackAt(NOW).build()));

            PossessionView view = service.close(possessionId, false, null, "campo.responsable");

            assertThat(view.status()).isEqualTo(PossessionStatus.CLOSED);
            assertThat(possession.getClosedAt()).isEqualTo(NOW);
            assertThat(possession.getClosedBy()).isEqualTo("campo.responsable");
            assertThat(possession.isForced()).isFalse();
            verify(shifts).closeAll(possessionId, "campo.responsable");
            verify(events).publishEvent(new PossessionClosed(possessionId));
        }

        @Test
        void aForcedCloseNeedsAReasonAndIsRecordedAsForcedOnlyIfSomeoneWasStillOnTheTrack() {
            UUID possessionId = UUID.randomUUID();
            assertThatThrownBy(() -> service.close(possessionId, true, " ", "campo.responsable"))
                    .isInstanceOf(InvalidPossessionRequestException.class).hasMessageContaining("reason");

            Possession possession = openPossession();
            when(possessions.findWithLockById(possessionId)).thenReturn(Optional.of(possession));
            when(shifts.findByPossession_IdOrderByTeamCodeAsc(possessionId)).thenReturn(List.of(
                    PossessionShift.builder().shiftId(shiftB).teamCode("T-B").build()));

            service.close(possessionId, true, "Tension restablecida por el CTC", "campo.responsable");

            assertThat(possession.getStatus()).isEqualTo(PossessionStatus.CLOSED);
            assertThat(possession.isForced()).isTrue();
            assertThat(possession.getCloseReason()).isEqualTo("Tension restablecida por el CTC");
            verify(events).publishEvent(new PossessionClosed(possessionId));
        }

        @Test
        void closeOfAClosedOrUnknownPossession() {
            UUID closedId = UUID.randomUUID();
            Possession closed = openPossession();
            closed.setStatus(PossessionStatus.CLOSED);
            when(possessions.findWithLockById(closedId)).thenReturn(Optional.of(closed));
            assertThatThrownBy(() -> service.close(closedId, false, null, "x")).isInstanceOf(PossessionNotOpenException.class);

            UUID unknown = UUID.randomUUID();
            when(possessions.findWithLockById(unknown)).thenReturn(Optional.empty());
            assertThatThrownBy(() -> service.close(unknown, false, null, "x")).isInstanceOf(PossessionNotFoundException.class);
            assertThatThrownBy(() -> service.get(unknown)).isInstanceOf(PossessionNotFoundException.class);
        }

        private Possession openPossession() {
            return Possession.builder().code("PO-000007").status(PossessionStatus.OPEN).shiftDate(LocalDate.of(2026, 10, 9))
                    .endsAt(NOW.plus(Duration.ofHours(7))).openedAt(NOW.minus(Duration.ofHours(1))).openedBy("campo.responsable").build();
        }
    }

    @Nested
    class Commands {

        private final FieldCommandRepository commands = mock(FieldCommandRepository.class);
        private final PossessionRepository possessions = mock(PossessionRepository.class);
        private final ApplicationEventPublisher events = mock(ApplicationEventPublisher.class);
        private final PlatformTransactionManager transactions = mock(PlatformTransactionManager.class);
        private final FieldCommandServiceImpl service = new FieldCommandServiceImpl(commands, possessions, events, transactions, CLOCK);
        private final UUID possessionId = UUID.randomUUID();

        @BeforeEach
        void stubs() {
            when(transactions.getTransaction(any())).thenAnswer(invocation -> new SimpleTransactionStatus());
        }

        @Test
        void issueTakesTheNextSequenceWritesTheMessageAsSentAndPublishesItAfterTheInsert() {
            when(possessions.nextCommandSequence(possessionId)).thenReturn(Optional.of(4L));
            EvacuateNow evacuate = EvacuateNow.newBuilder().setReason("Tren de trabajos").build();

            IssuedCommand issued = service.issue(possessionId, CommandDraft.evacuateNow("k-1", evacuate), "campo.responsable");

            assertThat(issued.sequence()).isEqualTo(4L);
            ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
            verify(commands).insert(eq(issued.id()), eq(possessionId), eq(4L), eq(FieldCommandKind.EVACUATE_NOW.name()), isNull(), eq("k-1"),
                    eq(true), eq(NOW), eq("campo.responsable"), payload.capture());
            FieldCommand stored = ProtoJson.parse(payload.getValue(), FieldCommand.newBuilder()).build();
            ArgumentCaptor<CommandCommitted> committed = ArgumentCaptor.forClass(CommandCommitted.class);
            verify(events).publishEvent(committed.capture());
            assertThat(committed.getValue().possessionId()).isEqualTo(possessionId);
            assertThat(committed.getValue().targetShiftId()).isNull();
            assertThat(committed.getValue().command()).as("lo publicado es exactamente lo guardado").isEqualTo(stored);
            assertThat(stored.getCommandId()).isEqualTo(issued.id().toString());
            assertThat(stored.getSequence()).isEqualTo(4L);
            assertThat(stored.getRequiresAck()).isTrue();
            assertThat(stored.getIssuedAt()).isEqualTo(CommandDraft.timestamp(NOW));
            assertThat(stored.getEvacuateNow()).isEqualTo(evacuate);
            verify(transactions).commit(any());
        }

        @Test
        void anEventResultIsAddressedToItsShiftAndNeedsNoAck() {
            UUID shiftId = UUID.randomUUID();
            when(possessions.nextCommandSequence(possessionId)).thenReturn(Optional.of(9L));

            service.issue(possessionId, CommandDraft.eventResult(shiftId, 12, EventResult.Outcome.REJECTED, "unknown command"), "system");

            verify(commands).insert(any(), eq(possessionId), eq(9L), eq(FieldCommandKind.EVENT_RESULT.name()), eq(shiftId), isNull(), eq(false),
                    eq(NOW), eq("system"), anyString());
            ArgumentCaptor<CommandCommitted> committed = ArgumentCaptor.forClass(CommandCommitted.class);
            verify(events).publishEvent(committed.capture());
            assertThat(committed.getValue().targetShiftId()).isEqualTo(shiftId);
            assertThat(committed.getValue().command().getEventResult().getSequence()).isEqualTo(12L);
            assertThat(committed.getValue().command().getEventResult().getOutcome()).isEqualTo(EventResult.Outcome.REJECTED);
            assertThat(committed.getValue().command().getEventResult().getReason()).isEqualTo("unknown command");
        }

        @Test
        void aRepeatedIdempotencyKeyReturnsTheExistingCommandWithoutTouchingTheCounter() {
            FieldCommandRecord existing = mock(FieldCommandRecord.class);
            UUID existingId = UUID.randomUUID();
            when(existing.getId()).thenReturn(existingId);
            when(existing.getSequence()).thenReturn(3L);
            when(commands.findByPossessionIdAndIdempotencyKey(possessionId, "k-1")).thenReturn(Optional.of(existing));

            IssuedCommand issued = service.issue(possessionId, CommandDraft.evacuateNow("k-1", EvacuateNow.getDefaultInstance()), "x");

            assertThat(issued).isEqualTo(new IssuedCommand(existingId, 3L));
            verify(possessions, never()).nextCommandSequence(any());
            verify(commands, never()).insert(any(), any(), anyLong(), any(), any(), any(), anyBoolean(), any(), any(), any());
            verify(events, never()).publishEvent(any());
        }

        @Test
        void twoRetriesAtOnceWithTheSameKeyEndWithTheOneThatCommitted() {
            FieldCommandRecord winner = mock(FieldCommandRecord.class);
            UUID winnerId = UUID.randomUUID();
            when(winner.getId()).thenReturn(winnerId);
            when(winner.getSequence()).thenReturn(5L);
            when(commands.findByPossessionIdAndIdempotencyKey(possessionId, "k-2")).thenReturn(Optional.empty(), Optional.of(winner));
            when(possessions.nextCommandSequence(possessionId)).thenReturn(Optional.of(6L));
            when(commands.insert(any(), any(), anyLong(), any(), any(), any(), anyBoolean(), any(), any(), any()))
                    .thenThrow(new DataIntegrityViolationException("uq_field_command_idempotency_key"));

            IssuedCommand issued = service.issue(possessionId, CommandDraft.evacuateNow("k-2", EvacuateNow.getDefaultInstance()), "x");

            assertThat(issued).isEqualTo(new IssuedCommand(winnerId, 5L));
            verify(transactions).rollback(any());
            verify(events, never()).publishEvent(any());
        }

        @Test
        void issueOnAClosedOrUnknownPossession() {
            when(possessions.nextCommandSequence(possessionId)).thenReturn(Optional.empty());
            when(possessions.existsById(possessionId)).thenReturn(true);
            assertThatThrownBy(() -> service.issue(possessionId, CommandDraft.evacuateNow(null, EvacuateNow.getDefaultInstance()), "x"))
                    .isInstanceOf(PossessionNotOpenException.class);

            when(possessions.existsById(possessionId)).thenReturn(false);
            assertThatThrownBy(() -> service.issue(possessionId, CommandDraft.evacuateNow(null, EvacuateNow.getDefaultInstance()), "x"))
                    .isInstanceOf(PossessionNotFoundException.class);
            verify(commands, never()).insert(any(), any(), anyLong(), any(), any(), any(), anyBoolean(), any(), any(), any());
        }

        @Test
        void aStoredCommandIsReadBackExactlyAsItWasSent() {
            UUID shiftId = UUID.randomUUID();
            FieldCommand sent = CommandDraft.supervisorMessage(null, com.alejandro.mtofield.grpc.v1.SupervisorMessage.newBuilder()
                    .setAuthor("campo.responsable").setText("Pausa de 10 min").build()).build(UUID.randomUUID(), 7L, NOW);
            FieldCommandRecord record = mock(FieldCommandRecord.class);
            when(record.getId()).thenReturn(UUID.fromString(sent.getCommandId()));
            when(record.getSequence()).thenReturn(7L);
            when(record.getTargetShiftId()).thenReturn(null);
            when(record.getPayload()).thenReturn(ProtoJson.print(sent));
            when(commands.findReplay(eq(possessionId), eq(shiftId), eq(3L), any())).thenReturn(List.of(record));

            List<StoredCommand> replay = service.replayAfter(possessionId, shiftId, 3L, 100);

            assertThat(replay).hasSize(1);
            assertThat(replay.getFirst().command()).isEqualTo(sent);
            assertThat(replay.getFirst().addressedTo(shiftId)).isTrue();
            assertThat(service.range(possessionId, 5, 4)).isEmpty();
        }
    }

    @Nested
    class Events {

        private final FieldEventRepository eventRepository = mock(FieldEventRepository.class);
        private final FieldCommandRepository commandRepository = mock(FieldCommandRepository.class);
        private final CommandAckRepository ackRepository = mock(CommandAckRepository.class);
        private final PossessionShiftRepository shiftRepository = mock(PossessionShiftRepository.class);
        private final FieldCommandService commands = mock(FieldCommandService.class);
        private final FieldEventServiceImpl service = new FieldEventServiceImpl(eventRepository, commandRepository, ackRepository, shiftRepository,
                commands, CLOCK, new FieldMetrics(new SimpleMeterRegistry()));
        private final UUID possessionId = UUID.randomUUID();
        private final UUID shiftId = UUID.randomUUID();

        @BeforeEach
        void stubs() {
            when(eventRepository.insertIfMissing(any(), any(), anyLong(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(1);
        }

        @Test
        void anAckOfACommandOfThisPossessionIsStoredOncePerTeamAndAnsweredApplied() {
            UUID commandId = UUID.randomUUID();
            FieldCommandRecord command = command(commandId, possessionId, true);
            when(command.getIssuedAt()).thenReturn(NOW.minusSeconds(20));
            when(commandRepository.findById(commandId)).thenReturn(Optional.of(command));
            when(ackRepository.insertIfMissing(any(), any(), any(), any(), anyBoolean(), any())).thenReturn(1);
            TeamMessage message = message("dev-1", 5).setCommandAck(CommandAck.newBuilder().setCommandId(commandId.toString()).setAccepted(true)).build();

            service.recordAck(context(possessionId, shiftId, "dev-1", message));

            verify(eventRepository).insertIfMissing(any(), eq("dev-1"), eq(5L), eq(possessionId), eq(shiftId), eq("COMMAND_ACK"), eq(NOW),
                    eq("campo.tecnico1"), eq(ProtoJson.print(message)), eq("NOT_REQUIRED"), isNull());
            verify(ackRepository).insertIfMissing(commandId, shiftId, "dev-1", "campo.tecnico1", true, null);
            assertAnswered(5, EventResult.Outcome.APPLIED, null);
        }

        @Test
        void anAckOfAnUnknownCommandOrOfAnotherPossessionIsRejectedInBand() {
            TeamMessage garbage = message("dev-1", 6).setCommandAck(CommandAck.newBuilder().setCommandId("not-a-uuid")).build();
            service.recordAck(context(possessionId, shiftId, "dev-1", garbage));
            assertAnswered(6, EventResult.Outcome.REJECTED, FieldEventServiceImpl.UNKNOWN_COMMAND);

            UUID foreign = UUID.randomUUID();
            FieldCommandRecord ofAnotherPossession = command(foreign, UUID.randomUUID(), true);
            when(commandRepository.findById(foreign)).thenReturn(Optional.of(ofAnotherPossession));
            service.recordAck(context(possessionId, shiftId, "dev-1", message("dev-1", 7)
                    .setCommandAck(CommandAck.newBuilder().setCommandId(foreign.toString())).build()));
            assertAnswered(7, EventResult.Outcome.REJECTED, FieldEventServiceImpl.UNKNOWN_COMMAND);

            UUID noAck = UUID.randomUUID();
            FieldCommandRecord withoutAck = command(noAck, possessionId, false);
            when(commandRepository.findById(noAck)).thenReturn(Optional.of(withoutAck));
            service.recordAck(context(possessionId, shiftId, "dev-1", message("dev-1", 8)
                    .setCommandAck(CommandAck.newBuilder().setCommandId(noAck.toString())).build()));
            assertAnswered(8, EventResult.Outcome.REJECTED, FieldEventServiceImpl.COMMAND_WITHOUT_ACK);
            verify(ackRepository, never()).insertIfMissing(any(), any(), any(), any(), anyBoolean(), any());
        }

        @Test
        void aResentAckWalksTheSameIdempotentPathAndGetsTheSameAnswer() {
            UUID commandId = UUID.randomUUID();
            FieldCommandRecord command = command(commandId, possessionId, true);
            FieldEventRecord stored = event(FieldEventSyncStatus.NOT_REQUIRED, null);
            when(commandRepository.findById(commandId)).thenReturn(Optional.of(command));
            when(eventRepository.insertIfMissing(any(), any(), anyLong(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(0);
            when(eventRepository.findByDeviceIdAndSequence("dev-1", 5)).thenReturn(Optional.of(stored));
            when(ackRepository.insertIfMissing(any(), any(), any(), any(), anyBoolean(), any())).thenReturn(0);

            service.recordAck(context(possessionId, shiftId, "dev-1", message("dev-1", 5)
                    .setCommandAck(CommandAck.newBuilder().setCommandId(commandId.toString()).setAccepted(true)).build()));

            assertAnswered(5, EventResult.Outcome.APPLIED, null);
        }

        @Test
        void clearOfTrackMarksTheShiftOnceAndIsAppliedAlsoWhenResent() {
            when(shiftRepository.markClear(possessionId, shiftId, "campo.tecnico1", "dev-2", true)).thenReturn(1, 0);
            TeamMessage message = message("dev-2", 3).setClearOfTrack(ClearOfTrack.newBuilder().setEarthingRemoved(true)).build();

            service.recordClearOfTrack(context(possessionId, shiftId, "dev-2", message));
            service.recordClearOfTrack(context(possessionId, shiftId, "dev-2", message));

            verify(commands, org.mockito.Mockito.times(2)).issue(eq(possessionId), any(), eq("system"));
            assertAnswered(3, EventResult.Outcome.APPLIED, null);
        }

        @Test
        void aNewTaskEventIsStoredPendingAndAnsweredByWhoeverSynchronizesIt() {
            TeamMessage message = message("dev-1", 9).setTaskStarted(TaskStarted.newBuilder().setOrderId("o").setTaskId("t")).build();

            StoredEvent stored = service.recordTaskEvent(context(possessionId, shiftId, "dev-1", message));

            assertThat(stored.inserted()).isTrue();
            assertThat(stored.status()).isEqualTo(FieldEventSyncStatus.PENDING);
            assertThat(stored.id()).isNotNull();
            verify(eventRepository).insertIfMissing(eq(stored.id()), eq("dev-1"), eq(9L), eq(possessionId), eq(shiftId), eq("TASK_STARTED"), eq(NOW),
                    eq("campo.tecnico1"), anyString(), eq("PENDING"), eq(NOW));
            verify(commands, never()).issue(any(), any(), any());
        }

        @Test
        void aResentTaskEventIsAnsweredFromItsStoredStateAndNotQueuedAgain() {
            when(eventRepository.insertIfMissing(any(), any(), anyLong(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(0);
            TeamMessage message = message("dev-1", 9).setTaskStarted(TaskStarted.newBuilder().setOrderId("o").setTaskId("t")).build();
            EventContext context = context(possessionId, shiftId, "dev-1", message);

            FieldEventRecord pending = event(FieldEventSyncStatus.PENDING, null);
            when(eventRepository.findByDeviceIdAndSequence("dev-1", 9)).thenReturn(Optional.of(pending));
            assertThat(service.recordTaskEvent(context).inserted()).isFalse();
            assertAnswered(9, EventResult.Outcome.PENDING_SYNC, null);

            FieldEventRecord synced = event(FieldEventSyncStatus.SYNCED, null);
            when(eventRepository.findByDeviceIdAndSequence("dev-1", 9)).thenReturn(Optional.of(synced));
            assertThat(service.recordTaskEvent(context).status()).isEqualTo(FieldEventSyncStatus.SYNCED);
            assertAnswered(9, EventResult.Outcome.APPLIED, null);

            FieldEventRecord rejected = event(FieldEventSyncStatus.REJECTED, "task is not IN_PROGRESS");
            when(eventRepository.findByDeviceIdAndSequence("dev-1", 9)).thenReturn(Optional.of(rejected));
            when(eventRepository.findById(rejected.getId())).thenReturn(Optional.of(rejected));
            service.recordTaskEvent(context);
            assertAnswered(9, EventResult.Outcome.REJECTED, "task is not IN_PROGRESS");
        }

        @Test
        void sequenceZeroIsOnlyForHeartbeats() {
            TeamMessage message = message("dev-1", 0).setTaskStarted(TaskStarted.newBuilder().setOrderId("o").setTaskId("t")).build();

            StoredEvent stored = service.recordTaskEvent(context(possessionId, shiftId, "dev-1", message));

            assertThat(stored).isEqualTo(StoredEvent.rejected());
            verify(eventRepository, never()).insertIfMissing(any(), any(), anyLong(), any(), any(), any(), any(), any(), any(), any(), any());
            assertAnswered(0, EventResult.Outcome.REJECTED, FieldEventServiceImpl.SEQUENCE_REQUIRED);
        }

        @Test
        void thePendingSynchronizerOfPhaseOneAnswersPendingSync() {
            new PendingSyncEventSynchronizer(commands).process(new SyncJob(UUID.randomUUID(), context(possessionId, shiftId, "dev-1",
                    message("dev-1", 11).setTaskStarted(TaskStarted.getDefaultInstance()).build())));

            assertAnswered(11, EventResult.Outcome.PENDING_SYNC, null);
        }

        private void assertAnswered(long sequence, EventResult.Outcome outcome, String reason) {
            ArgumentCaptor<CommandDraft> draft = ArgumentCaptor.forClass(CommandDraft.class);
            verify(commands, org.mockito.Mockito.atLeastOnce()).issue(eq(possessionId), draft.capture(), eq("system"));
            FieldCommand command = draft.getValue().build(UUID.randomUUID(), 1, NOW);
            assertThat(draft.getValue().kind()).isEqualTo(FieldCommandKind.EVENT_RESULT);
            assertThat(draft.getValue().targetShiftId()).isEqualTo(shiftId);
            assertThat(draft.getValue().requiresAck()).isFalse();
            assertThat(command.getEventResult().getSequence()).isEqualTo(sequence);
            assertThat(command.getEventResult().getOutcome()).isEqualTo(outcome);
            assertThat(command.getEventResult().getReason()).isEqualTo(reason == null ? "" : reason);
        }

        private FieldCommandRecord command(UUID id, UUID ofPossession, boolean requiresAck) {
            FieldCommandRecord record = mock(FieldCommandRecord.class);
            when(record.getId()).thenReturn(id);
            when(record.getPossessionId()).thenReturn(ofPossession);
            when(record.isRequiresAck()).thenReturn(requiresAck);
            return record;
        }

        private FieldEventRecord event(FieldEventSyncStatus status, String lastError) {
            FieldEventRecord record = mock(FieldEventRecord.class);
            when(record.getId()).thenReturn(UUID.randomUUID());
            when(record.getSyncStatus()).thenReturn(status);
            when(record.getLastError()).thenReturn(lastError);
            return record;
        }
    }

    @Nested
    class Board {

        private final PossessionRepository possessions = mock(PossessionRepository.class);
        private final PossessionShiftRepository shifts = mock(PossessionShiftRepository.class);
        private final FieldCommandRepository commands = mock(FieldCommandRepository.class);
        private final CommandAckRepository acks = mock(CommandAckRepository.class);
        private final DeviceStreamPresence presence = mock(DeviceStreamPresence.class);
        private final LivenessRegistry liveness = mock(LivenessRegistry.class);
        private final BoardPublisher publisher = mock(BoardPublisher.class);
        private final PlatformTransactionManager transactions = mock(PlatformTransactionManager.class);
        private final UUID possessionId = UUID.randomUUID();
        private final UUID shiftA = UUID.randomUUID();
        private final UUID shiftB = UUID.randomUUID();
        private final UUID shiftC = UUID.randomUUID();

        @BeforeEach
        void stubs() {
            when(transactions.getTransaction(any())).thenAnswer(invocation -> new SimpleTransactionStatus());
            when(possessions.findById(possessionId)).thenReturn(Optional.of(Possession.builder().code("PO-000009").status(PossessionStatus.OPEN)
                    .shiftDate(LocalDate.of(2026, 10, 9)).endsAt(NOW.plus(Duration.ofHours(6))).openedAt(NOW).openedBy("x").build()));
            when(shifts.findByPossession_IdOrderByTeamCodeAsc(possessionId)).thenReturn(List.of(
                    PossessionShift.builder().shiftId(shiftA).teamCode("T-A").clearOfTrackAt(NOW).build(),
                    PossessionShift.builder().shiftId(shiftB).teamCode("T-B").build(),
                    PossessionShift.builder().shiftId(shiftC).teamCode("T-C").build()));
        }

        private PossessionBoardServiceImpl service(Executor executor) {
            FieldProperties properties = new FieldProperties(256, 256, 64, Duration.ofMinutes(5), 200, Duration.ofSeconds(30),
                    new FieldProperties.Liveness(Duration.ofSeconds(30), Duration.ofSeconds(60)), new FieldProperties.Board(Duration.ofSeconds(5)),
                    new FieldProperties.TokenExpiry(false, Duration.ofSeconds(30)));
            return new PossessionBoardServiceImpl(possessions, shifts, commands, acks, presence, liveness, publisher, transactions, executor, CLOCK,
                    properties, new FieldMetrics(new SimpleMeterRegistry()));
        }

        @Test
        void theBoardTellsWhoIsAliveWhoAckedAndWhetherACommandWasSentOrIsQueued() {
            UUID broadcastId = UUID.randomUUID();
            UUID targetedId = UUID.randomUUID();
            FieldCommandRecord broadcast = command(broadcastId, 7, null);
            FieldCommandRecord targeted = command(targetedId, 9, shiftC);
            CommandAckRecord ackA = ack(broadcastId, shiftA);
            CommandAckRecord ackB = ack(broadcastId, shiftB);
            when(commands.findByPossessionIdAndRequiresAckTrueOrderBySequenceAsc(possessionId)).thenReturn(List.of(broadcast, targeted));
            when(acks.findByCommandIdIn(List.of(broadcastId, targetedId))).thenReturn(List.of(ackA, ackB));
            when(presence.streamsOf(possessionId)).thenReturn(List.of(
                    new DeviceStreamPresence.StreamPresence("dev-a", shiftA, "T-A", 5),
                    new DeviceStreamPresence.StreamPresence("dev-c", shiftC, "T-C", 9)));
            when(liveness.ofPossession(possessionId)).thenReturn(List.of(
                    new LivenessRegistry.DeviceLiveness("dev-a", shiftA, possessionId, NOW.minusSeconds(5), "12.345", 80, -90, true),
                    new LivenessRegistry.DeviceLiveness("dev-a2", shiftA, possessionId, NOW.minusSeconds(200), "11.000", 10, -100, false),
                    new LivenessRegistry.DeviceLiveness("dev-b", shiftB, possessionId, NOW.minusSeconds(45), "20.000", 50, -95, true)));

            BoardSnapshot board = service(Runnable::run).snapshot(possessionId);

            assertThat(board.status()).isEqualTo(PossessionStatus.OPEN);
            assertThat(board.allClear()).isFalse();
            assertThat(board.teams()).extracting(BoardSnapshot.TeamState::teamCode).containsExactly("T-A", "T-B", "T-C");
            assertThat(board.teams()).extracting(BoardSnapshot.TeamState::liveness)
                    .containsExactly(TeamLiveness.Liveness.CONNECTED, TeamLiveness.Liveness.STALE, TeamLiveness.Liveness.DISCONNECTED);
            assertThat(board.teams().getFirst().kp()).as("el dispositivo que hablo el ultimo").isEqualTo("12.345");
            assertThat(board.teams().getFirst().lastSeen()).isEqualTo(NOW.minusSeconds(5));
            assertThat(board.teams().getFirst().clearOfTrack()).isTrue();
            assertThat(board.teams().get(2).lastSeen()).isNull();
            assertThat(board.commands()).hasSize(2);
            CommandAckSummary broadcastAcks = board.commands().getFirst().acks();
            assertThat(broadcastAcks.ackedBy()).containsExactly("T-A", "T-B");
            assertThat(broadcastAcks.pending()).containsExactly("T-C");
            assertThat(broadcastAcks.sentTo()).containsExactly("T-C");
            assertThat(broadcastAcks.queuedFor()).isEmpty();
            CommandAckSummary targetedAcks = board.commands().get(1).acks();
            assertThat(targetedAcks.pending()).as("una orden dirigida solo cuenta a su equipo").containsExactly("T-C");
            assertThat(targetedAcks.sentTo()).containsExactly("T-C");
            assertThat(board.connectedTeams()).isEqualTo(1);
            assertThat(board.commandsPendingAck()).isEqualTo(2);
        }

        @Test
        void manyChangesInARowAreCoalescedIntoFewRecomputesAndTheLastOneWins() throws Exception {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            CountDownLatch firstPublished = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            List<BoardSnapshot> published = new java.util.concurrent.CopyOnWriteArrayList<>();
            org.mockito.Mockito.doAnswer(invocation -> {
                published.add(invocation.getArgument(0));
                if (published.size() == 1) {
                    firstPublished.countDown();
                    release.await(5, TimeUnit.SECONDS);
                }
                return null;
            }).when(publisher).publish(any());
            PossessionBoardServiceImpl service = service(executor);

            service.markDirty(possessionId);
            assertThat(firstPublished.await(5, TimeUnit.SECONDS)).isTrue();
            for (int index = 0; index < 20; index++) {
                service.markDirty(possessionId);
            }
            release.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

            assertThat(published).as("uno en marcha y uno despues, no veinte").hasSizeBetween(2, 3);
            assertThat(published.getLast().version()).isGreaterThan(published.getFirst().version());
            org.mockito.Mockito.verify(possessions, org.mockito.Mockito.atMost(3)).findById(possessionId);
        }

        @Test
        void theTickOnlyTouchesWatchedPossessionsAndClosingPublishesTheLastBoardAndCompletes() {
            PossessionBoardServiceImpl service = service(Runnable::run);
            when(publisher.watchedPossessions()).thenReturn(java.util.Set.of(possessionId));

            service.tick();
            verify(publisher).publish(any());

            service.possessionClosed(possessionId);
            ArgumentCaptor<BoardSnapshot> last = ArgumentCaptor.forClass(BoardSnapshot.class);
            verify(publisher).completeAll(last.capture());
            assertThat(last.getValue().possessionId()).isEqualTo(possessionId);
            when(publisher.watchedPossessions()).thenReturn(java.util.Set.of());
            service.tick();
            verify(publisher, org.mockito.Mockito.times(1)).publish(any());
        }

        private FieldCommandRecord command(UUID id, long sequence, UUID target) {
            FieldCommandRecord record = mock(FieldCommandRecord.class);
            when(record.getId()).thenReturn(id);
            when(record.getSequence()).thenReturn(sequence);
            when(record.getKind()).thenReturn(FieldCommandKind.EVACUATE_NOW);
            when(record.getIssuedAt()).thenReturn(NOW.minusSeconds(60));
            when(record.getTargetShiftId()).thenReturn(target);
            when(record.isBroadcast()).thenReturn(target == null);
            when(record.addressedTo(any())).thenAnswer(invocation -> target == null || target.equals(invocation.getArgument(0)));
            return record;
        }

        private CommandAckRecord ack(UUID commandId, UUID shiftId) {
            CommandAckRecord record = mock(CommandAckRecord.class);
            when(record.getCommandId()).thenReturn(commandId);
            when(record.getShiftId()).thenReturn(shiftId);
            return record;
        }
    }

    @Nested
    class Maintenance {

        @Test
        void theDisconnectedClientInventsADeterministicShiftInProgressForAnyId() {
            NoOpMaintenanceClient client = new NoOpMaintenanceClient(CLOCK);
            UUID shiftId = UUID.fromString("a1b2c3d4-0000-4000-8000-000000000000");

            ShiftSnapshot first = client.findShift(shiftId).orElseThrow();

            assertThat(client.isEnabled()).isFalse();
            assertThat(client.findShift(shiftId)).contains(first);
            assertThat(first.teamCode()).isEqualTo("T-A1B2");
            assertThat(first.status()).isEqualTo("IN_PROGRESS");
            assertThat(first.shiftDate()).isEqualTo(LocalDate.of(2026, 10, 9));
            assertThat(first.plannedEnd()).isEqualTo(NOW.plus(Duration.ofHours(8)));
            assertThat(client.findShift(UUID.randomUUID())).isPresent();
        }

        @Test
        void withTheClientEnabledTheContextRefusesToStartUntilPhaseTwo() {
            ApplicationContextRunner runner = new ApplicationContextRunner().withUserConfiguration(MaintenanceClientConfiguration.class)
                    .withPropertyValues("app.maintenance.base-url=http://localhost:8083", "app.maintenance.client-registration-id=mto-services");

            runner.withPropertyValues("app.maintenance.enabled=true").run(context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure()).rootCause().hasMessageContaining("Phase 2").hasMessageContaining("APP_MAINTENANCE_ENABLED=false");
            });
            runner.withPropertyValues("app.maintenance.enabled=false").run(context -> assertThat(context).hasNotFailed());
        }
    }

    @Nested
    class Liveness {

        private final InMemoryLivenessRegistry registry = new InMemoryLivenessRegistry(CLOCK);
        private final UUID possessionId = UUID.randomUUID();
        private final UUID shiftId = UUID.randomUUID();

        @Test
        void aDeviceIsKnownFromItsStreamAndKeepsItsLastHeartbeatAfterTheStreamCloses() {
            registry.streamOpened("dev-1", shiftId, possessionId);
            registry.heartbeat("dev-1", "34.271", 81, -97);
            registry.heartbeat("ghost", "0", 0, 0);

            List<LivenessRegistry.DeviceLiveness> devices = registry.ofPossession(possessionId);
            assertThat(devices).hasSize(1);
            assertThat(devices.getFirst()).isEqualTo(new LivenessRegistry.DeviceLiveness("dev-1", shiftId, possessionId, NOW, "34.271", 81, -97, true));

            registry.streamClosed("dev-1");
            assertThat(registry.ofPossession(possessionId).getFirst().streamOpen()).isFalse();
            assertThat(registry.ofPossession(possessionId).getFirst().kp()).isEqualTo("34.271");

            registry.streamOpened("dev-1", shiftId, possessionId);
            assertThat(registry.ofPossession(possessionId).getFirst().streamOpen()).isTrue();
            assertThat(registry.ofPossession(possessionId).getFirst().batteryPct()).as("lo ultimo que dijo sobrevive a la reconexion").isEqualTo(81);

            registry.forget(possessionId);
            assertThat(registry.ofPossession(possessionId)).isEmpty();
            registry.touch("dev-1");
            assertThat(registry.ofPossession(possessionId)).isEmpty();
        }

        @Test
        void aDeviceThatMovesToAnotherPossessionLeavesTheFirstOne() {
            UUID other = UUID.randomUUID();
            registry.streamOpened("dev-1", shiftId, possessionId);
            registry.streamOpened("dev-1", shiftId, other);

            assertThat(registry.ofPossession(possessionId)).isEmpty();
            assertThat(registry.ofPossession(other)).extracting(LivenessRegistry.DeviceLiveness::deviceId).containsExactly("dev-1");
        }
    }
}
