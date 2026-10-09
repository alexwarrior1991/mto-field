package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.application.dto.CommandDraft;
import com.alejandro.mtofield.application.dto.DevicePrincipal;
import com.alejandro.mtofield.application.dto.EventContext;
import com.alejandro.mtofield.application.dto.StoredCommand;
import com.alejandro.mtofield.application.dto.SyncJob;
import com.alejandro.mtofield.application.event.CommandCommitted;
import com.alejandro.mtofield.application.service.FieldCommandService;
import com.alejandro.mtofield.application.service.FieldEventService;
import com.alejandro.mtofield.application.service.FieldEventSynchronizer;
import com.alejandro.mtofield.application.service.PossessionBoardService;
import com.alejandro.mtofield.grpc.v1.FieldCommand;
import com.alejandro.mtofield.grpc.v1.SupervisorMessage;
import com.alejandro.mtofield.grpc.v1.TeamMessage;
import com.alejandro.mtofield.grpc.v1.Welcome;
import com.alejandro.mtofield.infrastructure.grpc.GrpcErrors;
import com.alejandro.mtofield.configuration.metrics.FieldMetrics;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * El nucleo de streams con un observador falso cuyo {@code isReady} se controla a mano: lo que
 * el servidor in-process no deja provocar de forma determinista (un drenado a medias, una cola
 * llena, el orden entre la reproduccion y lo que llega en vivo).
 */
class DeviceStreamTest {

    static final Instant NOW = Instant.parse("2026-10-09T22:00:00Z");
    static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    static final UUID POSSESSION = UUID.randomUUID();
    static final UUID SHIFT = UUID.randomUUID();
    static final DevicePrincipal PRINCIPAL = new DevicePrincipal("campo.tecnico1", "sub");

    static FieldCommand command(long sequence) {
        return CommandDraft.supervisorMessage(null, SupervisorMessage.newBuilder().setText("#" + sequence).build())
                .build(UUID.randomUUID(), sequence, NOW);
    }

    static FieldCommand welcome() {
        return FieldCommand.newBuilder().setCommandId("welcome").setSequence(0)
                .setWelcome(Welcome.newBuilder().setPossessionId(POSSESSION.toString()).setLastAppliedSequence(0)).build();
    }

    static List<Long> sequences(List<FieldCommand> commands) {
        return commands.stream().map(FieldCommand::getSequence).toList();
    }

    /** Un ServerCallStreamObserver cuyo estado de listo se decide desde el test. */
    static final class FakeCall extends ServerCallStreamObserver<FieldCommand> {

        volatile boolean ready = true;
        final List<FieldCommand> sent = new CopyOnWriteArrayList<>();
        final List<Throwable> errors = new CopyOnWriteArrayList<>();
        final AtomicInteger completed = new AtomicInteger();
        volatile Consumer<FieldCommand> duringOnNext = command -> {
        };

        @Override
        public void onNext(FieldCommand value) {
            sent.add(value);
            duringOnNext.accept(value);
        }

        @Override
        public void onError(Throwable throwable) {
            errors.add(throwable);
        }

        @Override
        public void onCompleted() {
            completed.incrementAndGet();
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnCancelHandler(Runnable onCancelHandler) {
        }

        @Override
        public void setCompression(String compression) {
        }

        @Override
        public boolean isReady() {
            return ready;
        }

        @Override
        public void setOnReadyHandler(Runnable onReadyHandler) {
        }

        @Override
        public void disableAutoInboundFlowControl() {
        }

        @Override
        public void request(int count) {
        }

        @Override
        public void setMessageCompression(boolean enable) {
        }
    }

    static final class Harness {

        final FakeCall call = new FakeCall();
        final FieldMetrics metrics = new FieldMetrics(new SimpleMeterRegistry());
        final List<DeviceStream> closed = new CopyOnWriteArrayList<>();
        final DeviceStream stream;

        Harness(int outboundCapacity, int heldCapacity, int pageSize, Duration putTimeout) {
            stream = new DeviceStream("dev-1", SHIFT, POSSESSION, "T-A", PRINCIPAL, NOW.plus(Duration.ofHours(8)), call, outboundCapacity,
                    heldCapacity, pageSize, putTimeout, metrics, closed::add);
        }

        static Harness standard() {
            return new Harness(8, 8, 2, Duration.ofSeconds(5));
        }

        /** Un stream que ya paso por el catch-up sin atraso: en vivo desde el principio. */
        Harness live() {
            stream.catchUp(0, welcome(), (after, limit) -> List.of(), CatchUpProbe.NONE);
            assertThat(stream.mode()).isEqualTo(DeviceStream.Mode.LIVE);
            call.sent.clear();
            return this;
        }
    }

    @Nested
    class Draining {

        @Test
        void accumulatesWhileTheTransportIsNotReadyAndDrainsInOrderWhenItIs() {
            Harness h = Harness.standard().live();
            h.call.ready = false;

            h.stream.offer(command(1));
            h.stream.offer(command(2));
            h.stream.offer(command(3));
            assertThat(h.call.sent).isEmpty();
            assertThat(h.stream.outboundDepth()).isEqualTo(3);

            h.call.ready = true;
            h.stream.drain();

            assertThat(sequences(h.call.sent)).containsExactly(1L, 2L, 3L);
            assertThat(h.stream.lastSentSequence()).isEqualTo(3);
            assertThat(h.stream.outboundDepth()).isZero();
            assertThat(h.metrics.registry().get(FieldMetrics.NOT_READY).timer().count()).as("el tiempo no listo se apunta al volver").isEqualTo(1);
        }

        @Test
        void anOfferThatArrivesWhileAnotherThreadIsDrainingIsNotLost() {
            Harness h = Harness.standard().live();
            // Una oferta durante un drenado: su propio drain() pierde el CAS y vuelve sin escribir;
            // el drenador en curso la ve en la re-comprobacion.
            h.call.duringOnNext = command -> {
                if (command.getSequence() == 1) {
                    h.stream.offer(command(2));
                }
            };

            h.stream.offer(command(1));

            assertThat(sequences(h.call.sent)).containsExactly(1L, 2L);
        }

        @Test
        void aSequenceAlreadyEnqueuedIsIgnored() {
            Harness h = Harness.standard().live();

            h.stream.offer(command(5));
            h.stream.offer(command(5));
            h.stream.offer(command(4));
            h.stream.offer(command(6));

            assertThat(sequences(h.call.sent)).containsExactly(5L, 6L);
        }

        @Test
        void anOverflowClosesWithResourceExhaustedExactlyOnceFromTheDrainer() {
            Harness h = new Harness(2, 8, 2, Duration.ofSeconds(5)).live();
            h.call.ready = false;

            h.stream.offer(command(1));
            h.stream.offer(command(2));
            h.stream.offer(command(3));
            h.stream.offer(command(4));

            assertThat(h.call.errors).hasSize(1);
            StatusRuntimeException error = (StatusRuntimeException) h.call.errors.getFirst();
            assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
            assertThat(GrpcErrors.reasonOf(error)).isEqualTo(DeviceStream.REASON_OUTBOUND_FULL);
            assertThat(h.call.sent).as("nada se escribe con el transporte sin listo").isEmpty();
            assertThat(h.stream.isClosed()).isTrue();
            assertThat(h.closed).containsExactly(h.stream);
            h.call.ready = true;
            h.stream.offer(command(5));
            h.stream.drain();
            assertThat(h.call.sent).isEmpty();
            assertThat(h.call.errors).hasSize(1);
            assertThat(h.call.completed).hasValue(0);
        }

        @Test
        void completeDuringADrainEmitsASingleOnCompletedAndNothingAfterIt() {
            Harness h = Harness.standard().live();
            h.call.ready = false;
            h.stream.offer(command(1));
            h.stream.offer(command(2));
            h.stream.offer(command(3));
            h.call.duringOnNext = command -> {
                if (command.getSequence() == 1) {
                    h.stream.complete();
                    h.stream.complete();
                }
            };
            h.call.ready = true;

            h.stream.drain();

            assertThat(sequences(h.call.sent)).containsExactly(1L);
            assertThat(h.call.completed).hasValue(1);
            assertThat(h.call.errors).isEmpty();
            assertThat(h.closed).hasSize(1);
        }

        @Test
        void supersedeClosesWithAbortedAndItsReason() {
            Harness h = Harness.standard().live();

            h.stream.supersede();

            StatusRuntimeException error = (StatusRuntimeException) h.call.errors.getFirst();
            assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
            assertThat(GrpcErrors.reasonOf(error)).isEqualTo(DeviceStream.REASON_SUPERSEDED);
        }

        @Test
        void aWriteTheClientNoLongerAcceptsClosesTheStreamWithoutBreakingTheOfferingThread() {
            Harness h = Harness.standard().live();
            h.call.duringOnNext = command -> {
                throw new IllegalStateException("call already closed");
            };

            h.stream.offer(command(1));
            h.stream.offer(command(2));
            h.stream.fail(GrpcErrors.of(Status.Code.UNAVAILABLE, "X", "x"));

            assertThat(h.stream.isClosed()).isTrue();
            assertThat(h.call.sent).as("tras el primer fallo no se escribe nada mas").hasSize(1);
            assertThat(h.call.errors).as("ni un terminal: la llamada ya no estaba").isEmpty();
            assertThat(h.closed).hasSize(1);
        }
    }

    @Nested
    class CatchUp {

        @Test
        void welcomeGoesFirstThenTheReplayPagedThenWhatArrivedMeanwhileOnceAndInOrder() {
            Harness h = new Harness(16, 16, 2, Duration.ofSeconds(5));
            List<Long> replayRequests = new ArrayList<>();
            ReplaySource replay = (after, limit) -> {
                replayRequests.add(after);
                return List.of(command(4), command(5), command(6), command(7)).stream()
                        .filter(command -> command.getSequence() > after).limit(limit).toList();
            };
            CatchUpProbe probe = new CatchUpProbe() {
                @Override
                public void afterRegistered(DeviceStream stream) {
                    // Confirmadas despues de registrarse y antes de leer la base: estaran tambien en la reproduccion.
                    stream.offer(command(5));
                    stream.offer(command(6));
                }

                @Override
                public void beforeFlush(DeviceStream stream) {
                    // Confirmada despues de la reproduccion: solo esta retenida.
                    stream.offer(command(8));
                    assertThat(stream.mode()).isEqualTo(DeviceStream.Mode.CATCHING_UP);
                }
            };

            h.stream.catchUp(3, welcome(), replay, probe);

            assertThat(replayRequests).as("paginado de dos en dos desde la ultima aplicada").containsExactly(3L, 5L, 7L);
            assertThat(sequences(h.call.sent)).containsExactly(0L, 4L, 5L, 6L, 7L, 8L);
            assertThat(h.call.sent.getFirst().hasWelcome()).isTrue();
            assertThat(h.stream.mode()).isEqualTo(DeviceStream.Mode.LIVE);
            h.stream.offer(command(8));
            h.stream.offer(command(9));
            assertThat(sequences(h.call.sent)).containsExactly(0L, 4L, 5L, 6L, 7L, 8L, 9L);
        }

        @Test
        void whatTheDeviceDeclaredAppliedCountsAsSentOnTheNewStream() {
            Harness h = Harness.standard();

            h.stream.catchUp(8, welcome(), (after, limit) -> List.of(), CatchUpProbe.NONE);

            assertThat(h.stream.lastSentSequence()).as("el tablero no pone en cola lo que el equipo ya tiene").isEqualTo(8L);
            h.stream.offer(command(9));
            assertThat(h.stream.lastSentSequence()).isEqualTo(9L);
        }

        @Test
        void aDeviceThatDoesNotReadDuringCatchUpIsClosedWithResourceExhausted() {
            Harness h = new Harness(2, 8, 10, Duration.ofMillis(300));
            h.call.ready = false;

            h.stream.catchUp(0, welcome(), (after, limit) -> List.of(command(1), command(2), command(3)), CatchUpProbe.NONE);

            assertThat(h.call.errors).hasSize(1);
            StatusRuntimeException error = (StatusRuntimeException) h.call.errors.getFirst();
            assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
            assertThat(GrpcErrors.reasonOf(error)).isEqualTo(DeviceStream.REASON_NOT_READING);
            assertThat(h.stream.isClosed()).isTrue();
        }

        @Test
        void tooManyCommandsHeldDuringCatchUpCloseTheStream() {
            Harness h = new Harness(16, 2, 10, Duration.ofSeconds(5));
            CatchUpProbe probe = new CatchUpProbe() {
                @Override
                public void afterRegistered(DeviceStream stream) {
                    stream.offer(command(1));
                    stream.offer(command(2));
                    stream.offer(command(3));
                }
            };

            h.stream.catchUp(0, welcome(), (after, limit) -> List.of(), probe);

            assertThat(h.call.errors).hasSize(1);
            assertThat(((StatusRuntimeException) h.call.errors.getFirst()).getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
            assertThat(h.stream.isClosed()).isTrue();
        }

        @Test
        void aStreamSupersededDuringCatchUpStopsQuietly() {
            Harness h = Harness.standard();
            CatchUpProbe probe = new CatchUpProbe() {
                @Override
                public void afterRegistered(DeviceStream stream) {
                    stream.supersede();
                }
            };

            h.stream.catchUp(0, welcome(), (after, limit) -> List.of(command(1)), probe);

            assertThat(h.call.errors).hasSize(1);
            assertThat(((StatusRuntimeException) h.call.errors.getFirst()).getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
            assertThat(h.closed).hasSize(1);
        }
    }

    @Nested
    class Registry {

        private final FieldCommandService commands = mock(FieldCommandService.class);
        private final DeviceStreamRegistry registry = new DeviceStreamRegistry(commands, new FieldMetrics(new SimpleMeterRegistry()));

        private DeviceStream stream(String deviceId, UUID possessionId) {
            return new DeviceStream(deviceId, SHIFT, possessionId, "T-A", PRINCIPAL, null, new FakeCall(), 8, 8, 2, Duration.ofSeconds(1),
                    new FieldMetrics(new SimpleMeterRegistry()), registry::unregister);
        }

        @Test
        void theLaneIsCreatedAtRegistrationWithTheMaximumOfTheDatabase() {
            when(commands.maxSequence(POSSESSION)).thenReturn(7L);
            DeviceStream first = stream("dev-1", POSSESSION);

            assertThat(registry.lane(POSSESSION)).isNull();
            assertThat(registry.register(first)).isNull();

            assertThat(registry.lane(POSSESSION).lastDispatched()).isEqualTo(7L);
            assertThat(registry.lane(POSSESSION).streams()).containsExactly(first);
            registry.register(stream("dev-2", POSSESSION));
            verify(commands).maxSequence(POSSESSION);
            assertThat(registry.openStreams()).isEqualTo(2);
            assertThat(registry.ofPossession(POSSESSION)).hasSize(2);
        }

        @Test
        void aSecondStreamOfTheSameDeviceSupersedesTheFirstAndTheFirstNeverRemovesTheSecond() {
            DeviceStream first = stream("dev-1", POSSESSION);
            DeviceStream second = stream("dev-1", POSSESSION);
            registry.register(first);

            DeviceStream previous = registry.register(second);

            assertThat(previous).isSameAs(first);
            previous.supersede();
            assertThat(registry.ofDevice("dev-1")).isSameAs(second);
            assertThat(registry.lane(POSSESSION).streams()).containsExactly(second);
            assertThat(registry.openStreams()).isEqualTo(1);
        }

        @Test
        void closingThePossessionCompletesEveryStreamAndDropsTheLane() {
            DeviceStream a = stream("dev-1", POSSESSION);
            DeviceStream b = stream("dev-2", POSSESSION);
            registry.register(a);
            registry.register(b);

            registry.closeAll(POSSESSION);

            assertThat(a.isClosed()).isTrue();
            assertThat(b.isClosed()).isTrue();
            assertThat(registry.lane(POSSESSION)).isNull();
            assertThat(registry.openStreams()).isZero();
            registry.closeAll(POSSESSION);
        }
    }

    @Nested
    class Dispatcher {

        private final FieldCommandService commands = mock(FieldCommandService.class);
        private final DeviceStreamRegistry registry = new DeviceStreamRegistry(commands, new FieldMetrics(new SimpleMeterRegistry()));
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        private final PossessionBoardService board = mock(PossessionBoardService.class);
        private final CommandDispatcher dispatcher = new CommandDispatcher(registry, commands, board, executor);

        @Test
        void aBroadcastReachesEveryStreamAndATargetedOneOnlyItsShift() {
            UUID otherShift = UUID.randomUUID();
            FakeCall a = registered("dev-a", SHIFT);
            FakeCall b = registered("dev-b", otherShift);

            dispatcher.dispatch(new CommandCommitted(POSSESSION, null, command(1)));
            dispatcher.dispatch(new CommandCommitted(POSSESSION, otherShift, command(2)));

            assertThat(sequences(a.sent)).containsExactly(1L);
            assertThat(sequences(b.sent)).containsExactly(1L, 2L);
            assertThat(registry.lane(POSSESSION).lastDispatched()).isEqualTo(2L);
        }

        @Test
        void aGapIsFilledFromTheDatabaseAndAResendIsIgnored() {
            FakeCall a = registered("dev-a", SHIFT);
            when(commands.range(POSSESSION, 1L, 2L)).thenReturn(List.of(
                    new StoredCommand(UUID.randomUUID(), 1, null, false, command(1)),
                    new StoredCommand(UUID.randomUUID(), 2, null, false, command(2))));

            dispatcher.dispatch(new CommandCommitted(POSSESSION, null, command(3)));
            dispatcher.dispatch(new CommandCommitted(POSSESSION, null, command(2)));

            assertThat(sequences(a.sent)).containsExactly(1L, 2L, 3L);
            verify(commands).range(POSSESSION, 1L, 2L);
        }

        @Test
        void withNobodyConnectedNothingIsReadAndNothingBreaks() {
            UUID nobody = UUID.randomUUID();
            dispatcher.dispatch(new CommandCommitted(nobody, null, command(1)));

            verify(commands, never()).range(any(), anyLong(), anyLong());
            verify(board).markDirty(nobody);
        }

        @Test
        void twoEmittersOutOfOrderStillReachTheStreamInOrder() throws Exception {
            FakeCall a = registered("dev-a", SHIFT);
            when(commands.range(eq(POSSESSION), anyLong(), anyLong())).thenAnswer(invocation -> {
                long from = invocation.getArgument(1);
                long to = invocation.getArgument(2);
                List<StoredCommand> range = new ArrayList<>();
                for (long sequence = from; sequence <= to; sequence++) {
                    range.add(new StoredCommand(UUID.randomUUID(), sequence, null, false, command(sequence)));
                }
                return range;
            });
            CountDownLatch go = new CountDownLatch(1);
            List<Thread> emitters = new ArrayList<>();
            for (long sequence = 40; sequence >= 1; sequence--) {
                long mine = sequence;
                emitters.add(Thread.ofVirtual().start(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException ignored) {
                        return;
                    }
                    dispatcher.dispatch(new CommandCommitted(POSSESSION, null, command(mine)));
                }));
            }
            go.countDown();
            for (Thread emitter : emitters) {
                emitter.join(TimeUnit.SECONDS.toMillis(10));
            }

            List<Long> expected = new ArrayList<>();
            for (long sequence = 1; sequence <= 40; sequence++) {
                expected.add(sequence);
            }
            assertThat(sequences(a.sent)).containsExactlyElementsOf(expected);
        }

        private FakeCall registered(String deviceId, UUID shiftId) {
            FakeCall call = new FakeCall();
            DeviceStream stream = new DeviceStream(deviceId, shiftId, POSSESSION, "T", PRINCIPAL, null, call, 64, 64, 10, Duration.ofSeconds(5),
                    new FieldMetrics(new SimpleMeterRegistry()), registry::unregister);
            registry.register(stream);
            stream.catchUp(0, welcome(), (after, limit) -> List.of(), CatchUpProbe.NONE);
            call.sent.clear();
            return call;
        }
    }

    @Nested
    class WorkQueues {

        private final FieldEventSynchronizer synchronizer = mock(FieldEventSynchronizer.class);
        private final FieldEventService events = mock(FieldEventService.class);

        private SyncJob job(String deviceId, long sequence) {
            return SyncJob.first(UUID.randomUUID(), new EventContext(POSSESSION, SHIFT, deviceId, PRINCIPAL,
                    TeamMessage.newBuilder().setDeviceId(deviceId).setSequence(sequence).build()));
        }

        @Test
        void jobsOfADeviceRunInOrderOnTheirOwnThreadAndAFullQueueIsRefused() throws Exception {
            CountDownLatch release = new CountDownLatch(1);
            List<Long> processed = new CopyOnWriteArrayList<>();
            org.mockito.Mockito.doAnswer(invocation -> {
                release.await(5, TimeUnit.SECONDS);
                processed.add(((SyncJob) invocation.getArgument(0)).context().sequence());
                return null;
            }).when(synchronizer).process(any());
            DeviceWorkQueues queues = new DeviceWorkQueues(synchronizer, events, 2, Duration.ofMinutes(1), Duration.ofMinutes(1), CLOCK,
                    new FieldMetrics(new SimpleMeterRegistry()));

            assertThat(queues.submit("dev-1", job("dev-1", 1))).isTrue();
            await().atMost(2, TimeUnit.SECONDS).until(() -> queues.pending("dev-1") == 0);
            assertThat(queues.submit("dev-1", job("dev-1", 2))).isTrue();
            assertThat(queues.submit("dev-1", job("dev-1", 3))).isTrue();
            assertThat(queues.submit("dev-1", job("dev-1", 4))).as("la cola esta llena: quien llama cierra el stream").isFalse();
            assertThat(queues.submit("dev-2", job("dev-2", 9))).as("otra cola, otro hilo").isTrue();

            release.countDown();

            await().atMost(5, TimeUnit.SECONDS).until(() -> processed.size() == 4);
            assertThat(processed.stream().filter(sequence -> sequence != 9L).toList()).containsExactly(1L, 2L, 3L);
            assertThat(processed).contains(9L);
            queues.shutdown();
        }

        @Test
        void aFailureIsRecordedOnTheEventAndTheNextJobStillRuns() throws Exception {
            SyncJob failing = job("dev-1", 1);
            SyncJob next = job("dev-1", 2);
            doThrow(new IllegalStateException("maintenance down")).when(synchronizer).process(failing);
            DeviceWorkQueues queues = new DeviceWorkQueues(synchronizer, events, 8, Duration.ofMinutes(1), Duration.ofMinutes(1), CLOCK,
                    new FieldMetrics(new SimpleMeterRegistry()));

            queues.submit("dev-1", failing);
            queues.submit("dev-1", next);

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(synchronizer).process(next));
            verify(events).markFailed(failing.eventId(), "java.lang.IllegalStateException: maintenance down", NOW.plus(Duration.ofMinutes(1)));
            queues.shutdown();
        }

        @Test
        void anIdleWorkerRetiresAndTheNextJobStartsANewOne() throws Exception {
            DeviceWorkQueues queues = new DeviceWorkQueues(synchronizer, events, 8, Duration.ofMillis(50), Duration.ofMinutes(1), CLOCK,
                    new FieldMetrics(new SimpleMeterRegistry()));

            queues.submit("dev-1", job("dev-1", 1));
            await().atMost(5, TimeUnit.SECONDS).until(() -> queues.activeWorkers() == 0);

            assertThat(queues.submit("dev-1", job("dev-1", 2))).isTrue();
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(synchronizer, org.mockito.Mockito.times(2)).process(any()));
            queues.shutdown();
        }
    }
}
