package com.alejandro.mtofield.domain.model;

import com.alejandro.mtofield.domain.model.TeamLiveness.Liveness;
import com.alejandro.mtofield.domain.model.TeamLiveness.Thresholds;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Las reglas sin framework: la posesion, la vida de los equipos y la agregacion de acuses. */
class DomainModelTest {

    private static final LocalDate NIGHT = LocalDate.of(2026, 10, 9);

    @Nested
    @DisplayName("Reglas de la posesion")
    class Possession {

        @Test
        void theShiftsMustBeAtLeastOneDistinctOnTheSameDateAndStillWorkable() {
            assertThatThrownBy(() -> PossessionRules.validateShifts(List.of())).isInstanceOf(IllegalArgumentException.class);

            ShiftSnapshot shift = shift(UUID.randomUUID(), NIGHT, "PLANNED", "2026-10-10T05:00:00Z");
            assertThatThrownBy(() -> PossessionRules.validateShifts(List.of(shift, shift)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("repeated");

            ShiftSnapshot otherNight = shift(UUID.randomUUID(), NIGHT.plusDays(1), "PLANNED", "2026-10-11T05:00:00Z");
            assertThatThrownBy(() -> PossessionRules.validateShifts(List.of(shift, otherNight)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("same date");

            ShiftSnapshot closed = shift(UUID.randomUUID(), NIGHT, "CLOSED", "2026-10-10T05:00:00Z");
            assertThatThrownBy(() -> PossessionRules.validateShifts(List.of(shift, closed)))
                    .isInstanceOf(ShiftNotWorkableException.class);
            ShiftSnapshot cancelled = shift(UUID.randomUUID(), NIGHT, "CANCELLED", "2026-10-10T05:00:00Z");
            assertThatThrownBy(() -> PossessionRules.validateShifts(List.of(cancelled)))
                    .isInstanceOf(ShiftNotWorkableException.class);

            ShiftSnapshot inProgress = shift(UUID.randomUUID(), NIGHT, "IN_PROGRESS", "2026-10-10T04:00:00Z");
            assertThat(PossessionRules.validateShifts(List.of(shift, inProgress))).isEqualTo(NIGHT);
        }

        @Test
        void theDefaultEndIsTheEarliestPlannedEnd() {
            ShiftSnapshot early = shift(UUID.randomUUID(), NIGHT, "PLANNED", "2026-10-10T04:00:00Z");
            ShiftSnapshot late = shift(UUID.randomUUID(), NIGHT, "PLANNED", "2026-10-10T05:30:00Z");
            ShiftSnapshot none = shift(UUID.randomUUID(), NIGHT, "PLANNED", null);

            assertThat(PossessionRules.defaultEndsAt(List.of(late, early, none))).isEqualTo(Instant.parse("2026-10-10T04:00:00Z"));
            assertThatThrownBy(() -> PossessionRules.defaultEndsAt(List.of(none))).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void closingNeedsEveryoneClearUnlessForcedAndForcingNeedsAReason() {
            assertThat(PossessionStateMachine.canClose(PossessionStatus.OPEN)).isTrue();
            assertThat(PossessionStateMachine.canClose(PossessionStatus.CLOSED)).isFalse();
            assertThat(PossessionStateMachine.acceptsTraffic(PossessionStatus.CLOSED)).isFalse();

            assertThat(PossessionStateMachine.closeAllowed(true, false)).isTrue();
            assertThat(PossessionStateMachine.closeAllowed(false, false)).isFalse();
            assertThat(PossessionStateMachine.closeAllowed(false, true)).isTrue();

            assertThatThrownBy(() -> PossessionRules.validateCloseRequest(true, " ")).isInstanceOf(IllegalArgumentException.class);
            PossessionRules.validateCloseRequest(true, "train approaching");
            PossessionRules.validateCloseRequest(false, null);
        }
    }

    @Nested
    @DisplayName("Vida de los equipos")
    class Liveness_ {

        private final Thresholds thresholds = Thresholds.standard();

        @Test
        void aDeviceIsConnectedStaleOrDisconnectedByItsSilenceAndItsStream() {
            assertThat(TeamLiveness.classify(true, Duration.ofSeconds(9), thresholds)).isEqualTo(Liveness.CONNECTED);
            assertThat(TeamLiveness.classify(true, Duration.ofSeconds(29), thresholds)).isEqualTo(Liveness.CONNECTED);
            assertThat(TeamLiveness.classify(true, Duration.ofSeconds(30), thresholds)).isEqualTo(Liveness.STALE);
            assertThat(TeamLiveness.classify(true, Duration.ofSeconds(59), thresholds)).isEqualTo(Liveness.STALE);
            assertThat(TeamLiveness.classify(true, Duration.ofSeconds(60), thresholds)).isEqualTo(Liveness.DISCONNECTED);
            assertThat(TeamLiveness.classify(false, Duration.ofSeconds(1), thresholds)).isEqualTo(Liveness.DISCONNECTED);
            assertThat(TeamLiveness.classify(true, null, thresholds)).isEqualTo(Liveness.DISCONNECTED);
        }

        @Test
        void aTeamIsAsAliveAsItsBestDevice() {
            assertThat(TeamLiveness.best(List.of())).isEqualTo(Liveness.DISCONNECTED);
            assertThat(TeamLiveness.best(List.of(Liveness.DISCONNECTED, Liveness.STALE))).isEqualTo(Liveness.STALE);
            assertThat(TeamLiveness.best(List.of(Liveness.STALE, Liveness.CONNECTED, Liveness.DISCONNECTED))).isEqualTo(Liveness.CONNECTED);
        }

        @Test
        void theThresholdsMustBeOrdered() {
            assertThatThrownBy(() -> new Thresholds(Duration.ofSeconds(60), Duration.ofSeconds(30)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("Acuses")
    class Acks {

        @Test
        void theSummaryTellsAckedPendingSentAndQueuedTeamsInBoardOrder() {
            CommandAckSummary summary = CommandAckSummary.of(List.of("T-1", "T-2", "T-3"), Set.of("T-2"), Set.of("T-1", "T-2"));

            assertThat(summary.ackedBy()).containsExactly("T-2");
            assertThat(summary.pending()).containsExactly("T-1", "T-3");
            assertThat(summary.sentTo()).containsExactly("T-1");
            assertThat(summary.queuedFor()).containsExactly("T-3");
            assertThat(summary.allAcked()).isFalse();
            assertThat(CommandAckSummary.of(List.of("T-1"), Set.of("T-1"), Set.of()).allAcked()).isTrue();
        }
    }

    private static ShiftSnapshot shift(UUID id, LocalDate date, String status, String plannedEnd) {
        return new ShiftSnapshot(id, "SH-000001", date, status, "T-" + id.toString().substring(0, 4), "Equipo",
                null, plannedEnd == null ? null : Instant.parse(plannedEnd), List.of(1L));
    }
}
