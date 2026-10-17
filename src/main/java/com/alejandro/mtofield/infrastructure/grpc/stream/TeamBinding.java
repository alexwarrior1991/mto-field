package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.application.dto.ShiftMembership;
import com.alejandro.mtofield.configuration.grpc.FieldProperties;
import com.alejandro.mtofield.configuration.security.CurrentUserService;
import com.alejandro.mtofield.configuration.security.SecurityRoles;
import com.alejandro.mtofield.infrastructure.grpc.GrpcErrors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Liga a la persona con el turno al que se une. Quien lleva {@code field-supervise} actua por cualquier
 * turno. Si el turno trae la dotacion de mto-resources (V4: el usuario del jefe, del supervisor y de cada
 * lineman), decide la persona: su {@code preferred_username} tiene que estar en ella, y un suplente se une
 * sin tocar el realm. Si no la trae (un turno de un equipo local, o de un mto-maintenance anterior),
 * decide el grupo, como en la fase 3: el claim de grupos de Keycloak ({@code app.field.team-binding.claim},
 * {@code groups}) trae los equipos de la persona, y el codigo del equipo del turno tiene que estar entre
 * ellos. Con {@code app.field.team-binding.enabled=false} basta con el rol, como en la fase 1.
 *
 * <p>Los grupos llegan como ruta ({@code /EQ-NORTE}, o {@code /equipos/EQ-NORTE} con el mapper en
 * ruta completa): cuenta el ultimo segmento. Se capturan al abrir la llamada, en el unico hilo que
 * tiene el {@code SecurityContext}, igual que el principal y la caducidad del token.</p>
 */
@Component
public class TeamBinding {

    public static final String REASON_TEAM_NOT_ALLOWED = "TEAM_NOT_ALLOWED";
    public static final String TEAM_CODE = "team_code";

    /** Lo que el token dice de la persona: su usuario, sus equipos y si es responsable. */
    public record Membership(String username, List<String> teams, boolean supervisor) {

        public static final Membership ANY = new Membership(null, List.of(), true);

        /** Sin usuario: solo decide el grupo. */
        public Membership(List<String> teams, boolean supervisor) {
            this(null, teams, supervisor);
        }
    }

    private final FieldProperties properties;
    private final CurrentUserService currentUser;

    public TeamBinding(FieldProperties properties, CurrentUserService currentUser) {
        this.properties = properties;
        this.currentUser = currentUser;
    }

    public boolean isEnabled() {
        return properties.teamBinding().enabled();
    }

    /** Lee el token de la llamada en curso. */
    public Membership capture() {
        return new Membership(currentUser.getUsername().orElse(null), currentUser.getGroups(properties.teamBinding().claim()),
                currentUser.hasRole(SecurityRoles.FIELD_SUPERVISE));
    }

    /** La regla del grupo: el codigo del equipo entre los grupos del token. */
    public boolean allows(Membership membership, String teamCode) {
        if (!isEnabled() || membership.supervisor()) {
            return true;
        }
        return teamCode != null && membership.teams().stream().anyMatch(team -> team.equalsIgnoreCase(teamCode.trim()));
    }

    /** Con dotacion, la persona; sin ella, el grupo. */
    public boolean allowsJoin(Membership membership, ShiftMembership shift) {
        if (!isEnabled() || membership.supervisor()) {
            return true;
        }
        return shift.hasCrew() ? shift.isInCrew(membership.username()) : allows(membership, shift.teamCode());
    }

    /**
     * El turno con el que se une la persona: el copiado al abrir si ya la admite, y si no, el que
     * devuelva {@code reread} (mto-maintenance otra vez, una sola) si la admite; vacio si ninguno.
     */
    public Optional<ShiftMembership> admit(Membership membership, ShiftMembership shift, Supplier<Optional<ShiftMembership>> reread) {
        if (allowsJoin(membership, shift)) {
            return Optional.of(shift);
        }
        return reread.get().filter(fresh -> allowsJoin(membership, fresh));
    }

    public StatusRuntimeException refusal(Membership membership, String teamCode) {
        return GrpcErrors.of(Status.Code.PERMISSION_DENIED, REASON_TEAM_NOT_ALLOWED,
                "the token is not of team " + teamCode + " (its groups: " + membership.teams() + "); a device joins only the shift of its team",
                Map.of(TEAM_CODE, teamCode == null ? "" : teamCode));
    }

    public StatusRuntimeException refusal(Membership membership, ShiftMembership shift) {
        if (!shift.hasCrew()) {
            return refusal(membership, shift.teamCode());
        }
        String teamCode = shift.teamCode() == null ? "" : shift.teamCode();
        return GrpcErrors.of(Status.Code.PERMISSION_DENIED, REASON_TEAM_NOT_ALLOWED,
                "user " + membership.username() + " is not in the crew of shift " + shift.shiftId() + " (team " + teamCode
                        + "); a device joins only the shift of its crew", Map.of(TEAM_CODE, teamCode));
    }
}
