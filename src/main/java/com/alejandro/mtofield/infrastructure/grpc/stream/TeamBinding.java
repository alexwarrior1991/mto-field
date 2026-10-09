package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.configuration.grpc.FieldProperties;
import com.alejandro.mtofield.configuration.security.CurrentUserService;
import com.alejandro.mtofield.configuration.security.SecurityRoles;
import com.alejandro.mtofield.infrastructure.grpc.GrpcErrors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Liga a la persona con su equipo: el claim de grupos de Keycloak ({@code app.field.team-binding.claim},
 * {@code groups}) trae los equipos de la persona, y al unirse a un turno el codigo de su equipo
 * (el {@code code} del equipo en mto-maintenance) tiene que estar entre ellos. Quien lleva
 * {@code field-supervise} actua por cualquier equipo. Con {@code app.field.team-binding.enabled=false}
 * basta con el rol, como en la fase 1.
 *
 * <p>Los grupos llegan como ruta ({@code /EQ-NORTE}, o {@code /equipos/EQ-NORTE} con el mapper en
 * ruta completa): cuenta el ultimo segmento. Se capturan al abrir la llamada, en el unico hilo que
 * tiene el {@code SecurityContext}, igual que el principal y la caducidad del token.</p>
 */
@Component
public class TeamBinding {

    public static final String REASON_TEAM_NOT_ALLOWED = "TEAM_NOT_ALLOWED";
    public static final String TEAM_CODE = "team_code";

    /** Lo que el token dice de la persona: sus equipos y si es responsable. */
    public record Membership(List<String> teams, boolean supervisor) {

        public static final Membership ANY = new Membership(List.of(), true);
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
        return new Membership(currentUser.getGroups(properties.teamBinding().claim()), currentUser.hasRole(SecurityRoles.FIELD_SUPERVISE));
    }

    public boolean allows(Membership membership, String teamCode) {
        if (!isEnabled() || membership.supervisor()) {
            return true;
        }
        return teamCode != null && membership.teams().stream().anyMatch(team -> team.equalsIgnoreCase(teamCode.trim()));
    }

    public StatusRuntimeException refusal(Membership membership, String teamCode) {
        return GrpcErrors.of(Status.Code.PERMISSION_DENIED, REASON_TEAM_NOT_ALLOWED,
                "the token is not of team " + teamCode + " (its groups: " + membership.teams() + "); a device joins only the shift of its team",
                Map.of(TEAM_CODE, teamCode == null ? "" : teamCode));
    }
}
