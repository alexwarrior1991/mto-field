package com.alejandro.mtofield.configuration.security;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprobaciones unitarias de las piezas de seguridad que no necesitan cadena de filtros: la
 * traducción de claims a autoridades, la validación de audiencia, la lectura del usuario actual y
 * las invariantes de configuración. Los permisos por RPC se prueban en
 * {@code GrpcServiceLayerTest}.
 */
class SecurityLayerTest {

    private static final String CLIENT_ID = "mto-field-api";

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @AfterEach
    void clearSecurityContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void clientRolesBecomeRolePrefixedAuthoritiesNormalizedToUpperCase() {
        AbstractAuthenticationToken authentication = convert(jwt(Map.of(
                JwtClaimNames.RESOURCE_ACCESS, Map.of(CLIENT_ID, Map.of(JwtClaimNames.ROLES, List.of("field-team", "field supervise")))
        )));

        assertTrue(authorities(authentication).containsAll(Set.of(
                "ROLE_FIELD_TEAM", "ROLE_CLIENT_FIELD_TEAM",
                "ROLE_FIELD_SUPERVISE", "ROLE_CLIENT_FIELD_SUPERVISE"
        )));
    }

    /**
     * Si los roles de realm se emitieran también como {@code ROLE_}, crear en Keycloak un rol de
     * realm llamado igual que un permiso bastaría para concederlo: quien administra el realm no es
     * necesariamente quien escribe el código.
     */
    @Test
    void realmRolesNeverProduceThePlainRolePrefixReservedForClientRoles() {
        AbstractAuthenticationToken authentication = convert(jwt(Map.of(
                JwtClaimNames.REALM_ACCESS, Map.of(JwtClaimNames.ROLES, List.of("field-team"))
        )));

        Set<String> authorities = authorities(authentication);
        assertTrue(authorities.contains("ROLE_REALM_FIELD_TEAM"));
        assertFalse(authorities.contains("ROLE_FIELD_TEAM"));
    }

    @Test
    void clientRolesOfOtherClientsAreIgnored() {
        AbstractAuthenticationToken authentication = convert(jwt(Map.of(
                JwtClaimNames.RESOURCE_ACCESS, Map.of("mto-configuration-api", Map.of(JwtClaimNames.ROLES, List.of("field-supervise")))
        )));

        assertTrue(authorities(authentication).isEmpty());
    }

    @Test
    void scopesBecomeScopePrefixedAuthorities() {
        AbstractAuthenticationToken authentication = convert(jwt(Map.of(JwtClaimNames.SCOPE, "openid profile")));

        assertTrue(authorities(authentication).containsAll(Set.of("SCOPE_openid", "SCOPE_profile")));
    }

    @Test
    void principalNameFallsBackToSubjectWhenTheConfiguredClaimIsMissing() {
        assertEquals("campo.tecnico", convert(jwt(Map.of(JwtClaimNames.PREFERRED_USERNAME, "campo.tecnico"))).getName());
        assertEquals("subject-1", convert(jwt(Map.of())).getName());
    }

    @Test
    void audienceValidatorRejectsTokensIssuedForAnotherApplication() {
        JwtAudienceValidator audienceValidator = new JwtAudienceValidator(CLIENT_ID);

        assertFalse(audienceValidator.validate(jwt(Map.of(JwtClaimNames.AUDIENCE, List.of(CLIENT_ID)))).hasErrors());

        OAuth2TokenValidatorResult rejected = audienceValidator.validate(jwt(Map.of(JwtClaimNames.AUDIENCE, List.of("mto-configuration-api"))));
        assertTrue(rejected.hasErrors());
    }

    @Test
    void audienceValidatorRefusesToBeBuiltWithoutAnAudience() {
        assertThrows(IllegalArgumentException.class, () -> new JwtAudienceValidator(" "));
    }

    @Test
    void currentUserServiceReadsTheAuthenticatedUserAndIgnoresAnonymousAuthentication() {
        CurrentUserService currentUserService = new CurrentUserService();

        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(
                jwt(Map.of(JwtClaimNames.PREFERRED_USERNAME, "campo.tecnico", JwtClaimNames.EMAIL, "operator@example.com")),
                List.of(new SimpleGrantedAuthority("ROLE_FIELD_TEAM")),
                "campo.tecnico"
        ));

        assertEquals("campo.tecnico", currentUserService.getUsername().orElseThrow());
        assertEquals("subject-1", currentUserService.getUserId().orElseThrow());
        assertEquals("operator@example.com", currentUserService.getEmail().orElseThrow());
        assertTrue(currentUserService.hasRole("FIELD_TEAM"));
        assertTrue(currentUserService.hasRole("ROLE_FIELD_TEAM"));
        assertFalse(currentUserService.hasRole("FIELD_SUPERVISE"));

        SecurityContextHolder.getContext().setAuthentication(new AnonymousAuthenticationToken(
                "key", "anonymousUser", AuthorityUtils.createAuthorityList("ROLE_ANONYMOUS")));

        assertTrue(currentUserService.getUsername().isEmpty());
        assertTrue(currentUserService.getAuthorities().isEmpty());
    }

    @Test
    void currentUserServiceReportsNoUserWhenTheContextIsEmpty() {
        assertTrue(new CurrentUserService().getUsername().isEmpty());
    }

    /**
     * Un {@code required-audience} vacío apagaría la validación de audiencia en tiempo de petición y
     * sin rastro en el log, y la API pasaría a aceptar cualquier token del realm. Que la aplicación
     * no arranque es lo que impide que eso ocurra sin que nadie lo note.
     */
    @Test
    void securityPropertiesRefuseAudienceValidationWithoutAnAudience() {
        assertTrue(validator.validate(properties(true, CLIENT_ID)).isEmpty());
        assertTrue(validator.validate(properties(true, " ")).stream()
                .anyMatch(violation -> violation.getMessage().contains("required-audience")));
        assertTrue(validator.validate(properties(false, null)).isEmpty());
    }

    private static SecurityProperties properties(boolean audienceValidationEnabled, String requiredAudience) {
        return new SecurityProperties(
                CLIENT_ID,
                JwtClaimNames.PREFERRED_USERNAME,
                audienceValidationEnabled,
                requiredAudience
        );
    }

    private static AbstractAuthenticationToken convert(Jwt jwt) {
        return new KeycloakJwtAuthenticationConverter(properties(false, null)).convert(jwt);
    }

    private static Set<String> authorities(AbstractAuthenticationToken authentication) {
        return authentication.getAuthorities().stream().map(GrantedAuthority::getAuthority).collect(Collectors.toSet());
    }

    private static Jwt jwt(Map<String, Object> claims) {
        Jwt.Builder builder = Jwt.withTokenValue("token")
                .header("alg", "RS256")
                .subject("subject-1")
                .issuedAt(Instant.EPOCH)
                .expiresAt(Instant.EPOCH.plusSeconds(300));
        claims.forEach(builder::claim);
        return builder.build();
    }
}
