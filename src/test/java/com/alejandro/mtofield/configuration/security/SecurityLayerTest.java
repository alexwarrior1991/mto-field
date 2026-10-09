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

    @Test
    void currentUserServiceReadsTheGroupsOfTheTokenWithoutTheirPath() {
        Jwt jwt = jwt(Map.of("groups", List.of("/EQ-NORTE", "/equipos/EQ-SUR", " ", "EQ-ESTE", "/EQ-NORTE")));
        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(
                jwt, List.of(new SimpleGrantedAuthority("ROLE_FIELD_TEAM")), "campo.tecnico"));

        CurrentUserService service = new CurrentUserService();
        assertEquals(List.of("EQ-NORTE", "EQ-SUR", "EQ-ESTE"), service.getGroups("groups"));
        assertEquals(List.of(), service.getGroups("teams"));
        SecurityContextHolder.clearContext();
        assertEquals(List.of(), service.getGroups("groups"));
    }

    /**
     * El JWT viaja en la cabecera {@code authorization} de cada llamada y el servidor admite 8 KiB de
     * cabeceras (spring.grpc.server.inbound.metadata.max-size). Se mide el peor caso del dominio
     * con la forma exacta de Keycloak: una persona con todos los roles de cliente de las siete API,
     * todos los perfiles del realm, los claims estandar y sus grupos. Tiene que quedar por debajo de
     * la mitad del limite, para que el realm pueda crecer sin tocar la configuracion.
     */
    @Test
    void aTokenWithEveryRoleAndProfileOfTheDomainStaysUnderHalfTheMetadataLimit() {
        Map<String, List<String>> clientRoles = new java.util.LinkedHashMap<>();
        clientRoles.put("mto-configuration-api", List.of("config-audit", "config-delete", "config-import", "config-read", "config-write", "lov-manage", "ops-metrics", "ops-write"));
        clientRoles.put("mto-stock-api", List.of("ops-metrics", "ops-write", "stock-adjust", "stock-delete", "stock-read", "stock-write"));
        clientRoles.put("mto-maintenance-api", List.of("maintenance-delete", "maintenance-read", "maintenance-supervise", "maintenance-write", "ops-metrics", "ops-write"));
        clientRoles.put("mto-users-api", List.of("ops-metrics", "ops-write", "users-credentials-write", "users-delete", "users-password-reset", "users-profiles-write", "users-read", "users-roles-write", "users-sessions-write", "users-write"));
        clientRoles.put("mto-notification-api", List.of("notification-access-read", "notification-activity-read", "notification-admin", "notification-inbox", "ops-metrics", "ops-write"));
        clientRoles.put("mto-gateway-api", List.of("ops-metrics", "ops-write"));
        clientRoles.put("mto-field-api", List.of("field-supervise", "field-team", "ops-metrics", "ops-write"));
        clientRoles.put("account", List.of("manage-account", "manage-account-links", "view-profile"));
        List<String> realmRoles = List.of("mto-admin", "mto-auditor", "mto-editor", "mto-field-supervisor", "mto-field-technician",
                "mto-maintenance-manager", "mto-maintenance-technician", "mto-maintenance-viewer", "mto-notification-admin",
                "mto-notification-auditor", "mto-notification-viewer", "mto-users-admin", "mto-users-manager", "mto-users-viewer", "mto-viewer",
                "mto-warehouse-admin", "mto-warehouse-operator", "mto-warehouse-viewer", "default-roles-mto", "offline_access", "uma_authorization");
        Map<String, Object> resourceAccess = new java.util.LinkedHashMap<>();
        clientRoles.forEach((client, roles) -> resourceAccess.put(client, Map.of(JwtClaimNames.ROLES, roles)));
        Instant now = Instant.now();
        com.nimbusds.jwt.JWTClaimsSet claims = new com.nimbusds.jwt.JWTClaimsSet.Builder()
                .expirationTime(java.util.Date.from(now.plusSeconds(300)))
                .issueTime(java.util.Date.from(now))
                .jwtID(java.util.UUID.randomUUID().toString())
                .issuer("http://auth.mto.local:8082/realms/mto")
                .audience(List.of("mto-configuration-api", "mto-stock-api", "mto-maintenance-api", "mto-users-api", "mto-notification-api",
                        "mto-gateway-api", "mto-field-api", "account"))
                .subject(java.util.UUID.randomUUID().toString())
                .claim("typ", "Bearer")
                .claim("azp", "mto-frontend")
                .claim("sid", java.util.UUID.randomUUID().toString())
                .claim("acr", "1")
                .claim("allowed-origins", List.of("http://localhost:4200", "http://localhost:8085"))
                .claim(JwtClaimNames.REALM_ACCESS, Map.of(JwtClaimNames.ROLES, realmRoles))
                .claim(JwtClaimNames.RESOURCE_ACCESS, resourceAccess)
                .claim(JwtClaimNames.SCOPE, "openid profile email")
                .claim("email_verified", true)
                .claim("name", "Responsable De Todo El Dominio")
                .claim("groups", List.of("EQ-NORTE", "EQ-SUR", "EQ-ESTE"))
                .claim(JwtClaimNames.PREFERRED_USERNAME, "responsable.de.todo")
                .claim("given_name", "Responsable")
                .claim("family_name", "De Todo El Dominio")
                .claim(JwtClaimNames.EMAIL, "responsable.de.todo@mto.local")
                .build();
        String token = com.alejandro.mtofield.support.TestTokens.signClaims(claims);

        // Lo que cuenta HTTP/2 para el limite: nombre + valor + 32 por cabecera. Las fijas de una
        // llamada gRPC (:method, :scheme, :path, :authority, te, content-type, user-agent,
        // grpc-accept-encoding, grpc-timeout) caben de sobra en 512 bytes.
        int authorization = "authorization".length() + ("Bearer " + token).length() + 32;
        int fixedHeaders = 512;
        int metadataLimit = 8 * 1024;
        int size = authorization + fixedHeaders;
        System.out.printf("Worst-case realm token: %d bytes of JWT, %d bytes of metadata, limit %d%n", token.length(), size, metadataLimit);
        assertTrue(size < metadataLimit / 2, "the worst-case token takes " + size + " of " + metadataLimit + " bytes of metadata");
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
