package com.alejandro.mtofield.configuration.security;

import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Traduce un token de Keycloak a las autoridades que comprueban la cadena de filtros y las
 * anotaciones de seguridad de método.
 *
 * <p>Lo publica {@link SecurityConfiguration} como {@code @Bean} en lugar de anotarse como
 * {@code @Component}: al implementar {@link Converter}, un componente lo recogería también cada
 * slice de {@code @WebMvcTest} —que incluye los convertidores en su filtro de escaneo—, y cualquier
 * test de contrato de un controlador fallaría al arrancar por faltarle {@link SecurityProperties},
 * que solo registra la configuración de seguridad.</p>
 */
public class KeycloakJwtAuthenticationConverter implements Converter<Jwt, AbstractAuthenticationToken> {

    private final SecurityProperties securityProperties;

    public KeycloakJwtAuthenticationConverter(SecurityProperties securityProperties) {
        this.securityProperties = securityProperties;
    }

    @Override
    public AbstractAuthenticationToken convert(Jwt jwt) {
        Set<GrantedAuthority> authorities = new HashSet<>();

        authorities.addAll(extractScopes(jwt));
        authorities.addAll(extractRealmRoles(jwt));
        authorities.addAll(extractClientRoles(jwt, securityProperties.clientId()));

        return new JwtAuthenticationToken(jwt, authorities, resolvePrincipalName(jwt));
    }

    private Collection<GrantedAuthority> extractScopes(Jwt jwt) {
        String scope = jwt.getClaimAsString(JwtClaimNames.SCOPE);
        if (scope == null || scope.isBlank()) {
            return Set.of();
        }

        return Stream.of(scope.split(" "))
                .filter(value -> !value.isBlank())
                .map(value -> SecurityAuthorityPrefixes.SCOPE_PREFIX + value)
                .map(SimpleGrantedAuthority::new)
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Los roles de realm se emiten <b>solo</b> con el prefijo {@code ROLE_REALM_}, nunca con
     * {@code ROLE_} a secas.
     *
     * <p>Emitir ambos haría que un rol de realm y uno de cliente que se llamaran igual acabaran en
     * la misma autoridad: quien tuviera el de realm pasaría una comprobación pensada para el de
     * cliente. Y como quien administra el realm no es necesariamente quien escribe el código,
     * bastaría con crear allí un rol llamado igual que un permiso para concedérselo a cualquiera.</p>
     *
     * <p>La separación encaja con el modelo: los permisos que comprueba el código son roles de
     * cliente, y los roles de realm son perfiles de negocio <em>compuestos</em> que los agrupan.
     * Keycloak expande los compuestos al emitir el token, así que un usuario con el perfil de realm
     * {@code mto-field-technician} ya llega con {@code field-team} y {@code field-supervise} dentro de
     * {@code resource_access}. Comprobar un perfil sigue siendo posible, pero hay que nombrarlo:
     * {@code hasRole("REALM_MTO_MAINTENANCE_TECHNICIAN")}.</p>
     */
    private Collection<GrantedAuthority> extractRealmRoles(Jwt jwt) {
        Map<String, Object> realmAccess = jwt.getClaimAsMap(JwtClaimNames.REALM_ACCESS);
        if (realmAccess == null) {
            return Set.of();
        }

        return extractRoles(realmAccess).stream()
                .map(role -> SecurityAuthorityPrefixes.REALM_ROLE_PREFIX + normalize(role))
                .map(SimpleGrantedAuthority::new)
                .collect(Collectors.toUnmodifiableSet());
    }

    private Collection<GrantedAuthority> extractClientRoles(Jwt jwt, String clientId) {
        Map<String, Object> resourceAccess = jwt.getClaimAsMap(JwtClaimNames.RESOURCE_ACCESS);
        if (resourceAccess == null || clientId == null || clientId.isBlank()) {
            return Set.of();
        }

        if (!(resourceAccess.get(clientId) instanceof Map<?, ?> clientAccess)) {
            return Set.of();
        }

        return extractRoles(clientAccess).stream()
                .flatMap(role -> Stream.of(
                        SecurityAuthorityPrefixes.ROLE_PREFIX + normalize(role),
                        SecurityAuthorityPrefixes.CLIENT_ROLE_PREFIX + normalize(role)
                ))
                .map(SimpleGrantedAuthority::new)
                .collect(Collectors.toUnmodifiableSet());
    }

    private Set<String> extractRoles(Map<?, ?> accessMap) {
        if (!(accessMap.get(JwtClaimNames.ROLES) instanceof Collection<?> roleCollection)) {
            return Set.of();
        }

        return roleCollection.stream()
                .filter(Objects::nonNull)
                .map(Object::toString)
                .filter(value -> !value.isBlank())
                .collect(Collectors.toUnmodifiableSet());
    }

    private String resolvePrincipalName(Jwt jwt) {
        String claimValue = jwt.getClaimAsString(securityProperties.principalClaim());
        if (claimValue != null && !claimValue.isBlank()) {
            return claimValue;
        }

        String preferredUsername = jwt.getClaimAsString(JwtClaimNames.PREFERRED_USERNAME);
        if (preferredUsername != null && !preferredUsername.isBlank()) {
            return preferredUsername;
        }

        return jwt.getSubject();
    }

    /**
     * Permite nombrar los roles en Keycloak como es costumbre allí —minúsculas y guiones— y
     * comprobarlos aquí como es costumbre en Spring: {@code field-team} se convierte en
     * {@code FIELD_TEAM}, listo para {@code hasRole("FIELD_TEAM")}.
     *
     * <p>La conversión no es inyectiva: {@code field-team}, {@code stock_read} y {@code Stock Read}
     * producen la misma autoridad. No es un problema de seguridad —los roles de realm y los de
     * cliente viven en prefijos distintos—, pero sigue siendo motivo para no crear en el mismo
     * cliente dos roles que solo se diferencien en el separador.</p>
     */
    private String normalize(String value) {
        return value.trim()
                .replace('-', '_')
                .replace(' ', '_')
                .toUpperCase();
    }
}
