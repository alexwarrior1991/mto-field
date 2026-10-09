package com.alejandro.mtofield.configuration.security;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.security.oauth2.server.resource.autoconfigure.OAuth2ResourceServerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.util.StringUtils;

/**
 * La seguridad HTTP y las piezas que comparte con la gRPC. La aplicacion es un <em>resource
 * server</em>: no emite tokens ni guarda usuarios, solo valida los JWT que emite Keycloak y
 * traduce sus roles a autoridades.
 *
 * <p>Por HTTP solo se sirve Actuator (Tomcat no expone ninguna API de negocio: la API es gRPC, en
 * {@link GrpcSecurityConfiguration}). Las dos cadenas comparten el {@link JwtDecoder} y el
 * {@link KeycloakJwtAuthenticationConverter} que se definen aqui, asi que un token vale lo mismo
 * llegue por donde llegue.</p>
 *
 * <p>{@code @EnableMethodSecurity} es lo que hace que los {@code @PreAuthorize} de
 * {@code FieldGrpcService} se comprueben: en gRPC no hay verbos ni rutas, y el permiso de cada RPC
 * vive en su metodo.</p>
 */
@Configuration
@EnableWebSecurity
@EnableMethodSecurity
@EnableConfigurationProperties(SecurityProperties.class)
public class SecurityConfiguration {

    private final SecurityProperties securityProperties;

    public SecurityConfiguration(SecurityProperties securityProperties) {
        this.securityProperties = securityProperties;
    }

    @Bean
    public KeycloakJwtAuthenticationConverter jwtAuthenticationConverter() {
        return new KeycloakJwtAuthenticationConverter(securityProperties);
    }

    @Bean
    public SecurityFilterChain securityFilterChain(
            HttpSecurity http,
            KeycloakJwtAuthenticationConverter jwtAuthenticationConverter
    ) throws Exception {
        return http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(authorize -> {
                    // Las sondas de arranque y de vida las consulta el orquestador, que no tiene
                    // token. El resto del detalle de health lo gobierna
                    // 'management.endpoint.health.show-details'.
                    authorize.requestMatchers(
                            "/actuator/health",
                            "/actuator/health/**",
                            "/actuator/info"
                    ).permitAll();

                    // Todo Actuator cerrado salvo health e info. Lo que modifica va antes que la
                    // regla general y con su propio permiso: en Actuator las @WriteOperation viajan
                    // por POST y las @DeleteOperation por DELETE; el resto es lectura.
                    authorize.requestMatchers(HttpMethod.POST, "/actuator/**").hasRole(SecurityRoles.OPS_WRITE);
                    authorize.requestMatchers(HttpMethod.DELETE, "/actuator/**").hasRole(SecurityRoles.OPS_WRITE);
                    authorize.requestMatchers("/actuator/**").hasRole(SecurityRoles.OPS_METRICS);

                    authorize.anyRequest().authenticated();
                })
                .oauth2ResourceServer(oauth2 -> oauth2
                        .jwt(jwt -> jwt.jwtAuthenticationConverter(jwtAuthenticationConverter))
                )
                .build();
    }

    /**
     * Se construye con el JWK Set en lugar de con el descubrimiento por issuer: {@code
     * JwtDecoders.fromIssuerLocation()} hace una llamada HTTP bloqueante al crear el bean, de modo
     * que la aplicacion no arranca si Keycloak todavia no sirve el {@code
     * .well-known/openid-configuration}. Con el JWK Set la descarga es perezosa (la primera vez que
     * llega un token) y un reinicio simultaneo de los dos servicios deja de ser un fallo de
     * arranque.
     */
    @Bean
    public JwtDecoder jwtDecoder(OAuth2ResourceServerProperties properties) {
        OAuth2ResourceServerProperties.Jwt jwtProperties = properties.getJwt();

        NimbusJwtDecoder jwtDecoder = NimbusJwtDecoder
                .withJwkSetUri(resolveJwkSetUri(jwtProperties))
                .build();
        jwtDecoder.setJwtValidator(jwtValidator(securityProperties, jwtProperties.getIssuerUri()));

        return jwtDecoder;
    }

    /**
     * La cadena de validadores de un token, aparte y estatica para que un decoder de test (con una
     * clave propia en vez del JWK Set de Keycloak) compruebe exactamente lo mismo que el de verdad.
     *
     * <p>Se parte del validador por defecto en vez de reemplazarlo: incluye la comprobacion de
     * emisor y de vigencia, y hereda las que Spring Security anada en versiones futuras.</p>
     */
    public static OAuth2TokenValidator<Jwt> jwtValidator(SecurityProperties securityProperties, String issuerUri) {
        return new DelegatingOAuth2TokenValidator<>(
                JwtValidators.createDefaultWithIssuer(issuerUri),
                audienceValidator(securityProperties)
        );
    }

    /**
     * Keycloak publica el JWK Set en una ruta fija bajo el realm. Se respeta {@code jwk-set-uri} si
     * esta configurado, para no atar la aplicacion a esa convencion.
     */
    static String resolveJwkSetUri(OAuth2ResourceServerProperties.Jwt jwtProperties) {
        if (StringUtils.hasText(jwtProperties.getJwkSetUri())) {
            return jwtProperties.getJwkSetUri();
        }

        return jwtProperties.getIssuerUri() + "/protocol/openid-connect/certs";
    }

    /**
     * Que {@code required-audience} este relleno lo garantiza {@link SecurityProperties} en el
     * arranque, asi que aqui no hay ninguna rama que deje pasar el token por falta de
     * configuracion: o se valida la audiencia, o se ha desactivado de forma explicita.
     */
    private static OAuth2TokenValidator<Jwt> audienceValidator(SecurityProperties securityProperties) {
        if (!securityProperties.audienceValidationEnabled()) {
            return jwt -> OAuth2TokenValidatorResult.success();
        }

        return new JwtAudienceValidator(securityProperties.requiredAudience());
    }
}
