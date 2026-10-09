package com.alejandro.mtofield.configuration.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.server.GlobalServerInterceptor;
import org.springframework.grpc.server.security.AuthenticationProcessInterceptor;
import org.springframework.grpc.server.security.GrpcSecurity;
import org.springframework.security.oauth2.jwt.JwtDecoder;

/**
 * La seguridad del servidor gRPC: el mismo {@link JwtDecoder} (JWK Set de Keycloak mas la
 * validacion de audiencia) y el mismo {@link KeycloakJwtAuthenticationConverter} que la cadena HTTP.
 *
 * <p>Sustituye al interceptor que Spring Boot configura por defecto cuando encuentra un
 * {@code JwtDecoder}, que exige token en todas las llamadas. Aqui la salud y la reflexion quedan
 * abiertas: la salud la consulta el orquestador, que no tiene token, y la reflexion es lo que usa
 * {@code grpcurl} para listar el servicio (en prod se apaga con
 * {@code spring.grpc.server.reflection.enabled=false}). Todo lo demas pide un token valido; que rol
 * hace falta en cada RPC lo dice el {@code @PreAuthorize} de su metodo en {@code FieldGrpcService}.</p>
 *
 * <p>El interceptor autentica, deja el {@code SecurityContext} puesto antes de que el servicio
 * reciba la llamada (tambien al abrir un stream) y lo repone alrededor de cada callback del
 * stream. En los hilos que crea el propio servicio no existe: quien los necesite captura el
 * principal al abrir la llamada.</p>
 */
@Configuration
public class GrpcSecurityConfiguration {

    static final String[] OPEN_METHODS = {
            "grpc.health.v1.Health/*",
            "grpc.reflection.v1.ServerReflection/*",
            "grpc.reflection.v1alpha.ServerReflection/*"
    };

    @Bean
    @GlobalServerInterceptor
    public AuthenticationProcessInterceptor fieldGrpcSecurity(
            GrpcSecurity grpcSecurity,
            JwtDecoder jwtDecoder,
            KeycloakJwtAuthenticationConverter jwtAuthenticationConverter
    ) throws Exception {
        return grpcSecurity
                .authorizeRequests(requests -> requests
                        .methods(OPEN_METHODS).permitAll()
                        .allRequests().authenticated())
                .oauth2ResourceServer(oauth2 -> oauth2
                        .jwt(jwt -> jwt
                                .decoder(jwtDecoder)
                                .jwtAuthenticationConverter(jwtAuthenticationConverter)))
                .build();
    }
}
