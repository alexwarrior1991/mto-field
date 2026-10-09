package com.alejandro.mtofield.support;

import com.alejandro.mtofield.configuration.security.SecurityConfiguration;
import com.alejandro.mtofield.configuration.security.SecurityProperties;
import org.springframework.boot.security.oauth2.server.resource.autoconfigure.OAuth2ResourceServerProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

/**
 * El decoder de los tests: la clave publica de {@link TestTokens} en lugar del JWK Set de Keycloak,
 * y la misma cadena de validadores que el de produccion ({@link SecurityConfiguration#jwtValidator}).
 * Lo que aqui pasa o falla (firma, emisor, vigencia, audiencia) es lo que pasaria o fallaria con un
 * token de verdad.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestJwtDecoderConfiguration {

    @Bean
    @Primary
    JwtDecoder testJwtDecoder(SecurityProperties securityProperties, OAuth2ResourceServerProperties resourceServer) {
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withPublicKey(TestTokens.publicKey()).build();
        decoder.setJwtValidator(SecurityConfiguration.jwtValidator(securityProperties, resourceServer.getJwt().getIssuerUri()));
        return decoder;
    }
}
