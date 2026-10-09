package com.alejandro.mtofield.support;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;

import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tokens firmados con una clave RSA que solo existe en los tests. {@link TestJwtDecoderConfiguration}
 * pone un decoder con su clave publica y la MISMA cadena de validadores que el de produccion, asi
 * que emisor, vigencia, firma y audiencia se comprueban de verdad sin un Keycloak detras.
 */
public final class TestTokens {

    public static final String ISSUER = "http://localhost:8082/realms/mto";
    public static final String AUDIENCE = "mto-field-api";
    public static final String OTHER_AUDIENCE = "mto-maintenance-api";

    private static final RSAKey KEY = generateKey("field-test");
    private static final RSAKey FOREIGN_KEY = generateKey("foreign");

    private TestTokens() {
    }

    public static RSAPublicKey publicKey() {
        try {
            return KEY.toRSAPublicKey();
        } catch (JOSEException exception) {
            throw new IllegalStateException(exception);
        }
    }

    /** El JWK Set publico de la clave de test, con la forma que sirve Keycloak: lo que el simulador ofrece como emisor local. */
    public static String publicJwkSetJson() {
        return new JWKSet(KEY.toPublicJWK()).toString();
    }

    /** Un dispositivo de un equipo: solo {@code field-team}. */
    public static String technician(String username) {
        return mint(username, List.of(AUDIENCE), List.of("field-team"), List.of("mto-field-technician"), inOneHour());
    }

    /** El responsable: {@code field-team} y {@code field-supervise}. */
    public static String supervisor(String username) {
        return mint(username, List.of(AUDIENCE), List.of("field-team", "field-supervise"), List.of("mto-field-supervisor"), inOneHour());
    }

    /** Solo {@code field-supervise}, sin {@code field-team}: no existe en el realm, pero separa los dos permisos. */
    public static String supervisorWithoutTeam(String username) {
        return mint(username, List.of(AUDIENCE), List.of("field-supervise"), List.of(), inOneHour());
    }

    public static String forAnotherAudience(String username) {
        return mint(username, List.of(OTHER_AUDIENCE), List.of("field-team", "field-supervise"), List.of(), inOneHour());
    }

    public static String expired(String username) {
        return mint(username, List.of(AUDIENCE), List.of("field-team", "field-supervise"), List.of(), Instant.now().minus(Duration.ofMinutes(5)));
    }

    public static String signedByAnotherKey(String username) {
        return sign(FOREIGN_KEY, claims(username, List.of(AUDIENCE), List.of("field-team", "field-supervise"), List.of(), inOneHour()));
    }

    public static String mint(String username, List<String> audience, List<String> clientRoles, List<String> realmRoles,
                              Instant expiresAt) {
        return sign(KEY, claims(username, audience, clientRoles, realmRoles, expiresAt));
    }

    public static Metadata bearer(String token) {
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
        return metadata;
    }

    public static <S extends AbstractStub<S>> S withToken(S stub, String token) {
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(bearer(token)));
    }

    private static JWTClaimsSet claims(String username, List<String> audience, List<String> clientRoles,
                                       List<String> realmRoles, Instant expiresAt) {
        Instant issuedAt = expiresAt.minus(Duration.ofHours(1));
        return new JWTClaimsSet.Builder()
                .issuer(ISSUER)
                .subject(UUID.nameUUIDFromBytes(username.getBytes()).toString())
                .audience(audience)
                .issueTime(Date.from(issuedAt))
                .expirationTime(Date.from(expiresAt))
                .jwtID(UUID.randomUUID().toString())
                .claim("preferred_username", username)
                .claim("email", username + "@mto.local")
                .claim("resource_access", Map.of(AUDIENCE, Map.of("roles", clientRoles)))
                .claim("realm_access", Map.of("roles", realmRoles))
                .build();
    }

    private static String sign(RSAKey key, JWTClaimsSet claims) {
        try {
            SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(), claims);
            jwt.sign(new RSASSASigner(key));
            return jwt.serialize();
        } catch (JOSEException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static RSAKey generateKey(String keyId) {
        try {
            return new RSAKeyGenerator(2048).keyID(keyId).generate();
        } catch (JOSEException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static Instant inOneHour() {
        return Instant.now().plus(Duration.ofHours(1));
    }
}
