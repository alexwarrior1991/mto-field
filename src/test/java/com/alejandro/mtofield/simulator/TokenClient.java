package com.alejandro.mtofield.simulator;

import com.alejandro.mtofield.support.TestTokens;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * De donde salen los tokens. Contra la plataforma, el password grant de {@code mto-frontend} del
 * realm local (abierto a proposito alli). Sin Keycloak, el emisor local: el simulador sirve el JWK
 * Set de la clave de {@link TestTokens} en el puerto del emisor, y el servidor, arrancado con
 * {@code KEYCLOAK_ISSUER_URI=http://localhost:8082/realms/mto}, valida contra el al primer token.
 */
final class TokenClient implements AutoCloseable {

    private static final Pattern ACCESS_TOKEN = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");

    private final SimulatorOptions options;
    private final HttpServer issuer;
    private String cachedPasswordToken;

    TokenClient(SimulatorOptions options) throws IOException {
        this.options = options;
        this.issuer = options.localIssuer() ? startLocalIssuer(options.issuerPort()) : null;
    }

    private static HttpServer startLocalIssuer(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        byte[] jwks = TestTokens.publicJwkSetJson().getBytes(StandardCharsets.UTF_8);
        server.createContext("/realms/mto/protocol/openid-connect/certs", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, jwks.length);
            try (OutputStream body = exchange.getResponseBody()) {
                body.write(jwks);
            }
        });
        server.start();
        Log.info("issuer", "serving the test JWK Set at http://127.0.0.1:" + port + "/realms/mto/protocol/openid-connect/certs"
                + " (run the server with KEYCLOAK_ISSUER_URI=" + TestTokens.ISSUER + ")");
        return server;
    }

    String supervisorToken() throws IOException, InterruptedException {
        if (options.localIssuer()) {
            return TestTokens.supervisor("sim.responsable");
        }
        return token();
    }

    String deviceToken(int team, int device) throws IOException, InterruptedException {
        if (options.localIssuer()) {
            return TestTokens.technician("sim.tecnico" + team + "." + device);
        }
        return token();
    }

    private synchronized String token() throws IOException, InterruptedException {
        if (options.token() != null) {
            return options.token();
        }
        if (cachedPasswordToken != null) {
            return cachedPasswordToken;
        }
        if (options.user() == null || options.password() == null) {
            throw new IllegalArgumentException("Give --token, --user and --password, or --local-issuer");
        }
        String form = "grant_type=password&client_id=" + encode(options.clientId()) + "&username=" + encode(options.user())
                + "&password=" + encode(options.password());
        HttpRequest request = HttpRequest.newBuilder(URI.create(options.tokenUrl()))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form))
                .build();
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("Token request failed with " + response.statusCode() + ": " + response.body());
            }
            Matcher matcher = ACCESS_TOKEN.matcher(response.body());
            if (!matcher.find()) {
                throw new IllegalStateException("No access_token in the token response");
            }
            cachedPasswordToken = matcher.group(1);
            Log.info("issuer", "token obtained for " + options.user() + " from " + options.tokenUrl());
            return cachedPasswordToken;
        }
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        if (issuer != null) {
            issuer.stop(0);
        }
    }
}
