package com.alejandro.mtofield.support;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Assumptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * El Toxiproxy entre el cliente de un test y el Netty de esta JVM: un proxy TCP por test al que se
 * le cortan, retrasan o estrangulan las conexiones.
 *
 * <p>Con Docker levanta el contenedor oficial y le expone el puerto del servidor
 * ({@code host.testcontainers.internal}); sin Docker, {@code TOXIPROXY_URL} apunta a un
 * {@code toxiproxy-server} corriendo en esta misma maquina, como {@code TEST_DATABASE_URL} con
 * PostgreSQL. Sin ninguna de las dos cosas el test se omite, no falla.</p>
 *
 * <p>El contenedor expone 32 puertos (8666..8697), uno por proxy, y cada test borra el suyo al
 * acabar; con el servidor externo el proxy escucha en el puerto libre que le de el sistema.</p>
 */
public final class ToxiproxyGateway {

    private static final Logger LOG = LoggerFactory.getLogger(ToxiproxyGateway.class);
    private static final String EXTERNAL_URL = System.getenv("TOXIPROXY_URL");
    private static final String IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0";
    private static final int FIRST_PROXIED_PORT = 8666;
    private static final int PROXIED_PORTS = 32;
    private static final String CONTAINER_UPSTREAM_HOST = "host.testcontainers.internal";

    private static ToxiproxyGateway instance;

    private final ToxiproxyClient client;
    private final ToxiproxyContainer container;
    private final String upstreamHost;
    private final AtomicInteger proxies = new AtomicInteger();

    private ToxiproxyGateway(ToxiproxyClient client, ToxiproxyContainer container, String upstreamHost) {
        this.client = client;
        this.container = container;
        this.upstreamHost = upstreamHost;
    }

    /**
     * El Toxiproxy de esta JVM, capaz de llegar al puerto dado de esta maquina. Con Docker el
     * puerto se expone antes de arrancar el contenedor, que es cuando resuelve
     * {@code host.testcontainers.internal}; exponerlo otra vez es inocuo.
     */
    public static synchronized ToxiproxyGateway towards(int serverPort) {
        boolean external = EXTERNAL_URL != null && !EXTERNAL_URL.isBlank();
        if (!external) {
            Assumptions.assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                    "Neither Docker nor TOXIPROXY_URL is available: skipping the Toxiproxy-backed test");
            org.testcontainers.Testcontainers.exposeHostPorts(serverPort);
        }
        if (instance == null) {
            instance = external ? external(URI.create(EXTERNAL_URL)) : container();
        }
        return instance;
    }

    private static ToxiproxyGateway external(URI url) {
        int port = url.getPort() == -1 ? 8474 : url.getPort();
        ToxiproxyClient client = new ToxiproxyClient(url.getHost(), port);
        try {
            LOG.info("Using the external Toxiproxy {} (version {})", url, client.version());
        } catch (IOException unreachable) {
            throw new UncheckedIOException("TOXIPROXY_URL=" + url + " does not answer", unreachable);
        }
        return new ToxiproxyGateway(client, null, "127.0.0.1");
    }

    private static ToxiproxyGateway container() {
        ToxiproxyContainer container = new ToxiproxyContainer(IMAGE);
        container.start();
        ToxiproxyClient client = new ToxiproxyClient(container.getHost(), container.getControlPort());
        return new ToxiproxyGateway(client, container, CONTAINER_UPSTREAM_HOST);
    }

    /** Un proxy nuevo hacia el servidor de esta JVM; quien lo pide lo cierra al acabar. */
    public Link proxyTo(String name, int serverPort) {
        String upstream = upstreamHost + ":" + serverPort;
        try {
            if (container == null) {
                Proxy proxy = client.createProxy(name, "127.0.0.1:0", upstream);
                String listen = proxy.getListen();
                int separator = listen.lastIndexOf(':');
                return new Link(proxy, listen.substring(0, separator), Integer.parseInt(listen.substring(separator + 1)));
            }
            int listenPort = FIRST_PROXIED_PORT + proxies.getAndIncrement() % PROXIED_PORTS;
            Proxy proxy = client.createProxy(name, "0.0.0.0:" + listenPort, upstream);
            return new Link(proxy, container.getHost(), container.getMappedPort(listenPort));
        } catch (IOException failed) {
            throw new UncheckedIOException("Could not create the Toxiproxy proxy " + name, failed);
        }
    }

    /** Un proxy y la direccion en la que el test lo marca. */
    public record Link(Proxy proxy, String host, int port) {

        public ManagedChannelBuilder<?> channelBuilder() {
            return ManagedChannelBuilder.forAddress(host, port).usePlaintext();
        }

        public void close() {
            try {
                proxy.delete();
            } catch (IOException failed) {
                LOG.warn("Could not delete the Toxiproxy proxy {}: {}", proxy.getName(), failed.toString());
            }
        }
    }
}
