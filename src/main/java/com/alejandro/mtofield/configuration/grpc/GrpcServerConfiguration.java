package com.alejandro.mtofield.configuration.grpc;

import jakarta.annotation.PreDestroy;
import org.springframework.boot.grpc.server.autoconfigure.GrpcServerExecutorProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Los hilos del servidor gRPC.
 *
 * <p>Spring Boot no usa hilos virtuales en el servidor gRPC aunque este
 * {@code spring.threads.virtual.enabled=true}: la autoconfiguracion solo consume un
 * {@link GrpcServerExecutorProvider} opcional y, en modo Netty, no define ninguno. Sin este bean,
 * grpc-java atiende las llamadas con su pool cacheado de hilos de plataforma. Un hilo virtual por
 * tarea es lo que se quiere para cientos de streams que pasan la noche esperando: grpc-java
 * serializa los callbacks de cada llamada, asi que el orden por stream se conserva igual.</p>
 *
 * <p>El segundo ejecutor es para el trabajo que el servicio saca del hilo del callback: la
 * reproduccion del atraso de un stream, el despacho de ordenes tras el commit y el recalculo del
 * tablero. Es otro bean para que ese trabajo no comparta nombre ni destino con el del servidor.</p>
 */
@Configuration
public class GrpcServerConfiguration {

    private final ExecutorService serverExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final ExecutorService streamExecutor = Executors.newVirtualThreadPerTaskExecutor();

    @Bean
    public GrpcServerExecutorProvider grpcServerExecutorProvider() {
        return () -> serverExecutor;
    }

    @Bean
    public ExecutorService fieldStreamExecutor() {
        return streamExecutor;
    }

    @PreDestroy
    void shutdown() {
        streamExecutor.shutdownNow();
        serverExecutor.shutdownNow();
    }
}
