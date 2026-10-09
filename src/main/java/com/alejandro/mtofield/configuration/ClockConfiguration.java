package com.alejandro.mtofield.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * El reloj con el que los servicios fechan lo que escriben ({@code opened_at}, {@code issued_at})
 * y con el que el tablero mide el silencio de un dispositivo. Es un bean para que los tests lo
 * fijen; en produccion es el del sistema en UTC.
 */
@Configuration
public class ClockConfiguration {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
