package com.alejandro.mtofield.simulator;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/** Salida del simulador: hora, quien habla y que dice. Sin framework de logs: es una herramienta. */
final class Log {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private Log() {
    }

    static void info(String who, String message) {
        System.out.printf("%s %-14s %s%n", LocalTime.now().format(TIME), who, message);
    }

    static void error(String who, String message) {
        System.out.printf("%s %-14s ERROR %s%n", LocalTime.now().format(TIME), who, message);
    }
}
