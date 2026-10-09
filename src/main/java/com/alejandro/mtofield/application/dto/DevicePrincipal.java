package com.alejandro.mtofield.application.dto;

/**
 * Quien abrio una llamada, capturado al abrirla: en los hilos propios del servicio no hay
 * {@code SecurityContext}, y lo que un dispositivo sube se registra a nombre de su persona.
 */
public record DevicePrincipal(String username, String userId) {
}
