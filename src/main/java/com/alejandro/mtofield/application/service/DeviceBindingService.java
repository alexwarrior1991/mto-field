package com.alejandro.mtofield.application.service;

import java.util.UUID;

/**
 * De quien es un dispositivo. El {@code device_id} lo dice el cliente; lo que lo ata a una persona
 * es que fue ella quien se unio primero con el.
 */
public interface DeviceBindingService {

    /**
     * Liga el dispositivo a la persona en esa posesion, o confirma que ya era suyo. Devuelve false
     * si es de otra persona en una posesion que sigue abierta: entonces el {@code Join} se rechaza
     * y no se registra ningun stream.
     */
    boolean bind(String deviceId, String username, UUID possessionId);
}
