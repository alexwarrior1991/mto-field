package com.alejandro.mtofield.application.dto;

import com.alejandro.mtofield.grpc.v1.TeamMessage;

import java.util.UUID;

/** Un mensaje subido por un dispositivo junto con lo que el stream ya sabe de el. */
public record EventContext(UUID possessionId, UUID shiftId, String deviceId, DevicePrincipal principal, TeamMessage message) {

    public long sequence() {
        return message.getSequence();
    }
}
