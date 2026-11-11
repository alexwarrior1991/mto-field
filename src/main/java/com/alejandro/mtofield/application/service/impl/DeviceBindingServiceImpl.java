package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.service.DeviceBindingService;
import com.alejandro.mtofield.infrastructure.persistence.repository.DeviceBindingRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * La decision es de la base: una sentencia, y el recuento de filas es la respuesta. La transaccion
 * es solo esa sentencia, y esta confirmada antes de que el {@code Join} registre su stream.
 */
@Service
@RequiredArgsConstructor
class DeviceBindingServiceImpl implements DeviceBindingService {

    private final DeviceBindingRepository bindings;

    @Override
    @Transactional
    public boolean bind(String deviceId, String username, UUID possessionId) {
        return bindings.bind(deviceId, username, possessionId) == 1;
    }
}
