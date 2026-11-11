package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.application.dto.DevicePrincipal;
import com.alejandro.mtofield.application.service.DeviceBindingService;
import com.alejandro.mtofield.configuration.grpc.FieldProperties;
import com.alejandro.mtofield.infrastructure.grpc.GrpcErrors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

/**
 * Liga el dispositivo con la persona. El {@code device_id} de un {@code Join} lo dice el cliente, y
 * de el cuelgan el stream (uno por dispositivo: un segundo sustituye al primero), la secuencia de
 * subida y la marca de agua: sin esta liga, cualquiera admitido en un turno se unia con el
 * dispositivo de otro, cerraba su stream y subia eventos en su nombre.
 *
 * <p>El dispositivo es de quien se unio primero con el, mientras siga abierta la posesion en la que
 * lo hizo (V5, {@code device_binding}); lo decide la base en una sentencia, asi que vale igual con
 * varias replicas. Se pregunta en los dos streams, despues de la liga del equipo (quien no entra en
 * el turno no reserva dispositivos) y ANTES de registrar el stream: un {@code Join} rechazado no
 * sustituye a nadie, ni aqui ni, por el bus, en otra replica.</p>
 *
 * <p>Nadie esta exento, tampoco quien lleva {@code field-supervise}: el responsable actua por
 * cualquier equipo, pero con su dispositivo; lo que subiera con el de otro quedaria en la secuencia
 * de ese otro. Con {@code app.field.device-binding.enabled=false} no se pregunta ni se escribe nada.</p>
 */
@Component
public class DeviceBinding {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceBinding.class);

    public static final String REASON_DEVICE_NOT_YOURS = "DEVICE_NOT_YOURS";
    public static final String REASON_INVALID_DEVICE_ID = "INVALID_DEVICE_ID";
    public static final String DEVICE_ID = "device_id";
    /** El ancho de {@code device_id} en todas las tablas. */
    public static final int MAX_DEVICE_ID_LENGTH = 100;

    private final FieldProperties properties;
    private final DeviceBindingService bindings;

    public DeviceBinding(FieldProperties properties, DeviceBindingService bindings) {
        this.properties = properties;
        this.bindings = bindings;
    }

    public boolean isEnabled() {
        return properties.deviceBinding().enabled();
    }

    /** Un {@code device_id} que no cabe en su columna no se puede ligar ni guardar. */
    public static boolean fits(String deviceId) {
        return deviceId.length() <= MAX_DEVICE_ID_LENGTH;
    }

    public static StatusRuntimeException tooLong() {
        return GrpcErrors.of(Status.Code.INVALID_ARGUMENT, REASON_INVALID_DEVICE_ID,
                "device_id is longer than " + MAX_DEVICE_ID_LENGTH + " characters");
    }

    /** True si el dispositivo es (desde ahora o desde antes) de esa persona. */
    public boolean admit(DevicePrincipal principal, String deviceId, UUID possessionId) {
        if (!isEnabled()) {
            return true;
        }
        boolean admitted = bindings.bind(deviceId, principal.username(), possessionId);
        if (!admitted) {
            LOGGER.warn("User {} tried to join possession {} with device {}, which belongs to another user: refused", principal.username(),
                    possessionId, deviceId);
        }
        return admitted;
    }

    /** No dice de quien es: el nombre de otra persona no se le cuenta a quien intenta usar su dispositivo. */
    public StatusRuntimeException refusal(String deviceId) {
        return GrpcErrors.of(Status.Code.PERMISSION_DENIED, REASON_DEVICE_NOT_YOURS,
                "device " + deviceId + " is bound to another user while the possession it joined is open; a device is of whoever joined with it first",
                Map.of(DEVICE_ID, deviceId));
    }
}
