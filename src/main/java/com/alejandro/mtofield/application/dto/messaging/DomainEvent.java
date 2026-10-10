package com.alejandro.mtofield.application.dto.messaging;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * El {@code data} de los eventos propios de este servicio.
 *
 * <p>Es la forma que comparten los productores del dominio que no publican datos maestros
 * ({@code mto-configuration} con {@code job.finished}, {@code mto-users}): lo que cambio, su
 * identificador, que paso y lo que hace falta para contarlo. El nombre del evento va en texto y no
 * en un enumerado cerrado, porque {@code status-changed} o {@code rejected} no son un alta, una
 * modificacion ni un borrado, y un consumidor decide que hacer con lo que no conoce.</p>
 *
 * <p>Un evento nunca lleva secretos: una clave que huela a credencial, a cualquier profundidad,
 * se rechaza al construirlo, antes de que llegue al outbox. Los valores nulos si viajan: para el
 * consumidor «no tiene equipo» es informacion.</p>
 *
 * @param entityName lo que cambio, en minusculas y con guiones ({@code order}, {@code defect})
 * @param entityId   su identificador, como texto
 * @param eventName  que paso, en minusculas y con guiones ({@code created}, {@code status-changed})
 * @param values     lo que hace falta para contarlo, sin secretos; se conserva el orden de las claves
 */
public record DomainEvent(
        String entityName,
        String entityId,
        String eventName,
        Map<String, Object> values
) {

    /** Lo que no puede ser el nombre de una clave de {@code values}, a ninguna profundidad. */
    static final Pattern SENSITIVE_KEY = Pattern.compile(
            "(?i)password|passwd|pwd|secret|token|credential|otp|authorization|cookie|api[-_]?key");

    public DomainEvent {
        entityName = requireText(entityName, "entityName");
        entityId = requireText(entityId, "entityId");
        eventName = requireText(eventName, "eventName");
        values = values == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(values));
        rejectSensitiveKeys(values, "values");
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required in a domain event");
        }
        return value.trim();
    }

    private static void rejectSensitiveKeys(Object value, String path) {
        if (value instanceof Map<?, ?> map) {
            map.forEach((key, nested) -> {
                String name = String.valueOf(key);
                if (SENSITIVE_KEY.matcher(name).find()) {
                    throw new IllegalArgumentException(
                            "A value named '" + path + "." + name + "' cannot travel in a domain event");
                }
                rejectSensitiveKeys(nested, path + "." + name);
            });
        } else if (value instanceof Iterable<?> items) {
            for (Object item : items) {
                rejectSensitiveKeys(item, path + "[]");
            }
        }
    }
}
