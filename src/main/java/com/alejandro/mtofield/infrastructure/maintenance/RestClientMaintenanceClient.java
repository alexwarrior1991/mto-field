package com.alejandro.mtofield.infrastructure.maintenance;

import com.alejandro.mtofield.application.dto.CompleteTaskCommand;
import com.alejandro.mtofield.application.exception.MaintenanceRejectedException;
import com.alejandro.mtofield.application.exception.MaintenanceUnavailableException;
import com.alejandro.mtofield.application.service.MaintenanceClient;
import com.alejandro.mtofield.domain.model.ShiftSnapshot;
import com.alejandro.mtofield.domain.model.TaskSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.circuitbreaker.CircuitBreaker;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Cliente de mto-maintenance sobre su API REST ({@code /api/v1/maintenance}), con la cuenta de
 * servicio {@code mto-field-svc}. Los DTOs que se leen son un subconjunto de los de mantenimiento:
 * solo las claves que este servicio usa, para que un campo nuevo alla no rompa nada aqui.
 *
 * <p>Toda llamada pasa por el circuito 'maintenance': con mantenimiento caido las llamadas fallan al
 * instante en vez de esperar un timeout cada una. Un fallo llega al servicio de una de dos maneras.
 * Si mantenimiento respondio que no (la tarea no esta en el estado que exige, el turno no trabaja
 * en esa via, la orden no existe) es {@link MaintenanceRejectedException}, con su codigo y su
 * mensaje, y no cuenta para el circuito, porque mantenimiento esta respondiendo (ver
 * {@link #isRejection}). Todo lo demas -red, tiempo agotado, 5xx, circuito abierto, la cuenta de
 * servicio rechazada- es {@link MaintenanceUnavailableException}.</p>
 *
 * <p>Ni {@code start} ni {@code complete} son idempotentes en mantenimiento: un reintento tras una
 * respuesta perdida es un 409 {@code TRN-001}. Quien llama lo reconcilia leyendo la tarea
 * ({@link #findTask}): {@code MaintenanceEventSynchronizer}.</p>
 */
public class RestClientMaintenanceClient implements MaintenanceClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestClientMaintenanceClient.class);

    /**
     * Los 4xx que no son un rechazo de lo pedido: 401 y 403 hablan de la cuenta de servicio (es
     * configuracion, y se arregla sin tocar el evento), y 408 y 429 son transitorios.
     */
    private static final Set<Integer> NOT_REJECTIONS = Set.of(401, 403, 408, 429);

    static final String SHIFTS = "/api/v1/maintenance/shifts";
    static final String ORDERS = "/api/v1/maintenance/orders";

    private final RestClient restClient;
    private final CircuitBreaker circuitBreaker;

    public RestClientMaintenanceClient(RestClient restClient, CircuitBreaker circuitBreaker) {
        this.restClient = restClient;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public Optional<ShiftSnapshot> findShift(UUID shiftId) {
        return call("find shift " + shiftId, () -> {
            try {
                ShiftPayload payload = restClient.get().uri(SHIFTS + "/{id}", shiftId).retrieve().body(ShiftPayload.class);
                return Optional.ofNullable(payload).map(ShiftPayload::toSnapshot);
            } catch (HttpClientErrorException.NotFound notFound) {
                return Optional.empty();
            }
        });
    }

    @Override
    public Optional<TaskSnapshot> findTask(UUID orderId, UUID taskId) {
        return call("find task " + taskId + " of order " + orderId, () -> {
            try {
                TaskPayload payload = restClient.get().uri(ORDERS + "/{orderId}/tasks/{taskId}", orderId, taskId).retrieve().body(TaskPayload.class);
                return Optional.ofNullable(payload).map(TaskPayload::toSnapshot);
            } catch (HttpClientErrorException.NotFound notFound) {
                return Optional.empty();
            }
        });
    }

    @Override
    public TaskSnapshot startTask(UUID orderId, UUID taskId, UUID shiftId, String assignedUser) {
        return call("start task " + taskId + " of order " + orderId, () -> {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("shiftId", shiftId);
            put(body, "assignedUser", assignedUser);
            return task(restClient.post().uri(ORDERS + "/{orderId}/tasks/{taskId}/start", orderId, taskId).body(body)
                    .retrieve().body(TaskPayload.class));
        });
    }

    /**
     * El cuerpo es el {@code CompleteTaskRequest} de mantenimiento con lo que el contrato de campo
     * trae. Lo vacio no viaja: una lista de tipos vacia dejaria la tarea sin tipos, y mantenimiento
     * conserva los que tiene cuando no se mandan.
     */
    @Override
    public TaskSnapshot completeTask(UUID orderId, UUID taskId, CompleteTaskCommand command) {
        return call("complete task " + taskId + " of order " + orderId, () -> {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("shiftId", command.shiftId());
            if (!command.taskTypeCodes().isEmpty()) {
                body.put("taskTypeCodes", command.taskTypeCodes());
            }
            put(body, "notes", command.notes());
            body.put("workComplete", command.workComplete());
            if (!command.defects().isEmpty()) {
                body.put("inlineDefects", command.defects().stream().map(defect -> {
                    Map<String, Object> inline = new LinkedHashMap<>();
                    put(inline, "severity", defect.severity());
                    put(inline, "description", defect.description());
                    put(inline, "technicalNotes", defect.technicalNotes());
                    put(inline, "correctionType", defect.correctionType());
                    put(inline, "partsReplaced", defect.partsReplaced());
                    return inline;
                }).toList());
            }
            if (!command.photoRefs().isEmpty()) {
                body.put("photoRefs", command.photoRefs());
            }
            return task(restClient.post().uri(ORDERS + "/{orderId}/tasks/{taskId}/complete", orderId, taskId).body(body)
                    .retrieve().body(TaskPayload.class));
        });
    }

    private static void put(Map<String, Object> body, String key, String value) {
        if (value != null && !value.isBlank()) {
            body.put(key, value);
        }
    }

    private static TaskSnapshot task(TaskPayload payload) {
        if (payload == null) {
            throw new MaintenanceUnavailableException("mto-maintenance returned an empty task");
        }
        return payload.toSnapshot();
    }

    /**
     * Si el fallo es un rechazo de mantenimiento y no una caida: una respuesta 4xx salvo 401, 403,
     * 408 y 429. Es tambien el predicado con el que el circuito 'maintenance' ignora los rechazos
     * ({@code MaintenanceClientConfiguration}): sin el, cinco tareas rechazadas seguidas abririan el
     * circuito y dejarian a mantenimiento por caido durante medio minuto.
     */
    public static boolean isRejection(Throwable throwable) {
        if (!(throwable instanceof RestClientResponseException response)) {
            return false;
        }
        int status = response.getStatusCode().value();
        return status >= 400 && status < 500 && !NOT_REJECTIONS.contains(status);
    }

    private <T> T call(String operation, Supplier<T> action) {
        try {
            return circuitBreaker.run(action, throwable -> {
                throw translate(operation, throwable);
            });
        } catch (MaintenanceUnavailableException | MaintenanceRejectedException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw translate(operation, exception);
        }
    }

    private static RuntimeException translate(String operation, Throwable throwable) {
        if (throwable instanceof MaintenanceUnavailableException || throwable instanceof MaintenanceRejectedException) {
            return (RuntimeException) throwable;
        }
        if (throwable instanceof RestClientResponseException response) {
            int status = response.getStatusCode().value();
            String body = response.getResponseBodyAsString();
            if (isRejection(response)) {
                ErrorPayload error = errorOf(response);
                String code = error == null ? null : error.errorCode();
                String detail = error == null || error.message() == null || error.message().isBlank() ? body : error.message();
                MaintenanceRejectedException rejected = new MaintenanceRejectedException(operation, status, code, detail);
                LOGGER.warn("{}", rejected.getMessage());
                return rejected;
            }
            HttpStatus resolved = HttpStatus.resolve(status);
            LOGGER.warn("mto-maintenance failed '{}': status={}, body={}", operation, status, body);
            return new MaintenanceUnavailableException("mto-maintenance answered " + (resolved == null ? response.getStatusCode() : resolved)
                    + " to '" + operation + "'" + (body == null || body.isBlank() ? "" : ": " + body), response);
        }
        LOGGER.warn("mto-maintenance call '{}' failed: {}", operation, throwable.toString());
        return new MaintenanceUnavailableException("mto-maintenance is unavailable for '" + operation + "': " + throwable.getMessage(), throwable);
    }

    /** El cuerpo de error de mantenimiento ({@code ApiErrorResponse}), o null si no se puede leer (un proxy, un cuerpo vacio...). */
    private static ErrorPayload errorOf(RestClientResponseException response) {
        try {
            return response.getResponseBodyAs(ErrorPayload.class);
        } catch (RuntimeException unreadable) {
            return null;
        }
    }

    record ShiftPayload(UUID id, String code, LocalDate shiftDate, String status, TeamPayload team, Instant plannedStart, Instant plannedEnd,
                        List<Long> trackIds) {
        ShiftSnapshot toSnapshot() {
            return new ShiftSnapshot(id, code, shiftDate, status, team == null ? null : team.code(), team == null ? null : team.name(),
                    plannedStart, plannedEnd, trackIds);
        }
    }

    record TeamPayload(UUID id, String code, String name) {
    }

    record TaskPayload(UUID id, UUID orderId, String status, UUID shiftId, String assignedUser) {
        TaskSnapshot toSnapshot() {
            return new TaskSnapshot(id, orderId, status, shiftId, assignedUser);
        }
    }

    /** Lo que se lee del JSON de error de mantenimiento: su codigo estable y su mensaje. */
    record ErrorPayload(String errorCode, String message) {
    }
}
