package com.alejandro.mtofield.infrastructure.messaging;

import com.alejandro.mtofield.application.dto.messaging.AsynchronousMessage;
import com.alejandro.mtofield.application.dto.messaging.DomainEvent;
import com.alejandro.mtofield.application.dto.messaging.MessageActor;
import com.alejandro.mtofield.application.service.impl.FieldEvents;
import com.alejandro.mtofield.infrastructure.messaging.outbox.AsynchronousMessageFactory;
import com.alejandro.mtofield.infrastructure.messaging.outbox.AsynchronousMessageHashService;
import com.alejandro.mtofield.infrastructure.messaging.outbox.MessageContextResolver;
import com.alejandro.mtofield.infrastructure.messaging.rabbitmq.FieldRabbitMqNames;
import com.alejandro.mtofield.infrastructure.persistence.entity.Possession;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionShift;
import com.alejandro.mtofield.infrastructure.persistence.entity.PossessionStatus;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Los ejemplos de {@code docs/messaging/examples} son el contrato tal como lo ven los consumidores:
 * {@code mto-notification} los copia como fixtures. Un ejemplo escrito a mano se desalinea del codigo
 * sin que nadie lo note; este test lo evita construyendo cada mensaje con {@link FieldEvents} y la
 * factoria real y comparandolo con el fichero, salvo las dos claves que cambian en cada ejecucion
 * (la fecha y, con ella, la huella), que se comprueban aparte: la huella del fichero es la que un
 * consumidor calcularia sobre sus siete claves originales.
 *
 * <p>Un evento nuevo o una clave nueva cambian el ejemplo en el mismo commit. Para regenerarlos:
 * {@code MESSAGING_EXAMPLES_WRITE=true ./mvnw test -Dtest=MessagingContractExamplesTest}, y se
 * revisa el diff como cualquier cambio de contrato.</p>
 */
class MessagingContractExamplesTest {

    private static final Path EXAMPLES = Path.of("docs", "messaging", "examples");

    private static final boolean WRITE = "true".equalsIgnoreCase(System.getenv("MESSAGING_EXAMPLES_WRITE"));

    private static final Instant CREATION_DATE = Instant.parse("2026-10-09T22:05:00Z");

    private static final MessageActor SUPERVISOR = MessageActor.of("6f1b1c8e-0000-4000-8000-000000000041", "campo.responsable");
    private static final MessageActor TECHNICIAN = MessageActor.of("6f1b1c8e-0000-4000-8000-000000000042", "campo.tecnico1");

    private static final String POSSESSION_ID = "70000000-0000-4000-8000-000000000001";
    /** La correlacion de todo lo que pasa en una noche: el codigo de la posesion. */
    private static final String CORRELATION_ID = "PO-000012";
    private static final UUID SHIFT_NORTH = UUID.fromString("90000000-0000-4000-8000-000000000001");
    private static final UUID SHIFT_SOUTH = UUID.fromString("90000000-0000-4000-8000-000000000002");
    private static final UUID EVACUATION = UUID.fromString("c0000000-0000-4000-8000-000000000001");
    private static final Instant ISSUED_AT = Instant.parse("2026-10-09T23:40:00Z");

    private final ObjectMapper objectMapper = JsonMapper.builder().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build();
    private final AsynchronousMessageHashService hashService = new AsynchronousMessageHashService(objectMapper);

    @Test
    void possessionOpened() throws IOException {
        check("possession-opened.json", "b0000000-0000-4000-8000-000000000001", SUPERVISOR,
                FieldEvents.possessionOpened(possession()));
    }

    @Test
    void possessionClosed() throws IOException {
        Possession possession = possession();
        possession.setStatus(PossessionStatus.CLOSED);
        possession.setClosedAt(Instant.parse("2026-10-10T04:10:00Z"));
        possession.setClosedBy("campo.responsable");
        possession.setForced(true);
        possession.setCloseReason("Fin de la ventana");

        check("possession-closed.json", "b0000000-0000-4000-8000-000000000002", SUPERVISOR,
                FieldEvents.possessionClosed(possession, List.of("EQ-SUR")));
    }

    @Test
    void evacuationIssued() throws IOException {
        check("possession-evacuation-issued.json", "b0000000-0000-4000-8000-000000000003", SUPERVISOR,
                FieldEvents.evacuationIssued(possession(), EVACUATION, 3, "Tren de trabajos en aproximacion", ISSUED_AT, "campo.responsable"));
    }

    @Test
    void evacuationAcknowledged() throws IOException {
        Possession possession = possession();
        check("possession-evacuation-acknowledged.json", "b0000000-0000-4000-8000-000000000004", TECHNICIAN,
                FieldEvents.evacuationAcknowledged(possession, EVACUATION, 3, possession.getShifts().getFirst(), "tab-norte-01", "campo.tecnico1",
                        true, null, Instant.parse("2026-10-09T23:40:42Z"), List.of("EQ-SUR")));
    }

    @Test
    void evacuationUnacknowledged() throws IOException {
        check("possession-evacuation-unacknowledged.json", "b0000000-0000-4000-8000-000000000005", MessageActor.system(),
                FieldEvents.evacuationUnacknowledged(possession(), EVACUATION, 3, ISSUED_AT, "campo.responsable", List.of("EQ-SUR"),
                        Instant.parse("2026-10-09T23:42:30Z")));
    }

    @Test
    void clearOfTrack() throws IOException {
        Possession possession = possession();
        check("possession-clear-of-track.json", "b0000000-0000-4000-8000-000000000006", TECHNICIAN,
                FieldEvents.clearOfTrack(possession, possession.getShifts().getFirst(), "tab-norte-01", "campo.tecnico1", true,
                        Instant.parse("2026-10-10T03:55:00Z"), List.of("EQ-SUR")));
    }

    // ------------------------------------------------------------------------------ fixtures

    private static Possession possession() {
        Possession possession = Possession.builder()
                .code(CORRELATION_ID)
                .status(PossessionStatus.OPEN)
                .shiftDate(LocalDate.of(2026, 10, 9))
                .endsAt(Instant.parse("2026-10-10T04:00:00Z"))
                .openedAt(Instant.parse("2026-10-09T22:05:00Z"))
                .openedBy("campo.responsable")
                .build();
        ReflectionTestUtils.setField(possession, "id", UUID.fromString(POSSESSION_ID));
        possession.addShift(shift(SHIFT_NORTH, "SH-000077", "EQ-NORTE", "Equipo noche norte"));
        possession.addShift(shift(SHIFT_SOUTH, "SH-000078", "EQ-SUR", "Equipo noche sur"));
        return possession;
    }

    private static PossessionShift shift(UUID shiftId, String code, String teamCode, String teamName) {
        return PossessionShift.builder()
                .shiftId(shiftId)
                .shiftCode(code)
                .teamCode(teamCode)
                .teamName(teamName)
                .plannedEnd(Instant.parse("2026-10-10T04:00:00Z"))
                .open(true)
                .build();
    }

    // ------------------------------------------------------------------------------ helpers

    private void check(String fileName, String operationId, MessageActor actor, DomainEvent event) throws IOException {
        MessageContextResolver contextResolver = mock(MessageContextResolver.class);
        when(contextResolver.currentActor()).thenReturn(actor);
        when(contextResolver.currentCorrelationId()).thenReturn(CORRELATION_ID);
        AsynchronousMessageFactory factory = new AsynchronousMessageFactory(hashService, contextResolver, "mto-field");

        AsynchronousMessage<DomainEvent> message = factory.create(UUID.fromString(operationId),
                event.entityName() + "-" + event.entityId(),
                FieldRabbitMqNames.eventType(event.entityName(), event.eventName()), event);

        Path file = EXAMPLES.resolve(fileName);
        if (WRITE) {
            Files.createDirectories(EXAMPLES);
            Files.writeString(file, pretty(tree(withCreationDate(message, CREATION_DATE)), 0) + "\n");
        }

        assertThat(file).as("el ejemplo %s tiene que estar versionado", fileName).exists();
        assertSameAsExample(message, objectMapper.readTree(Files.readString(file)));
    }

    /**
     * Compara el mensaje con el ejemplo por el texto que viaja de verdad (releido como arbol para
     * que el orden de las claves no cuente), y la huella del ejemplo con la que se obtiene de sus
     * propias siete claves, que es la unica forma de que el ejemplo lleve una huella cierta.
     */
    private void assertSameAsExample(AsynchronousMessage<DomainEvent> message, JsonNode example) {
        ObjectNode produced = tree(message);

        assertThat(Instant.parse(produced.get("creationDate").asText())).isNotNull();
        assertThat(produced.get("messageHash").asText()).matches("[0-9a-f]{64}");

        produced.put("creationDate", example.get("creationDate").asText());
        produced.put("messageHash", example.get("messageHash").asText());

        assertThat(produced).isEqualTo(example);

        AsynchronousMessage<DomainEvent> asInExample = withCreationDate(message, Instant.parse(example.get("creationDate").asText()));
        assertThat(example.get("messageHash").asText())
                .as("la huella del ejemplo es la de sus siete claves originales, con la fecha del ejemplo")
                .isEqualTo(asInExample.messageHash());
    }

    private AsynchronousMessage<DomainEvent> withCreationDate(AsynchronousMessage<DomainEvent> message, Instant creationDate) {
        AsynchronousMessage<DomainEvent> dated = new AsynchronousMessage<>(message.operationId(), message.referenceId(), message.origin(),
                creationDate, message.eventType(), message.data(), "PENDING", message.actor(), message.correlationId());
        return dated.withMessageHash(hashService.calculate(dated));
    }

    private ObjectNode tree(AsynchronousMessage<DomainEvent> message) {
        return (ObjectNode) objectMapper.readTree(objectMapper.writeValueAsString(message));
    }

    /** Dos espacios, una clave por linea y los elementos de una lista tambien: un diff legible en una revision. */
    private static String pretty(JsonNode node, int depth) {
        String pad = "  ".repeat(depth + 1);
        if (node.isObject()) {
            if (node.isEmpty()) {
                return "{}";
            }
            List<String> fields = new ArrayList<>();
            node.properties().forEach(field -> fields.add(pad + "\"" + field.getKey() + "\": " + pretty(field.getValue(), depth + 1)));
            return "{\n" + String.join(",\n", fields) + "\n" + "  ".repeat(depth) + "}";
        }
        if (node.isArray()) {
            if (node.isEmpty()) {
                return "[]";
            }
            List<String> items = new ArrayList<>();
            node.forEach(item -> items.add(pad + pretty(item, depth + 1)));
            return "[\n" + String.join(",\n", items) + "\n" + "  ".repeat(depth) + "]";
        }
        return node.toString();
    }
}
