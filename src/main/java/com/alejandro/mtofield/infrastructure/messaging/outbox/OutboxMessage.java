package com.alejandro.mtofield.infrastructure.messaging.outbox;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.Generated;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.generator.EventType;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Entity
@Table(name = "outbox_message")
public class OutboxMessage {

    @Id
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(nullable = false, length = 100)
    private String aggregateType;

    @Column(nullable = false, length = 150)
    private String aggregateId;

    @Column(nullable = false, length = 150)
    private String eventType;

    @Column(nullable = false, length = 200)
    private String exchangeName;

    @Column(nullable = false, length = 200)
    private String routingKey;

    /**
     * {@code @Lob} sobre un String hace que PostgreSQL use una columna {@code oid},
     * es decir, un large object en {@code pg_largeobject} referenciado por OID. Eso
     * trae dos problemas serios para el outbox: el contenido solo es legible con la
     * transaccion abierta, y al borrar la fila el large object NO se borra, queda
     * huerfano hasta que alguien pase un vacuumlo. Con purga automatica de mensajes
     * publicados, la tabla adelgazaria mientras la base de datos sigue engordando.
     * <p>
     * LONGVARCHAR lo mapea a {@code text}, que es lo que se quiere: un JSON.
     */
    @JdbcTypeCode(SqlTypes.LONGVARCHAR)
    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private OutboxStatus status;

    @Column(nullable = false)
    private Integer attempts;

    @Column(nullable = false)
    private Integer maxAttempts;

    /**
     * Numero de secuencia monotono, asignado por la base de datos.
     * <p>
     * Lo pone la base de datos y no la aplicacion: con varias replicas escribiendo a
     * la vez, es el unico sitio donde el contador es de verdad unico y creciente.
     * Ordenar por createdAt no vale, porque un bulkCreate genera N mensajes con el
     * mismo instante y el orden entre ellos queda al azar.
     */
    @Generated(event = EventType.INSERT)
    @Column(name = "sequence_number", nullable = false, insertable = false, updatable = false)
    private Long sequenceNumber;

    @Column(nullable = false)
    private Instant createdAt;

    private Instant nextAttemptAt;

    private Instant publishedAt;

    @Column(length = 1000)
    private String lastError;

    /**
     * Contexto de traza W3C de la operacion que genero el evento.
     * <p>
     * El outbox parte la traza en dos por construccion: el mensaje se escribe dentro de
     * la peticion y se publica despues, desde el hilo del scheduler. Guardarlo aqui es
     * lo unico que permite que la publicacion cuelgue de la operacion que la origino y
     * no de un span suelto del planificador.
     */
    @Column(length = 64)
    private String traceParent;

    @Column(length = 512)
    private String traceState;
}
