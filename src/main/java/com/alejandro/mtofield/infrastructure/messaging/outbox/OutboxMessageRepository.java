package com.alejandro.mtofield.infrastructure.messaging.outbox;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, UUID> {

    /**
     * Reclama en exclusiva un lote de mensajes publicables.
     * <p>
     * {@code FOR UPDATE SKIP LOCKED} es lo que hace que el relay sea seguro con varias
     * replicas: cada instancia se lleva un lote DISJUNTO en lugar de leer todas las
     * mismas filas y publicar el mismo evento N veces. {@code SKIP LOCKED} ademas evita
     * que las replicas se bloqueen entre si; simplemente pasan a las filas libres.
     * <p>
     * Se recogen tambien los {@code IN_PROGRESS} cuya visibilidad ha expirado: son
     * mensajes que reclamo una replica que murio antes de cerrarlos, y sin esto se
     * quedarian atascados para siempre.
     * <p>
     * Lo que devuelve esta consulta todavia NO esta reclamado: quien la llama cuenta el
     * intento y escribe la visibilidad en la misma transaccion ({@code OutboxRelayService}).
     * <p>
     * {@code coalesce(next_attempt_at, created_at)} es defensivo: la columna admite
     * nulos y una fila con null jamas entraria por una comparacion directa.
     */
    @Query(value = """
            select * from {h-schema}outbox_message
            where status in ('PENDING', 'IN_PROGRESS')
              and coalesce(next_attempt_at, created_at) <= :now
            order by sequence_number
            limit :batchSize
            for update skip locked
            """, nativeQuery = true)
    List<OutboxMessage> claimBatch(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * Como {@link #claimBatch}, pero reteniendo los mensajes cuyo agregado tenga otro
     * anterior todavia sin publicar.
     * <p>
     * Sin esta retencion, un mensaje que falla y se reprograma deja pasar por delante
     * al siguiente del MISMO agregado, y el consumidor acaba aplicando el cambio viejo
     * encima del nuevo: el dato maestro queda mal y nadie se entera. Es un fallo que
     * antes se disimulaba, porque el mensaje que no se confirmaba se daba por
     * publicado y se perdia, de modo que solo llegaba el nuevo y el resultado final
     * salia bien de casualidad.
     * <p>
     * Solo retienen los PENDING y los IN_PROGRESS, que son los que todavia van a
     * llegar. Un FAILED no bloquea: ya no se va a publicar nunca por si mismo, y
     * dejarlo retener al resto pararia el agregado entero de forma indefinida.
     */
    @Query(value = """
            select * from {h-schema}outbox_message o
            where o.status in ('PENDING', 'IN_PROGRESS')
              and coalesce(o.next_attempt_at, o.created_at) <= :now
              and not exists (
                  select 1 from {h-schema}outbox_message anterior
                  where anterior.aggregate_type = o.aggregate_type
                    and anterior.aggregate_id = o.aggregate_id
                    and anterior.status in ('PENDING', 'IN_PROGRESS')
                    and anterior.sequence_number < o.sequence_number
              )
            order by o.sequence_number
            limit :batchSize
            for update skip locked
            """, nativeQuery = true)
    List<OutboxMessage> claimBatchInOrder(@Param("now") Instant now, @Param("batchSize") int batchSize);

    /**
     * Renueva un reclamo: alarga su visibilidad SOLO si la fila sigue siendo de quien
     * la reclamo.
     * <p>
     * {@code next_attempt_at} hace de testigo del reclamo. Cada reclamo escribe ahi un
     * instante estrictamente posterior al que habia (solo se reclama lo que ya ha
     * vencido), asi que si otra replica se ha llevado la fila, o si ya esta cerrada,
     * la condicion no casa y la sentencia no cambia nada. La respuesta es el numero de
     * filas, nunca una lectura previa.
     *
     * @return 1 si el reclamo seguia siendo de quien pregunta, 0 si lo ha perdido
     */
    @Modifying
    @Query(value = """
            update {h-schema}outbox_message
            set next_attempt_at = :newClaimedUntil
            where id = :id
              and status = 'IN_PROGRESS'
              and next_attempt_at = :claimedUntil
            """, nativeQuery = true)
    int renewClaim(@Param("id") UUID id,
                   @Param("claimedUntil") Instant claimedUntil,
                   @Param("newClaimedUntil") Instant newClaimedUntil);

    /** Devuelve a PENDING, con su siguiente intento aplazado, un mensaje cuyo reclamo sigue vigente. */
    @Modifying
    @Query(value = """
            update {h-schema}outbox_message
            set status = 'PENDING',
                next_attempt_at = :nextAttemptAt,
                last_error = :lastError
            where id = :id
              and status = 'IN_PROGRESS'
              and next_attempt_at = :claimedUntil
            """, nativeQuery = true)
    int rescheduleClaimed(@Param("id") UUID id,
                          @Param("claimedUntil") Instant claimedUntil,
                          @Param("nextAttemptAt") Instant nextAttemptAt,
                          @Param("lastError") String lastError);

    /** Deja en FAILED un mensaje cuyo reclamo sigue vigente y que ya no tiene mas intentos. */
    @Modifying
    @Query(value = """
            update {h-schema}outbox_message
            set status = 'FAILED',
                next_attempt_at = null,
                last_error = :lastError
            where id = :id
              and status = 'IN_PROGRESS'
              and next_attempt_at = :claimedUntil
            """, nativeQuery = true)
    int failClaimed(@Param("id") UUID id,
                    @Param("claimedUntil") Instant claimedUntil,
                    @Param("lastError") String lastError);

    /**
     * Suelta un reclamo que no se ha llegado a intentar: vuelve a PENDING, disponible
     * ya, y recupera el intento que se le conto al reclamarlo.
     */
    @Modifying
    @Query(value = """
            update {h-schema}outbox_message
            set status = 'PENDING',
                attempts = greatest(attempts - 1, 0),
                next_attempt_at = :now
            where id = :id
              and status = 'IN_PROGRESS'
              and next_attempt_at = :claimedUntil
            """, nativeQuery = true)
    int releaseClaimed(@Param("id") UUID id,
                       @Param("claimedUntil") Instant claimedUntil,
                       @Param("now") Instant now);

    long countByStatus(OutboxStatus status);

    @Query("select min(o.createdAt) from OutboxMessage o where o.status = :status")
    Instant findOldestCreatedAt(@Param("status") OutboxStatus status);

    List<OutboxMessage> findByStatusOrderByCreatedAtAsc(OutboxStatus status, Limit limit);

    /**
     * Borra un lote de mensajes ya publicados y suficientemente antiguos.
     * <p>
     * Por lotes y no de una sentencia: un DELETE de millones de filas mantiene una
     * transaccion larguisima, hincha el WAL y bloquea el vacuum de la tabla. El
     * subselect con LIMIT acota cada pasada.
     */
    @Modifying
    @Query(value = """
            delete from {h-schema}outbox_message
            where id in (
                select id from {h-schema}outbox_message
                where status = 'PUBLISHED'
                  and published_at < :threshold
                order by published_at
                limit :batchSize
            )
            """, nativeQuery = true)
    int deletePublishedOlderThan(@Param("threshold") Instant threshold, @Param("batchSize") int batchSize);
}
