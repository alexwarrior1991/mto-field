package com.alejandro.mtofield.infrastructure.persistence.repository;

import com.alejandro.mtofield.infrastructure.persistence.entity.Possession;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface PossessionRepository extends JpaRepository<Possession, UUID> {

    Optional<Possession> findByCode(String code);

    /**
     * La fila de la posesion bloqueada para escribir: es la misma fila que bloquea el contador de
     * ordenes, asi que cerrar la posesion y emitir una orden se serializan entre si.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select p from Possession p where p.id = :id")
    Optional<Possession> findWithLockById(@Param("id") UUID id);

    /**
     * El siguiente numero de la secuencia descendente de una posesion ABIERTA.
     *
     * <p>Se llama dentro de la transaccion que inserta la orden. El {@code update} bloquea la fila
     * hasta el commit, de modo que dos emisores concurrentes se serializan: ninguna secuencia se
     * hace visible antes que una menor, y un rollback deshace el incremento sin dejar hueco. Con una
     * secuencia de PostgreSQL un dispositivo que reanuda en la 11 se saltaria la 10 para siempre.</p>
     *
     * @return el numero, o vacio si la posesion no existe o ya esta cerrada
     */
    @Query(value = """
            update possession
               set next_command_seq = next_command_seq + 1,
                   updated_at = now()
             where id = :possessionId
               and status = 'OPEN'
            returning next_command_seq
            """, nativeQuery = true)
    Optional<Long> nextCommandSequence(@Param("possessionId") UUID possessionId);
}
