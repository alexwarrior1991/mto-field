package com.alejandro.mtofield.infrastructure.persistence.repository;

import com.alejandro.mtofield.infrastructure.persistence.entity.DeviceBindingRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.UUID;

/** De quien es cada dispositivo, decidido en la base y en una sentencia. */
public interface DeviceBindingRepository extends JpaRepository<DeviceBindingRecord, String> {

    /**
     * Liga el dispositivo a la persona, o confirma que ya era suyo: devuelve 1 si desde ahora (o
     * desde antes) es de {@code username}, y 0 si es de otra persona en una posesion que sigue
     * abierta. Nunca leer-y-escribir: dos {@code Join} a la vez con el mismo dispositivo, en la
     * misma replica o en dos, chocan en la clave primaria, y el segundo evalua el {@code where}
     * sobre la fila ya confirmada del primero.
     *
     * <p>El dispositivo se suelta solo: si la posesion en la que se ligo ya esta cerrada, el primero
     * que se une se lo queda. La misma persona lo lleva a otra posesion cuando quiera.</p>
     */
    @Modifying
    @Query(value = """
            insert into device_binding (device_id, username, possession_id, bound_at)
            values (:deviceId, :username, :possessionId, now())
            on conflict (device_id) do update
               set username = excluded.username,
                   possession_id = excluded.possession_id,
                   bound_at = case when device_binding.username = excluded.username
                                    and device_binding.possession_id = excluded.possession_id
                                   then device_binding.bound_at else now() end
             where device_binding.username = excluded.username
                or not exists (select 1
                                 from possession held
                                where held.id = device_binding.possession_id
                                  and held.status = 'OPEN')
            """, nativeQuery = true)
    int bind(@Param("deviceId") String deviceId, @Param("username") String username, @Param("possessionId") UUID possessionId);
}
