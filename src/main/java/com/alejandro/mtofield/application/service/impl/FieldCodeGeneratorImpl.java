package com.alejandro.mtofield.application.service.impl;

import com.alejandro.mtofield.application.service.FieldCodeGenerator;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Service;

/**
 * Codigos con secuencia de base de datos: unicos aunque dos responsables abran a la vez, sin
 * bloqueo ni MAX. Una secuencia no se deshace con el rollback, asi que puede haber huecos en los
 * codigos; lo que no puede tener huecos es la secuencia de ordenes de una posesion, y esa no sale
 * de aqui sino del contador de su fila.
 */
@Service
class FieldCodeGeneratorImpl implements FieldCodeGenerator {

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public String nextPossessionCode() {
        Number value = (Number) entityManager.createNativeQuery("select nextval('possession_code_seq')").getSingleResult();
        return "PO-" + String.format("%06d", value.longValue());
    }
}
