package com.alejandro.mtofield.infrastructure.messaging.outbox;

import com.alejandro.mtofield.application.dto.messaging.MessageActor;
import com.alejandro.mtofield.configuration.security.CurrentUserService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;

import java.util.Optional;

/**
 * Lo que un mensaje sabe de la operacion que lo genero: quien la pidio y con que identificador.
 *
 * <p>Se lee en el hilo que escribe el outbox, que es el que todavia tiene el contexto: el
 * {@code SecurityContext} que el interceptor de gRPC fija alrededor de cada callback (quien abre o
 * cierra la posesion y emite el desalojo es el responsable; quien acusa o sale de la via es el
 * tecnico del dispositivo) y la correlacion que el gancho fija con {@link MessagingCorrelation}.
 * El vigilante de acuses y cualquier hilo propio no tienen usuario y se dicen como {@code SYSTEM}.</p>
 */
@RequiredArgsConstructor
public class MessageContextResolver {

    private final CurrentUserService currentUserService;

    /**
     * Nunca nulo: lo que no tiene usuario se dice como {@code SYSTEM} en vez de callarse, para que
     * el consumidor distinga «el emisor no lo dice» de «el emisor dice que fue un proceso».
     */
    public MessageActor currentActor() {
        Optional<Authentication> authentication = currentUserService.getAuthentication();

        if (authentication.isEmpty()) {
            return MessageActor.system();
        }

        String username = currentUserService.getUsername().orElseGet(() -> authentication.get().getName());
        String id = currentUserService.getUserId().orElse(null);

        return MessageActor.of(id, username);
    }

    /** El de la operacion en curso ({@link MessagingCorrelation}), o nulo. */
    public String currentCorrelationId() {
        return MessagingCorrelation.current();
    }
}
