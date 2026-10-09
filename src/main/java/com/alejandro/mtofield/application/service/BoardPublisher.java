package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.application.dto.BoardSnapshot;

import java.util.Set;
import java.util.UUID;

/**
 * A donde van los tableros: los observadores de {@code WatchPossessionBoard} de esta replica. Lo
 * implementa el registro de observadores de la capa gRPC.
 */
public interface BoardPublisher {

    boolean hasWatchers(UUID possessionId);

    Set<UUID> watchedPossessions();

    void publish(BoardSnapshot board);

    /** La posesion se ha cerrado: el ultimo tablero y fin normal para todos sus observadores. */
    void completeAll(BoardSnapshot last);
}
