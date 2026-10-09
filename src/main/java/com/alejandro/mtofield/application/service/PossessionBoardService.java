package com.alejandro.mtofield.application.service;

import com.alejandro.mtofield.application.dto.BoardSnapshot;

import java.util.UUID;

/**
 * El tablero de cada posesion: se recalcula cuando algo cambia (una orden, un acuse, un latido, un
 * stream que se abre o se cierra) y periodicamente mientras alguien lo mira, porque la vida de
 * los equipos decae con el tiempo. El recalculo es coalescido: N cambios seguidos no son N
 * recalculos, sino uno en marcha y, como mucho, otro despues.
 */
public interface PossessionBoardService {

    /** Algo de la posesion ha cambiado: recalcular y publicar, fuera del hilo que llama. */
    void markDirty(UUID possessionId);

    /** Un tablero recien calculado, con su version nueva: lo que recibe un observador al empezar a mirar. */
    BoardSnapshot snapshot(UUID possessionId);

    /** Marca sucias las posesiones con observadores: la vida de los equipos cambia aunque nadie hable. */
    void tick();

    /** La posesion se ha cerrado: ultimo tablero, fin de los observadores y fuera lo que se guardaba de ella. */
    void possessionClosed(UUID possessionId);
}
