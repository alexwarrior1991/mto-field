package com.alejandro.mtofield.application.event;

import java.util.UUID;

/** La posesion ha quedado cerrada en la base; tras el commit, los streams terminan y el tablero se despide. */
public record PossessionClosed(UUID possessionId) {
}
