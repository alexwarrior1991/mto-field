package com.alejandro.mtofield.application.event;

import java.util.UUID;

/** El despachador acaba de escribir ordenes en los streams de la posesion: lo que se les ha enviado ha cambiado. */
public record CommandsFannedOut(UUID possessionId) {
}
