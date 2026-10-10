package com.alejandro.mtofield.application.service;

/**
 * El vigilante de acuses: un desalojo con equipos sin acusar pasado el plazo se cuenta hacia fuera
 * ({@code possession.evacuation-unacknowledged}), una sola vez por orden aunque haya varias
 * replicas, porque la marca la pone la base con un UPDATE condicional. Corre programado
 * ({@code app.field.evacuation.*}) y a mano en los tests.
 */
public interface EvacuationAckWatchdog {

    /** Una pasada: mira los desalojos vencidos que nadie miro y devuelve cuantos avisos publico. */
    int check();
}
