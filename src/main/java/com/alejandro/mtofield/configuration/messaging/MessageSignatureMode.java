package com.alejandro.mtofield.configuration.messaging;

/**
 * How strict this service is about the signature that travels with each message.
 */
public enum MessageSignatureMode {

    /** No se comprueba nada. Solo para depurar o para un entorno donde el emisor aún no firma. */
    DISABLED,

    /**
     * Se comprueba cuando se puede, y lo que no se pueda comprobar se acepta.
     *
     * <p>Es el valor por defecto porque una firma que no cuadra sigue rechazándose, pero un mensaje
     * sin firmar —uno publicado a mano para probar, o uno de un emisor anterior a la firma— no tumba
     * el consumo. Un servicio que de pronto manda todo a la DLQ es peor que uno que comprueba lo que
     * le llega firmado.</p>
     */
    OPTIONAL,

    /**
     * Todo mensaje tiene que llegar firmado y con una firma comprobable.
     *
     * <p>Es lo que hay que poner en producción una vez el secreto está repartido. Con esto, un
     * mensaje sin firma o firmado con un algoritmo que este servicio no puede recalcular va a la
     * DLQ.</p>
     */
    REQUIRED
}
