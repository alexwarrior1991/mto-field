package com.alejandro.mtofield.infrastructure.messaging.outbox;

/*
PENDING     -> evento pendiente de publicar
IN_PROGRESS -> evento reclamado por un relay que esta intentando publicarlo
PUBLISHED   -> evento publicado y CONFIRMADO por RabbitMQ
FAILED      -> evento que ha superado el numero maximo de intentos
*/
public enum OutboxStatus {
    PENDING,
    IN_PROGRESS,
    PUBLISHED,
    FAILED
}
