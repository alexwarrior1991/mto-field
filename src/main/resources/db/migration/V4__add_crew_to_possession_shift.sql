-- La dotacion y los vehiculos de cada turno de una posesion (docs/03-database.md, fase 4).
--
-- Desde que mto-maintenance lleva la dotacion de mto-resources en el turno (jefe, supervisor,
-- linemen con su usuario de Keycloak) y sus vehiculos, un Join se liga por la persona y no solo por
-- el grupo del equipo: quien esta en la dotacion del turno se une a el aunque su token no lleve el
-- grupo, y un suplente se une sin tocar el realm. Se copian al abrir la posesion, como el codigo del
-- equipo; un Join que no encuentra a la persona relee el turno una vez (un suplente anadido despues).
--
-- Las vias y el paquete de ejecucion son los del turno: la posicion de un vehiculo que manda el
-- dispositivo dice su via, o la unica del turno.
--
-- Vacio en los turnos de una posesion abierta antes de esta version y en los de un turno sin
-- dotacion: entonces decide el grupo, como hasta ahora.
ALTER TABLE possession_shift
    ADD COLUMN execution_package_id bigint,
    ADD COLUMN track_ids            bigint[]       NOT NULL DEFAULT '{}',
    ADD COLUMN vehicle_ids          uuid[]         NOT NULL DEFAULT '{}',
    ADD COLUMN crew_usernames       varchar(100)[] NOT NULL DEFAULT '{}';
