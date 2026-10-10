# Realm de Keycloak para `mto-field`

Estos ficheros son la definición de lo que `mto-field` necesita en el servidor de identidad. Se
versionan para que la configuración de Keycloak se revise en pull request como cualquier otro
cambio, y para que los entornos no diverjan por lo que alguien pinchó un día en la consola.

La parcial no contiene ningún secreto: el de la cuenta de servicio `mto-field-svc` lo genera
Keycloak al importar y se lee desde la consola. Solo el fichero de desarrollo lo fija a un valor
conocido, para que el stack local arranque sin copiar nada a mano.

## El realm es compartido

`mto-field` vive en el **mismo realm** (`mto`) que `mto-maintenance`, `mto-stock`,
`mto-configuration`, `mto-users` y `mto-notification`. Se separa por entorno y no por aplicación,
porque las personas son las mismas; el aislamiento entre servicios lo dan los roles de cliente y la
audiencia de los tokens.

El realm base lo crea y lo posee
[`mto-platform`](https://github.com/alexwarrior1991/mto-platform), y cada servicio aporta desde su
propio repositorio lo suyo. Este trae dos ficheros:

| Fichero | Qué aporta |
|---|---|
| `mto-field-partial-import.json` | Los clientes `mto-field-api` y `mto-field-svc`, los permisos y los perfiles `mto-field-*`. Vale para cualquier entorno. |
| `mto-field-dev.json` | Dos técnicos y un responsable de desarrollo, y el secreto fijo de la cuenta de servicio (`mto-field-svc-secret`, el mismo que `MTO_FIELD_SERVICE_CLIENT_SECRET` en `mto-platform/.env.example`). Aparte a propósito, para poder aplicar lo anterior en un entorno desplegado sin arrastrarlos. |

Los aplica `mto-platform/keycloak/apply-partials.sh`, que fija el orden: primero las parciales que
crean los clientes (esta va después de la de `mto-users`), después `mto-ops-cross-service.json`,
que los nombra.

## Qué hay dentro

### Clientes

| Cliente | Tipo | Para qué |
|---|---|---|
| `mto-field-api` | Confidencial, sin flujos | Declara los permisos como roles de cliente y es la **audiencia** de los tokens que valida este servicio, por gRPC y por HTTP (Actuator). |
| `mto-field-svc` | Cuenta de servicio (`client_credentials`) | Con ella `mto-field` llama a `mto-maintenance` para leer los turnos de una posesión y contar el inicio y el fin de cada tarea. Lleva un *audience mapper* hacia `mto-maintenance-api`. |

`mto-field` no declara ningún cliente de navegador propio: los dispositivos y el simulador piden
el token al `mto-frontend` del realm base de `mto-platform`, que lleva un *audience mapper* hacia
`mto-field-api` (y, en el realm local, el *password grant* abierto). El gateway no participa: los
clientes llegan directamente al puerto gRPC.

### Permisos y perfiles

Los **permisos** son roles de cliente de `mto-field-api` y son lo que comprueba el código
(`SecurityRoles`, RPC a RPC con `@PreAuthorize`: en gRPC no hay verbos ni rutas). Los **perfiles**
son roles de realm compuestos que los agrupan, y son lo que se asigna a las personas.

| Permiso | Concede |
|---|---|
| `field-team` | `TeamChannel` y `SyncBufferedEvents`: unirse a una posesión abierta, latidos, inicio y fin de tareas, acuses y salida de vía |
| `field-supervise` | `OpenPossession`, `ClosePossession`, `IssueCommand` y `WatchPossessionBoard` |
| `ops-metrics` | Lectura de los endpoints de Actuator |
| `ops-write` | Operaciones de Actuator que modifican estado |

| Perfil | Agrupa |
|---|---|
| `mto-field-technician` | `field-team` |
| `mto-field-supervisor` | `field-team`, `field-supervise`, y de `mto-notification-api`: `notification-inbox`, `notification-activity-read` |

Son los del enunciado del servicio más, desde la fase 5, lo que el responsable necesita para
recibir lo que `mto-field` publica: la campana (`notification-inbox`) y el registro de actividad
(`notification-activity-read`) de `mto-notification`, en los dos frontales. El técnico sigue sin
nada de notificaciones ni de `mto-maintenance-api`: un dispositivo solo habla con `mto-field`, y lo
que `mto-field` necesita de `mto-maintenance` lo hace con su cuenta de servicio. Si en algún
momento el responsable tiene que listar turnos en el backoffice, se le añade `maintenance-read`
aquí, en una línea. La parcial de `mto-notification` se aplica antes que esta
(`mto-platform/keycloak/apply-partials.sh`), que es lo que permite nombrar sus roles.

`ops-metrics` y `ops-write` los agrupa, junto con los de los demás servicios, el perfil `mto-ops` de
`mto-platform/keycloak/mto-ops-cross-service.json`.

### Los grupos: el equipo de cada persona

El rol dice qué puede hacer un token; el **grupo** dice por quién. La parcial declara los grupos
`EQ-NORTE` y `EQ-SUR`, y un grupo se llama como el `code` del equipo en `mto-maintenance`: al
unirse a un turno (`TeamChannel` y `SyncBufferedEvents`), `mto-field` exige que el código del
equipo del turno esté entre los grupos del token, salvo que lleve `field-supervise`
(`app.field.team-binding.enabled`, encendido por defecto; `.claim`, `groups`). Los grupos viajan
en el access token por el mapper `grupos` (*group membership*, sin la ruta completa) del cliente de
login `mto-frontend`, que vive en `mto-realm.json` y `mto-realm-local.json` de `mto-platform`. En
desarrollo, `campo.tecnico1` está en `EQ-NORTE` y `campo.tecnico2` en `EQ-SUR`. Un equipo nuevo en
`mto-maintenance` es un grupo nuevo aquí, con el mismo código.

## Cómo cargarlo

Lo normal es no cargar nada a mano: `mto-platform/keycloak/apply-partials.sh` aplica las parciales
de todos los servicios en orden sobre el Keycloak del `compose.yaml` de la plataforma. Para un
Keycloak cualquiera:

- Consola: *Realm settings → Action → Partial import*, con la estrategia *Skip* si el realm ya tenía
  algo.
- API de administración: `POST /admin/realms/mto/partialImport` con el fichero y
  `"ifResourceExists": "OVERWRITE"` (lo que hace el script).

## Después de importar

1. **Conceder a la cuenta de servicio sus roles en `mto-maintenance-api`**: `maintenance-read` y
   `maintenance-write` (*Clients → mto-field-svc → Service accounts roles*). No van en la parcial a
   propósito: una importación parcial no asigna roles a usuarios de servicio, y la parcial de un
   servicio no debería decidir por sí sola qué puede tocar en el de otro. En local lo hace el paso 8
   de `apply-partials.sh`. Sin ellos, cada llamada a `mto-maintenance` responde 403 y el evento se
   queda `FAILED`.
2. Copiar el secreto de `mto-field-svc` a `KEYCLOAK_SERVICE_CLIENT_SECRET` (en `mto-platform`,
   `MTO_FIELD_SERVICE_CLIENT_SECRET` en `.env`).
3. Crear las personas y asignarles `mto-field-technician` o `mto-field-supervisor` (en local los trae
   `mto-field-dev.json`).

## Pedir un token a mano

En el realm local, el `password grant` de `mto-frontend` está abierto:

```bash
curl -s -X POST http://auth.mto.local:8082/realms/mto/protocol/openid-connect/token \
  -d grant_type=password -d client_id=mto-frontend \
  -d username=campo.responsable -d password=local \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
```

El token lleva `aud: mto-field-api` y, en `resource_access.mto-field-api.roles`, los permisos del
perfil. `docs/grpc/field-api.md` lo usa con `grpcurl`.

## Comprobar que quedó bien

`mto-platform/scripts/check_realm_consistency.py` comprueba, con los ocho repositorios como
hermanos, que `mto-field-api` está en la lista de API, que todo cliente de login emite audiencia
hacia él, que `mto-ops` cubre sus `ops-*` y que ningún compuesto nombra un cliente que aún no
existe. `scripts/check_applied_realm.py` hace lo mismo contra un Keycloak de verdad tras
`apply-partials.sh`, incluidos los roles de la cuenta de servicio.
