# 00 · Project overview

## Purpose

`mto-field` is the live console of a night **track possession**: the blocking of a track so that
one or more maintenance teams can work on the catenary. `mto-maintenance` plans and records those
nights (shifts, tasks, defects, materials); `mto-field` keeps the channel open while the night
lasts, between the devices of the teams on the track and the person in charge of the possession:

- which teams are **connected**, at which kp, with how much battery, and since when they have been
  silent (`CONNECTED` / `STALE` / `DISCONNECTED`);
- which **tasks** each team starts and completes, passed on to `mto-maintenance` so the shift
  record stays the single record;
- the **supervisor's commands**: a change of the possession window, a message, and above all the
  **evacuation order**, with a nominal acknowledgement from every team and a board that shows who
  has acknowledged, who has not, and who will only receive it when their device reconnects;
- the **clear-of-track** of each team, which is what allows the possession to be closed without
  forcing it.

It is informational: it controls neither the voltage nor any SCADA. What it guarantees is that a
command issued is never lost and never duplicated for a device, however its connection behaves,
and that the supervisor sees the truth of the acknowledgements, not what was sent.

## Why gRPC

It is a practice project of gRPC, and each kind of call has a reason to be there:

| Call | RPC | Why that kind |
|---|---|---|
| Unary | `OpenPossession`, `ClosePossession`, `IssueCommand` | A request with one answer; `IssueCommand` carries an idempotency key because a retried evacuation must not become two |
| Server streaming | `WatchPossessionBoard` | A board where only the latest state matters: conflated, a slow watcher skips versions |
| Bidirectional streaming | `TeamChannel` | One stream per device for the whole night: events up, commands down, with resumption and in-band results |
| Client streaming | `SyncBufferedEvents` | The backlog a device accumulated without coverage, uploaded in order outside the live channel and answered with one `SyncResult` |

## What lives elsewhere, on purpose

| Concern | Owner | How this service uses it |
|---|---|---|
| Shifts, tasks, teams, defects, materials | `mto-maintenance` | Called through its REST API with the service account `mto-field-svc`; a possession stores the shift ids and a snapshot of the team; a `TaskStarted` / `TaskCompleted` is passed on from a per-device work queue, reconciled when the answer was lost and retried while `mto-maintenance` does not answer |
| Materials and the warehouse | `mto-stock` | Not touched: what a task consumed travels inside the `TaskCompleted` to `mto-maintenance` |
| Users, roles, tokens | Keycloak (`mto-platform`) | Resource server; permissions are client roles of `mto-field-api` |
| Public routing | `mto-gateway` | **Not involved**: the gateway does not proxy gRPC, clients reach the gRPC port directly |

## Sources of the domain

The night possession of the OCS maintenance plan that `mto-maintenance` models (the 21:00–05:00
window, partial or full possession, the teams and their blocking disconnectors), and what the
supervisor of a possession does by phone and radio today: count the teams in, pass the orders,
collect each team's "clear of track" before handing the track back. The service models the
channel, not the work; the work stays in `mto-maintenance`.
