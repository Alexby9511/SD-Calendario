# Arquitectura Ray — SD Calendario

**Versión Ray 1** · 2026-11-20

Esta adaptación sustituye Raft por Ray para el prototipo Fase 1. Conserva el
protocolo compartido, la autoridad por rol y la máquina de estados determinista
del diseño v3 congelado.

## Responsabilidad

Ray proporciona direccionamiento a actores, ejecución distribuida, un runtime
de tareas y el reinicio/reubicación básico gestionado por el clúster. Nuestra
aplicación conserva la lógica de dominio, la validación estructural, la dedupe,
la firma Ed25519 y el estado autoritativo.

## Modelo

Un actor `CoordinatorState` es la autoridad de escritura. Recibe comandos
firmados y aplica una única FSM determinista. El estado vive en memoria; el
snapshot permite exportarlo y recuperarlo durante el arranque, pero no hay
replicación transitiva ni consenso robusto sin componentes adicionales.

La API expone el comando autoritativo y lecturas Ray para disponibilidad,
conflictos y sincronización (`/v1/commands`, `/v1/availability`,
`/v1/conflicts` y `/v1/sync`). La outbox del cliente conserva el `msg_id` y
avanza un cursor solo después de aplicar una página contigua.

## Garantías

- Un comando válido ocupa un índice aplicado una sola vez.
- `msg_id` deduplica reintentos con el mismo resultado.
- No se usa `now()`, `random()` ni conversiones de zona horaria en `apply`.
- Los payloads no admiten números en coma flotante.
- `replay ≡ snapshot`: construir la FSM desde el snapshot y desde el log
  determinista produce el mismo estado observable.

## Límites

Este diseño no ofrece linealizabilidad completa ante particiones ni tolerancia
robusta a reinicios del actor. Si el coordinador muere, debe crearse otro actor
desde un snapshot persistido. El actor no debe considerarse durable por sí solo.
