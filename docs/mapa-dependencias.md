# Mapa de responsabilidades: Ray frente a SD Calendario

| Aspecto | Ray | Nosotros |
| --- | --- | --- |
| Distribución de procesos | Ejecución y direccionamiento de actores/tasks | Topología, API y bootstrap |
| Coordinación | Scheduling y ubicación | Actor coordinador y admisión de comandos |
| Estado | Memoria del actor | FSM, snapshot, persistencia explícita |
| Reintentos | Reenvía RPC/tasks según la política | Deduplicación por `msg_id` e idempotencia |
| Consistencia | Sin garantía de dominio | Secuencia, validación y reglas por rol |
| Serialización | Transporte de objetos | JCS, sobre firmado Ed25519 |
| Disponibilidad | Liveness del proceso/runtimes | Last-value-wins por `pub_seq` |
| Recuperación | Puede reubicar procesos | Reconstrucción desde snapshot/log persistido |
| Concurrencia | Scheduling | Registros de `seen` y estado autoritativo |

Ray **no declara automáticamente recursos de dominio** ni convierte tasks
reintentadas en operaciones idempotentes. El protocolo debe seguir siendo
correcto aunque Ray entregue el mismo RPC más de una vez.
