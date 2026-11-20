# AGENTS.md — SD Calendario

Calendario distribuido académico: servidor Python con Ray + cliente local-first.

## Diseño

- [`docs/arquitectura.md`](docs/arquitectura.md) es la fuente de verdad y está **congelado**: las dudas se resuelven en código y tests, no en más rondas de diseño ni en reescrituras del documento. Si se edita, citar versión + `git hash-object docs/arquitectura.md`.
- Decisiones cerradas que **no** se deben reabrir: sin CRDT en el MVP, FSM determinista, `pkg/protocol` compartido, cliente local-first con outbox y despliegue manual con Dockerfiles.
- Reglas duras de la FSM al implementar hitos futuros: prohibido `now()`/`random()` y conversión de zonas horarias en `apply`; iteración de mapas ordenada para snapshots; sin `float`; propiedad obligatoria *replay ≡ snapshot* (§7.2 del diseño).

## Comandos

```bash
go test ./...            # todo: protocolo, FSM, API HTTP y clúster de 3 nodos
go test ./pkg/protocol/ -run TestIsULID   # un test concreto
./deploy/demo.sh         # head Ray, dos workers y API con docker run
```

- El toolchain no está en PATH en esta máquina: `export PATH="$HOME/.local/go-root/usr/lib/go-1.22/bin:$PATH"` (Go 1.22 extraído de un .deb de apt; el cliente Wails necesitará el tarball 1.24+ aparte).

- El módulo Python raíz (`pyproject.toml`) cubre `pkg/`, `server/` y `client/`.

## Estado del código

- `client/` contiene la outbox SQLite; la aplicación Wails sigue siendo futura.
- La API actual expone `/v1/commands`, `/v1/availability`, `/v1/conflicts`, `/v1/sync`, `/v1/kv/{key}` y `/v1/status`.
- El actor `CoordinatorState` es la autoridad única; los workers Ray no mantienen estado de dominio independiente.
- `pkg/protocol/` (sobre, JCS RFC 8785, Ed25519) lo importarán ambos binarios: nunca duplicar firma/serialización canónica en el cliente.
- El formato de los bytes firmados debe permanecer canónico; no duplicar serialización fuera de `pkg/protocol`.

## Convenciones

- Documentación y comentarios de salida de demo en español; doc comments y nombres de código en inglés (patrón existente en todo el repo).
- Sin CI ni lint configurados; comprobar con `go fmt`/`go vet ./...` antes de terminar.
- Generados a ignorar: `/data/`, `*.db*`, `/sdcald` (binario local de la demo).
