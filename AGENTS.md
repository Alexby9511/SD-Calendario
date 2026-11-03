# AGENTS.md — SD Calendario

Calendario distribuido académico: servidor Go con Raft (`hashicorp/raft`) + cliente Wails v3. Diseño v3 **congelado**.

## Diseño

- [`docs/arquitectura.md`](docs/arquitectura.md) es la fuente de verdad y está **congelado**: las dudas se resuelven en código y tests, no en más rondas de diseño ni en reescrituras del documento. Si se edita, citar versión + `git hash-object docs/arquitectura.md`.
- Decisiones cerradas que **no** se deben reabrir: sin CRDT en el MVP, FSM determinista, `pkg/protocol` compartido, cliente local-first con outbox.
- Reglas duras de la FSM al implementar hitos futuros: prohibido `now()`/`random()` y conversión de zonas horarias en `apply`; iteración de mapas ordenada para snapshots; sin `float`; propiedad obligatoria *replay ≡ snapshot* (§7.2 del diseño).

## Comandos

```bash
go test ./...            # todo: protocolo, FSM y clúster de 3 nodos
go test ./pkg/protocol/ -run TestIsULID   # un test concreto
./deploy/demo.sh         # clúster 3 nodos en Docker (8081-8083) + fallo del líder
```

- El módulo raíz (`go.mod` en la raíz) cubre solo `pkg/` + `server/`. El import path es `github.com/Alexby9511/SD-Calendario` (sin espacio, con mayúsculas), distinto del nombre del directorio.
- El test de clúster (`server/internal/raftnode/cluster_test.go`) usa transports en memoria: sin puertos ni Docker, pero tarda (~timeout de elección).

## Estado del código

- `client/` **aún no tiene código**: solo `client/README.md` con lo decidido (Wails v3, SQLite, outbox). No compila en este entorno (falta `libwebkit2gtk-4.1-dev`; exige Go 1.24 + Node 22), y será andamio con `wails3 init`, no parte de este `go.mod`.
- La API actual (hito 1) es `/v1/command` (singular), `/v1/kv/{key}` y `/v1/status` con una FSM de eco; **no** coincide todavía con la del diseño (`/v1/commands`, etc.). Ante conflicto diseño vs. código durante hito 1, el código es lo que se ejecuta.
- Cliente propone por cualquier nodo y el follower reenvía al líder interno (`server/internal/api`), con lookup local **advisory** (un follower puede ir por detrás).
- `pkg/protocol/` (sobre, JCS RFC 8785, Ed25519) lo importarán ambos binarios: nunca duplicar firma/serialización canónica en el cliente.

## Convenciones

- Documentación y comentarios de salida de demo en español; doc comments y nombres de código en inglés (patrón existente en todo el repo).
- Sin CI ni lint configurados; comprobar con `go fmt`/`go vet ./...` antes de terminar.
- Generados a ignorar: `/data/`, `*.db*`, `/sdcald` (binario local de la demo).
