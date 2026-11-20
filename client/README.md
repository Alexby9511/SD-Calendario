# Cliente local-first

El prototipo incluye `outbox.py`, una cola persistente SQLite. La aplicación
Wails v3 puede mantener calendario personal y overlays locales sin coordinar
el estado autoritativo.

```python
from client.outbox import Outbox

outbox = Outbox("calendar.db")
outbox.enqueue("01J...", "alice:01J...", 1, {"title": "Reunión"})
for item in outbox.pending():
    print(item)
```

Cada envío conserva el `msg_id` y reintenta el mismo comando. El servidor no
duplica el efecto gracias a la dedupe FSM. Si llega `409`, el cliente debe
refrescar y reemitir siguiendo la secuencia del evento.
