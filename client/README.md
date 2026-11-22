# Cliente local-first (outbox)

`Outbox` conserva comandos completos en SQLite durante desconexiones. Al
reconectar, el cliente vuelve a enviar el mismo comando y `msg_id`; la FSM
deduplica reintentos ya aplicados. La credencial Bearer se entrega al enviar y
no se guarda en la base local.

```python
from client.outbox import Outbox

outbox = Outbox("calendar.db")
outbox.enqueue(command_envelope)
sent = outbox.send_pending(
    lambda command: send_to_api(command, bearer_token=token)
)
```

Un error de transporte deja el comando pendiente. Un `409` lo mueve a
conflictos para que el cliente actualice el estado y genere un comando nuevo
con la secuencia correcta. No debe reutilizarse el `msg_id` con otro contenido.

La reconciliación persiste la página recibida y su cursor en una única
transacción. El cursor permite saltar entradas privadas filtradas por el
servidor sin volver a solicitarlas:

```python
page = fetch_sync_page(since=outbox.cursor(), bearer_token=token)
outbox.reconcile(
    page["entries"],
    next_cursor=page["next"],
    snapshot=page["snapshot"],
)
```

`outbox.synced_snapshot()` devuelve la proyección privada/pública autorizada
para ese usuario. Cada página la reemplaza dentro de la misma transacción que
avanza el cursor, por lo que también elimina localmente eventos cuyo acceso ya
no está autorizado.
