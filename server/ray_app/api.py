from typing import Any, Dict, List, Optional

try:
    from fastapi import FastAPI, HTTPException
except ImportError:
    FastAPI = None

try:
    import ray
except ImportError:
    ray = None

from .coordinator import Coordinator, CoordinatorActor


local_coordinator = Coordinator()
coordinator_actor: Optional[Any] = None


def get_coordinator():
    global coordinator_actor
    if ray is not None and ray.is_initialized():
        if coordinator_actor is None:
            coordinator_actor = CoordinatorActor.options(
                name="sdcal-coordinator", get_if_exists=True
            ).remote()
        return coordinator_actor
    return local_coordinator


def call_coordinator(method: str, *args):
    coordinator = get_coordinator()
    if ray is not None and isinstance(coordinator, ray.actor.ActorHandle):
        return getattr(coordinator, method).remote(*args)
    return getattr(coordinator, method)(*args)


def resolve(result: Any) -> Any:
    if ray is not None and isinstance(result, ray.ObjectRef):
        result = ray.get(result)
    return result


app = FastAPI(title="SD Calendario sobre Ray") if FastAPI is not None else None


if app is not None:
    @app.post("/v1/commands")
    @app.post("/v1/command")
    async def apply_command(command: Dict[str, Any]):
        result = resolve(call_coordinator("apply", command))
        code = int(result["code"])
        if code != 200:
            raise HTTPException(status_code=code, detail=result)
        return result

    @app.get("/v1/status")
    async def status():
        return resolve(call_coordinator("status"))

    @app.get("/v1/kv/{key}")
    async def kv(key: str):
        snapshot = resolve(call_coordinator("snapshot"))
        if key == "*":
            return snapshot
        event = snapshot["events"].get(key)
        if event is None:
            return {"key": key, "value": None, "found": False}
        return {"key": key, "value": event, "found": True}

    @app.get("/v1/availability")
    async def availability(
        users: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ):
        selected = [user for user in users.split(",") if user] if users else None
        return resolve(call_coordinator("availability", selected, start, end))

    @app.get("/v1/conflicts")
    async def conflicts(
        user: str,
        start: str,
        end: str,
        exclude_uid: Optional[str] = None,
    ):
        return resolve(call_coordinator("conflicts", user, start, end, exclude_uid))

    @app.get("/v1/sync")
    async def sync(since: int = 0, limit: int = 100):
        try:
            return resolve(call_coordinator("sync", since, limit))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"reason": str(exc)}) from exc

    @app.get("/health")
    async def health():
        return {"status": "ok"}
