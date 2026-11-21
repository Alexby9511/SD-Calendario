from typing import Any, Dict, Optional

try:
    from fastapi import FastAPI, HTTPException
except ImportError:
    FastAPI = None

try:
    import ray
except ImportError:
    ray = None

from .coordinator import Coordinator, CoordinatorActor
from .raft_cluster import NoLeaderError, RaftCluster


local_coordinator = Coordinator()
coordinator_actor: Optional[Any] = None
raft_cluster: Optional[RaftCluster] = None


def set_cluster(cluster: Optional[RaftCluster]) -> None:
    """Attach (or detach) the Raft cluster used by the API."""
    global raft_cluster
    raft_cluster = cluster


def get_cluster() -> Optional[RaftCluster]:
    return raft_cluster


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
    def apply_command(command: Dict[str, Any]):
        cluster = get_cluster()
        if cluster is not None:
            try:
                result = cluster.propose(command)
            except NoLeaderError as exc:
                raise HTTPException(
                    status_code=503, detail={"reason": str(exc)}
                ) from exc
            if result is None:
                raise HTTPException(
                    status_code=503,
                    detail={"reason": "raft: command not committed (no quorum)"},
                )
            code = int(result["code"])
            if code != 200:
                raise HTTPException(status_code=code, detail=result)
            return result

        result = resolve(call_coordinator("apply", command))
        code = int(result["code"])
        if code != 200:
            raise HTTPException(status_code=code, detail=result)
        return result

    @app.get("/v1/status")
    def status():
        cluster = get_cluster()
        if cluster is not None:
            return cluster.status()
        return resolve(call_coordinator("status"))

    @app.get("/v1/kv/{key}")
    def kv(key: str):
        cluster = get_cluster()
        snapshot = (
            cluster.snapshot()
            if cluster is not None
            else resolve(call_coordinator("snapshot"))
        )
        if key == "*":
            return snapshot
        event = snapshot["events"].get(key)
        if event is None:
            return {"key": key, "value": None, "found": False}
        return {"key": key, "value": event, "found": True}

    @app.get("/v1/availability")
    def availability(
        users: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ):
        selected = [user for user in users.split(",") if user] if users else None
        cluster = get_cluster()
        if cluster is not None:
            return cluster.availability(selected, start, end)
        return resolve(call_coordinator("availability", selected, start, end))

    @app.get("/v1/conflicts")
    def conflicts(
        user: str,
        start: str,
        end: str,
        exclude_uid: Optional[str] = None,
    ):
        cluster = get_cluster()
        if cluster is not None:
            return cluster.conflicts(user, start, end, exclude_uid)
        return resolve(call_coordinator("conflicts", user, start, end, exclude_uid))

    @app.get("/v1/sync")
    def sync(since: int = 0, limit: int = 100):
        cluster = get_cluster()
        if cluster is not None:
            try:
                return cluster.sync(since, limit)
            except ValueError as exc:
                raise HTTPException(
                    status_code=422, detail={"reason": str(exc)}
                ) from exc
        try:
            return resolve(call_coordinator("sync", since, limit))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"reason": str(exc)}) from exc

    @app.get("/health")
    def health():
        return {"status": "ok"}
