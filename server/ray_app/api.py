from typing import Any, Dict, Optional

try:
    from fastapi import Depends, FastAPI, Header, HTTPException
except ImportError:
    FastAPI = None

try:
    import ray
except ImportError:
    ray = None

from .coordinator import Coordinator, CoordinatorActor
from .auth import AuthenticationNotConfigured, TokenRegistry
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


def authenticated_user(authorization: Optional[str] = Header(default=None)) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail={"reason": "authentication_required"})
    scheme, separator, token = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(status_code=401, detail={"reason": "invalid_authorization"})
    try:
        registry = TokenRegistry.from_environment()
    except AuthenticationNotConfigured as exc:
        raise HTTPException(
            status_code=503, detail={"reason": "authentication_not_configured"}
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=503, detail={"reason": "authentication_configuration_invalid"}
        ) from exc
    try:
        return registry.authenticate(token.strip())
    except ValueError as exc:
        raise HTTPException(status_code=401, detail={"reason": "invalid_bearer_token"}) from exc


def _can_view_event(
    event: Dict[str, Any], user_id: str, calendar_shares: Dict[str, Any]
) -> bool:
    if event.get("visibility", "private") == "public" or (
        event.get("organizer") == user_id
        or user_id in event.get("participants", [])
    ):
        return True
    share = calendar_shares.get(event.get("organizer"), {}).get(
        event.get("kind", "")
    )
    return bool(
        share
        and user_id in share.get("users", [])
        and share.get("permission") in ("details", "edit")
    )


def _visible_snapshot(snapshot: Dict[str, Any], user_id: str) -> Dict[str, Any]:
    calendar_shares = snapshot.get("calendar_shares", {})
    return {
        "events": {
            uid: event
            for uid, event in snapshot.get("events", {}).items()
            if _can_view_event(event, user_id, calendar_shares)
        },
        "applied_index": snapshot.get("applied_index", 0),
    }


app = FastAPI(title="SD Calendario sobre Ray") if FastAPI is not None else None


if app is not None:
    @app.post("/v1/commands")
    @app.post("/v1/command")
    def apply_command(
        command: Dict[str, Any],
        user_id: str = Depends(authenticated_user),
    ):
        if command.get("sender_id") != user_id:
            raise HTTPException(
                status_code=403, detail={"reason": "sender_identity_mismatch"}
            )
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
    def status(user_id: str = Depends(authenticated_user)):
        del user_id
        cluster = get_cluster()
        if cluster is not None:
            return cluster.status()
        return resolve(call_coordinator("status"))

    @app.get("/ready")
    def ready():
        try:
            TokenRegistry.from_environment()
        except AuthenticationNotConfigured as exc:
            raise HTTPException(
                status_code=503,
                detail={"reason": "authentication_not_configured"},
            ) from exc
        except ValueError as exc:
            raise HTTPException(
                status_code=503,
                detail={"reason": "authentication_configuration_invalid"},
            ) from exc
        cluster = get_cluster()
        if cluster is None:
            return {"status": "ready", "mode": "local"}
        status = cluster.status()
        if not status.get("quorum_available") or not status.get("leader_id"):
            raise HTTPException(
                status_code=503,
                detail={
                    "reason": "cluster_not_ready",
                    "leader_id": status.get("leader_id"),
                    "alive_replicas": status.get("alive_replicas", 0),
                    "quorum_size": status.get("quorum_size"),
                },
            )
        return {"status": "ready", **status}

    @app.get("/v1/kv/{key}")
    def kv(key: str, user_id: str = Depends(authenticated_user)):
        cluster = get_cluster()
        snapshot = (
            cluster.snapshot()
            if cluster is not None
            else resolve(call_coordinator("snapshot"))
        )
        visible = _visible_snapshot(snapshot, user_id)
        if key == "*":
            return visible
        event = visible["events"].get(key)
        if event is None:
            return {"key": key, "value": None, "found": False}
        return {"key": key, "value": event, "found": True}

    @app.get("/v1/availability")
    def availability(
        users: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        user_id: str = Depends(authenticated_user),
    ):
        selected = [user for user in users.split(",") if user] if users else [user_id]
        cluster = get_cluster()
        if cluster is not None:
            try:
                return cluster.availability(selected, start, end, user_id)
            except ValueError as exc:
                raise HTTPException(
                    status_code=422, detail={"reason": str(exc)}
                ) from exc
        try:
            return resolve(
                call_coordinator("availability", selected, start, end, user_id)
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=422, detail={"reason": str(exc)}
            ) from exc

    @app.get("/v1/conflicts")
    def conflicts(
        user: str,
        start: str,
        end: str,
        exclude_uid: Optional[str] = None,
        user_id: str = Depends(authenticated_user),
    ):
        if user != user_id:
            raise HTTPException(
                status_code=403, detail={"reason": "conflicts_are_self_only"}
            )
        cluster = get_cluster()
        if cluster is not None:
            try:
                return cluster.conflicts(user, start, end, exclude_uid)
            except ValueError as exc:
                raise HTTPException(
                    status_code=422, detail={"reason": str(exc)}
                ) from exc
        try:
            return resolve(call_coordinator("conflicts", user, start, end, exclude_uid))
        except ValueError as exc:
            raise HTTPException(
                status_code=422, detail={"reason": str(exc)}
            ) from exc

    @app.get("/v1/sync")
    def sync(
        since: int = 0,
        limit: int = 100,
        user_id: str = Depends(authenticated_user),
    ):
        cluster = get_cluster()
        if cluster is not None:
            try:
                return cluster.sync(since, limit, user_id)
            except ValueError as exc:
                raise HTTPException(
                    status_code=422, detail={"reason": str(exc)}
                ) from exc
        try:
            return resolve(call_coordinator("sync", since, limit, user_id))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"reason": str(exc)}) from exc

    @app.get("/health")
    def health():
        return {"status": "ok"}
