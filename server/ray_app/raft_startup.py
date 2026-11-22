"""Idempotent startup for detached, named Ray actors.

Several API processes start at the same time and each tries to create the
same replicas, tickers and supervisor. ``get_if_exists=True`` handles the
normal case, but concurrent creators can still race: the losing handle
reports ``ActorAlreadyExistsError`` on its first method call. In that case
the winner is resolved by name and the bootstrap method is retried.
"""

from typing import Any, Optional

from .logging_config import get_logger

try:
    import ray
except ImportError:
    ray = None


def is_actor_already_exists(exc: BaseException) -> bool:
    """Return whether ``exc`` means that the named actor already exists."""
    error_type = getattr(
        getattr(ray, "exceptions", None), "ActorAlreadyExistsError", None
    )
    if error_type is not None and isinstance(exc, error_type):
        return True
    message = str(exc)
    return "Actor with name" in message and "already exists" in message


def create_named_actor(
    actor_cls: Any,
    *args: Any,
    name: str,
    namespace: str,
    start_method: Optional[str] = None,
    wait_for_start: bool = False,
    **options: Any,
) -> Any:
    """Create a named actor, or attach to the existing one after a race.

    ``start_method`` is called on the resulting handle. That call is what
    usually surfaces the deferred ``ActorAlreadyExistsError`` from a losing
    concurrent creation, so it is inside the retry.
    """
    if ray is None:
        raise RuntimeError("raft: ray is not installed")

    options["name"] = name
    options["namespace"] = namespace
    options.setdefault("get_if_exists", True)
    logger = get_logger("raft.startup")
    try:
        actor = actor_cls.options(**options).remote(*args)
        _start(actor, start_method, wait_for_start)
        return actor
    except Exception as exc:
        if not is_actor_already_exists(exc):
            raise
        logger.debug("attached to existing actor name=%s", name)
        actor = ray.get_actor(name, namespace=namespace)
        _start(actor, start_method, wait_for_start)
        return actor


def _start(actor: Any, method: Optional[str], wait: bool) -> None:
    if method is None:
        return
    ref = getattr(actor, method).remote()
    if wait:
        ray.get(ref)
