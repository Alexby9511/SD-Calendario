import argparse
import time
from pathlib import Path

import uvicorn

from server.ray_app.logging_config import get_logger
from server.ray_app.raft_placement import PLACEMENT_MODES, alive_node_count


def connect_ray(address, attempts: int = 2) -> None:
    """Connect to the Ray cluster, retrying transient client-server races.

    The head exposes a single Ray Client port. When several APIs connect at
    the same instant, the per-connection proxy can fail to start on its
    allocated port; a short retry lets the proxy assign another one.
    """
    import ray

    logger = get_logger("raft.startup")
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            ray.init(address=address, ignore_reinit_error=True)
            return
        except Exception as exc:
            last_error = exc
            logger.warning(
                "ray connection failed attempt=%d/%d reason=%r",
                attempt,
                attempts,
                exc,
            )
            if attempt == attempts:
                break
            try:
                if ray.is_initialized():
                    ray.shutdown()
            except Exception:
                pass
            time.sleep(min(attempt, 2))
    raise RuntimeError("ray: could not connect to the cluster") from last_error


def _wait_for_nodes(count: int, logger, timeout: float = 60.0) -> bool:
    """Wait until the Ray cluster has enough alive nodes to place the replicas."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        alive = alive_node_count()
        if alive >= count:
            return True
        time.sleep(0.5)
    logger.warning(
        "timed out waiting for Ray nodes alive=%d expected=%d",
        alive_node_count(),
        count,
    )
    return False


def start_raft_cluster(
    replicas: int,
    data_dir: str,
    namespace: str,
    placement_mode: str = "auto",
) -> None:
    """Start (or attach to) the Raft replicas and wire them into the API.

    The replicas and their tickers are detached Ray actors: they survive the
    death of this API process, so several API instances can serve the same
    cluster.
    """
    from server.ray_app.api import set_cluster
    from server.ray_app.logging_config import get_logger
    from server.ray_app.raft_actor import start_raft_cluster as start_replicas
    from server.ray_app.raft_cluster import RaftCluster
    from server.ray_app.raft_placement import (
        PlacementError,
        head_placement_key,
        plan_node_placement,
    )
    from server.ray_app.raft_supervisor import start_supervisor
    from server.ray_app.raft_ticker import start_tickers

    node_ids = [f"n{index}" for index in range(replicas)]
    base = Path(data_dir).expanduser().resolve()
    db_paths = {
        node_id: str(base / f"{node_id}.db") for node_id in node_ids
    }
    logger = get_logger("raft.placement")
    skip_unassigned = False
    try:
        if placement_mode == "strict" and replicas > 1:
            _wait_for_nodes(replicas, logger)
        node_assignments = plan_node_placement(node_ids, placement_mode)
    except PlacementError as exc:
        # The cluster is already running but some nodes are down: attach to
        # the existing replicas and let the supervisor recreate the missing
        # ones on their original node.
        logger.warning("strict placement unavailable reason=%s", exc)
        node_assignments = {}
        skip_unassigned = True

    if node_assignments:
        logger.info(
            "strict replica placement planned replicas=%d assignments=%s",
            replicas,
            node_assignments,
        )
    elif placement_mode == "auto" and replicas > 1:
        logger.warning(
            "not enough Ray nodes for strict placement; "
            "using default placement replicas=%d",
            replicas,
        )
    else:
        logger.info(
            "default replica placement selected replicas=%d mode=%s",
            replicas,
            placement_mode,
        )

    actors = start_replicas(
        node_ids,
        db_paths,
        namespace=namespace,
        node_assignments=node_assignments,
        skip_unassigned=skip_unassigned,
    )
    start_tickers(
        actors,
        namespace=namespace,
        node_assignments=node_assignments,
    )
    start_supervisor(
        node_ids,
        db_paths,
        namespace=namespace,
        node_assignments=node_assignments,
        supervisor_node_id=head_placement_key(),
    )
    set_cluster(RaftCluster(node_ids, namespace=namespace))


def main() -> None:
    ray_monitor = None
    parser = argparse.ArgumentParser(description="SD Calendario sobre Ray")
    parser.add_argument("--ray-address", default=None)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-ray", action="store_true")
    parser.add_argument(
        "--raft-replicas",
        type=int,
        default=5,
        help="number of Raft replicas (0 disables the Raft cluster)",
    )
    parser.add_argument(
        "--raft-data-dir",
        default="data/raft",
        help="directory that holds one SQLite database per replica",
    )
    parser.add_argument(
        "--raft-namespace",
        default="sdcal",
        help="Ray namespace that holds the Raft actors",
    )
    parser.add_argument(
        "--raft-placement",
        choices=PLACEMENT_MODES,
        default="auto",
        help="replica placement: strict one-per-node, auto, or off",
    )
    args = parser.parse_args()

    if not args.no_ray:
        connect_ray(args.ray_address)
        if args.raft_replicas > 0:
            start_raft_cluster(
                args.raft_replicas,
                args.raft_data_dir,
                args.raft_namespace,
                args.raft_placement,
            )
        import ray

        from server.ray_app.ray_monitor import RayConnectionMonitor

        def reattach_raft_cluster() -> None:
            if args.raft_replicas > 0:
                start_raft_cluster(
                    args.raft_replicas,
                    args.raft_data_dir,
                    args.raft_namespace,
                    args.raft_placement,
                )

        ray_monitor = RayConnectionMonitor(
            ray,
            args.ray_address,
            connect_ray,
            reattach_raft_cluster,
        )
        ray_monitor.start()

    from server.ray_app.api import app

    try:
        uvicorn.run(app, host=args.host, port=args.port)
    finally:
        if ray_monitor is not None:
            ray_monitor.stop()


if __name__ == "__main__":
    main()
