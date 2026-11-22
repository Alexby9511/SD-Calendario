import argparse
from pathlib import Path

import uvicorn

from server.ray_app.raft_placement import PLACEMENT_MODES


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
        import ray

        ray.init(address=args.ray_address, ignore_reinit_error=True)
        if args.raft_replicas > 0:
            start_raft_cluster(
                args.raft_replicas,
                args.raft_data_dir,
                args.raft_namespace,
                args.raft_placement,
            )

    from server.ray_app.api import app

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
